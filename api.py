"""FastAPI Application & WebSocket Hub for Central 24/7 Server."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from .agent import ServerWorkflowAgent
from .db import ServerDB
from .models import (
    DeviceRegistration,
    FocusReport,
    PrioritizationResponse,
    TaskCreateRequest,
    TaskItem,
    TaskUpdateRequest,
    VaultSyncPayload,
    VaultSyncResponse,
)

logger = logging.getLogger("central_server.api")

app = FastAPI(title="Workflow Central 24/7 Sync Server", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = ServerDB()


class WebSocketHub:
    def __init__(self) -> None:
        self.connections: Dict[WebSocket, str] = {}
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket, device_id: str = "web") -> None:
        await ws.accept()
        async with self._lock:
            self.connections[ws] = device_id
        db.register_device(device_id, "websocket_client")
        logger.info(f"Client {device_id} connected. Total active: {len(self.connections)}")

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            device_id = self.connections.pop(ws, "unknown")
        logger.info(f"Client {device_id} disconnected. Total active: {len(self.connections)}")

    async def broadcast(self, message: Dict[str, Any], exclude_ws: Optional[WebSocket] = None) -> None:
        async with self._lock:
            targets = [conn for conn in self.connections.keys() if conn != exclude_ws]

        for ws in targets:
            try:
                await ws.send_text(json.dumps(message))
            except Exception as e:
                logger.debug(f"Failed to send to client: {e}")
                await self.disconnect(ws)


hub = WebSocketHub()
agent = ServerWorkflowAgent(db=db, broadcast_callback=hub.broadcast)


@app.on_event("startup")
async def on_startup() -> None:
    await agent.start(interval_seconds=30)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await agent.stop()


# --- REST Endpoints ---

@app.get("/api/health")
async def health_check() -> Dict[str, Any]:
    return {
        "status": "online",
        "active_clients": len(hub.connections),
        "db_path": db.db_path,
    }


@app.get("/api/tasks")
async def get_tasks(
    date_file: Optional[str] = None,
    status: str = "all",
    section: Optional[str] = None,
) -> Dict[str, Any]:
    tasks = db.get_tasks(date_file=date_file, status=status, section=section)
    return {"tasks": tasks, "count": len(tasks)}


@app.post("/api/tasks", response_model=TaskItem)
async def create_task(req: TaskCreateRequest) -> TaskItem:
    text = req.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Task text cannot be empty.")

    task_id = f"task_{abs(hash(text + (req.date_file or '')))}"
    task_data = {
        "id": task_id,
        "text": text,
        "completed": False,
        "section": req.section or "General",
        "date_file": req.date_file or "",
        "domain": req.domain or "general",
    }
    saved = db.upsert_task(task_data, device_id=req.device_id or "api")

    # Real-time event broadcast
    await hub.broadcast({"event": "task_added", "task": saved})
    return TaskItem(**saved)


@app.patch("/api/tasks/{task_id}", response_model=TaskItem)
async def update_task(task_id: str, req: TaskUpdateRequest) -> TaskItem:
    existing = db.get_task_by_id(task_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Task not found")

    if req.completed is not None:
        existing["completed"] = req.completed
    if req.text is not None:
        existing["text"] = req.text.strip()
    if req.section is not None:
        existing["section"] = req.section
    if req.priority_score is not None:
        existing["priority_score"] = req.priority_score

    saved = db.upsert_task(existing, device_id=req.device_id or "api")
    await hub.broadcast({"event": "task_updated", "task": saved})
    return TaskItem(**saved)


@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str, device_id: str = "api") -> Dict[str, Any]:
    success = db.delete_task(task_id, device_id=device_id)
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    await hub.broadcast({"event": "task_deleted", "task_id": task_id})
    return {"status": "deleted", "task_id": task_id}


@app.get("/api/tasks/prioritize", response_model=PrioritizationResponse)
async def prioritize_tasks(date_file: Optional[str] = None) -> PrioritizationResponse:
    tasks = db.get_tasks(date_file=date_file, status="open")
    devices = db.get_active_devices()
    active_focus = devices[0]["active_focus"] if devices else {}
    res = agent.prioritize(tasks, active_focus)
    return PrioritizationResponse(**res)


@app.post("/api/focus/report")
async def report_focus(report: FocusReport) -> Dict[str, Any]:
    focus_data = {
        "app_name": report.app_name,
        "window_title": report.window_title,
        "url": report.url,
        "timestamp": report.timestamp,
    }
    db.update_device_focus(report.device_id, focus_data)
    await hub.broadcast({
        "event": "focus_sync",
        "device_id": report.device_id,
        "focus": focus_data,
    })
    return {"status": "recorded", "device_id": report.device_id}


@app.get("/api/devices")
async def get_devices() -> List[Dict[str, Any]]:
    return db.get_active_devices()


# --- Obsidian Vault Sync Engine (Bi-directional Merge) ---

TASK_LINE_REGEX = re.compile(r"^(\s*[-*+]\s+\[)([ xX])(\]\s*)(.*)$")
HEADER_REGEX = re.compile(r"^(#+)\s+(.+)$")


@app.post("/api/sync/vault", response_model=VaultSyncResponse)
async def sync_vault_file(payload: VaultSyncPayload) -> VaultSyncResponse:
    lines = payload.content.splitlines(keepends=True)
    server_tasks = {t["text"].lower().strip(): t for t in db.get_tasks(date_file=payload.date_file)}

    current_section = f"## {payload.date_file.replace('.md', '')}"
    merged_lines: List[str] = []
    seen_texts = set()
    server_changes_applied = 0

    for line in lines:
        line_stripped = line.strip()
        h_match = HEADER_REGEX.match(line_stripped)
        if h_match:
            current_section = line_stripped
            merged_lines.append(line)
            continue

        t_match = TASK_LINE_REGEX.match(line)
        if t_match:
            prefix, check, suffix, text = t_match.group(1), t_match.group(2), t_match.group(3), t_match.group(4).strip()
            local_completed = check.lower() == "x"
            seen_texts.add(text.lower())

            # Check if server has newer state
            server_task = server_tasks.get(text.lower())
            if server_task and server_task["completed"] != local_completed:
                # If server is completed, reflect completion
                effective_check = "x" if server_task["completed"] else " "
                merged_lines.append(f"{prefix}{effective_check}{suffix}{text}\n")
                server_changes_applied += 1
            else:
                # Upsert local task into server DB
                task_id = server_task["id"] if server_task else f"task_{abs(hash(text + payload.date_file))}"
                db.upsert_task({
                    "id": task_id,
                    "text": text,
                    "completed": local_completed,
                    "section": current_section,
                    "date_file": payload.date_file,
                }, device_id=payload.device_id)
                merged_lines.append(line)
        else:
            merged_lines.append(line)

    # Append any server tasks created remotely that are missing in the local file
    for s_text_lower, s_task in server_tasks.items():
        if s_text_lower not in seen_texts:
            check_char = "x" if s_task["completed"] else " "
            merged_lines.append(f"- [{check_char}] {s_task['text']}\n")
            server_changes_applied += 1

    merged_content = "".join(merged_lines)
    return VaultSyncResponse(
        date_file=payload.date_file,
        merged_content=merged_content,
        conflicts_resolved=0,
        server_changes_applied=server_changes_applied,
    )


# --- WebSockets Endpoint ---

@app.websocket("/ws")
@app.websocket("/ws/{device_id}")
async def websocket_route(ws: WebSocket, device_id: str = "web") -> None:
    await hub.connect(ws, device_id=device_id)
    try:
        # Send initial snapshot
        tasks = db.get_tasks()
        await ws.send_text(json.dumps({
            "event": "initial_state",
            "device_id": device_id,
            "tasks": tasks,
        }))

        while True:
            raw = await ws.receive_text()
            if not raw:
                continue
            data = json.loads(raw)
            action = data.get("action")

            if action == "ping":
                await ws.send_text(json.dumps({"event": "pong"}))

            elif action == "toggle_task":
                task_id = data.get("task_id")
                completed = bool(data.get("completed", True))
                existing = db.get_task_by_id(task_id)
                if existing:
                    existing["completed"] = completed
                    saved = db.upsert_task(existing, device_id=device_id)
                    await hub.broadcast({"event": "task_updated", "task": saved}, exclude_ws=ws)
                    await ws.send_text(json.dumps({"event": "task_updated", "task": saved}))

            elif action == "add_task":
                text = data.get("text", "").strip()
                if text:
                    task_id = f"task_{abs(hash(text))}"
                    saved = db.upsert_task({
                        "id": task_id,
                        "text": text,
                        "completed": False,
                        "section": data.get("section", "General"),
                        "date_file": data.get("date_file", ""),
                    }, device_id=device_id)
                    await hub.broadcast({"event": "task_added", "task": saved})

            elif action == "report_focus":
                db.update_device_focus(device_id, {
                    "app_name": data.get("app_name", ""),
                    "window_title": data.get("window_title", ""),
                    "url": data.get("url", ""),
                })

    except WebSocketDisconnect:
        await hub.disconnect(ws)
    except Exception as e:
        logger.warning(f"WebSocket client error: {e}")
        await hub.disconnect(ws)


# --- Embedded Web Dashboard ---

@app.get("/", response_class=HTMLResponse)
async def web_ui() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Workflow Central • 24/7 Live Hub</title>
  <style>
    :root {
      --bg: #0d1117;
      --card: #161b22;
      --border: #30363d;
      --text: #f0f6fc;
      --muted: #8b949e;
      --accent: #58a6ff;
      --green: #238636;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', Roboto, sans-serif; padding: 18px; max-width: 680px; margin: 0 auto; }
    header { display: flex; justify-content: space-between; align-items: center; padding-bottom: 16px; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
    h1 { font-size: 1.2rem; font-weight: 700; }
    .status-pill { font-size: 0.75rem; padding: 4px 10px; border-radius: 999px; background: rgba(35, 134, 54, 0.2); color: #3fb950; font-weight: 600; display: flex; align-items: center; gap: 6px; }
    .status-dot { width: 7px; height: 7px; border-radius: 50%; background: #3fb950; }
    .focus-card { background: linear-gradient(135deg, rgba(88, 166, 255, 0.08), rgba(35, 134, 54, 0.05)); border: 1px solid rgba(88, 166, 255, 0.3); border-radius: 12px; padding: 14px 16px; margin-bottom: 18px; }
    .focus-label { font-size: 0.72rem; text-transform: uppercase; color: var(--accent); font-weight: 700; letter-spacing: 0.5px; }
    .focus-title { font-size: 1.05rem; font-weight: 600; margin-top: 4px; }
    .task-list { list-style: none; display: flex; flex-direction: column; gap: 8px; }
    .task-item { display: flex; align-items: center; gap: 12px; background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; }
    .task-item.completed { opacity: 0.5; }
    .task-item.completed .task-text { text-decoration: line-through; color: var(--muted); }
    .checkbox { width: 22px; height: 22px; border-radius: 6px; border: 2px solid var(--border); cursor: pointer; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
    .checkbox.checked { background: var(--green); border-color: var(--green); }
    .checkbox.checked::after { content: '✓'; color: white; font-size: 13px; font-weight: 800; }
    .task-text { flex: 1; font-size: 0.95rem; }
    .input-bar { margin-top: 18px; display: flex; gap: 8px; }
    input[type="text"] { flex: 1; background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px 16px; color: var(--text); outline: none; }
    button.add-btn { background: var(--green); color: white; border: none; border-radius: 10px; padding: 0 18px; font-weight: 600; cursor: pointer; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Workflow Central 24/7</h1>
      <span style="font-size: 0.8rem; color: var(--muted);" id="headerSub">Connected Devices • Live</span>
    </div>
    <div class="status-pill"><span class="status-dot"></span><span>Live Hub</span></div>
  </header>

  <div class="focus-card">
    <div class="focus-label">Top Priority Roadmap</div>
    <div class="focus-title" id="focusTitle">Loading priority tasks...</div>
  </div>

  <ul class="task-list" id="taskList"></ul>

  <div class="input-bar">
    <input type="text" id="taskInput" placeholder="Add central task..." />
    <button class="add-btn" id="addBtn">Add</button>
  </div>

  <script>
    let ws;
    const taskList = document.getElementById('taskList');
    const focusTitle = document.getElementById('focusTitle');
    const taskInput = document.getElementById('taskInput');
    const addBtn = document.getElementById('addBtn');

    function connectWs() {
      const loc = window.location;
      const wsUri = (loc.protocol === 'https:' ? 'wss://' : 'ws://') + loc.host + '/ws/web_client';
      ws = new WebSocket(wsUri);

      ws.onmessage = (e) => {
        const data = JSON.parse(e.data);
        if (data.event === 'initial_state') {
          renderTasks(data.tasks);
          fetchPriorities();
        } else if (data.event === 'task_updated' || data.event === 'task_added' || data.event === 'task_deleted') {
          loadTasks();
          fetchPriorities();
        }
      };

      ws.onclose = () => setTimeout(connectWs, 2000);
    }

    async function loadTasks() {
      const res = await fetch('/api/tasks');
      const data = await res.json();
      renderTasks(data.tasks);
    }

    async function fetchPriorities() {
      try {
        const res = await fetch('/api/tasks/prioritize');
        const data = await res.json();
        if (data.immediate_focus) {
          focusTitle.textContent = data.immediate_focus;
        }
      } catch (err) {}
    }

    function renderTasks(tasks) {
      taskList.innerHTML = '';
      tasks.forEach(t => {
        const li = document.createElement('li');
        li.className = 'task-item' + (t.completed ? ' completed' : '');

        const cb = document.createElement('div');
        cb.className = 'checkbox' + (t.completed ? ' checked' : '');
        cb.onclick = () => {
          ws.send(JSON.stringify({
            action: 'toggle_task',
            task_id: t.id,
            completed: !t.completed
          }));
        };

        const txt = document.createElement('div');
        txt.className = 'task-text';
        txt.textContent = t.text;

        li.appendChild(cb);
        li.appendChild(txt);
        taskList.appendChild(li);
      });
    }

    function addTask() {
      const text = taskInput.value.trim();
      if (!text) return;
      ws.send(JSON.stringify({
        action: 'add_task',
        text: text
      }));
      taskInput.value = '';
    }

    addBtn.onclick = addTask;
    taskInput.onkeydown = (e) => { if (e.key === 'Enter') addTask(); };

    connectWs();
    loadTasks();
  </script>
</body>
</html>
"""
