# AGENTS.md — central_server only

## Entrypoints

- `api.py:app` — `WebSocketHub` (api.py:43), `sync_vault_file` (api.py:196), inline dashboard `web_ui` (api.py:321); dual WS routes `/ws` + `/ws/{device_id}` (api.py:258-259).
- `db.py:ServerDB` — `_get_connection` (db.py:23) sets `PRAGMA journal_mode=WAL` with `DELETE` fallback, `synchronous=NORMAL`, `timeout=15.0`.
- `agent.py:ServerWorkflowAgent` — `build_dag_and_ranks` (agent.py:88), `prioritize` (agent.py:155), `_background_loop` (agent.py:268), `evaluate_and_dispatch` (agent.py:276).
- `models.py` — `PrioritizationResponse.mode` defaults to `"ai_thinking"` (models.py:74), not the runtime value; `DeviceRegistration.device_type` enumerates `"macos" | "android" | "web" | "extension"` (models.py:41).
- `__init__.py` re-exports `ServerDB, ServerWorkflowAgent, app` — no side effects beyond `db = ServerDB()` at `api.py:40` on import.

## Commands + required CWDs

- Dev: CWD `/Users/rahman/Downloads/complete_intellect_ecosystem`, run `python3 -m uvicorn central_server.api:app --host 0.0.0.0 --port 8765` — bare `python3 api.py` fails (package-relative `from .agent/.db`).
- Docker: CWD `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server`, run `docker compose up --build` — `docker-compose.yml:6` sets `context: .`; image `COPY . /app/central_server` (Dockerfile:16) so building from repo root breaks paths.
- systemd: unit at `systemd/workflow-server.service` pins `WorkingDirectory=/opt/workflow_server` + `PYTHONPATH=/opt/workflow_server` + `ExecStart=/usr/local/bin/uvicorn` — repo must be checked out at `/opt/workflow_server`, not `~`.
- Deps: only `requirements.txt` (fastapi, uvicorn[standard], pydantic) — no test runner or lint config in this folder.

## Env (folder-specific defaults)

- `OPENROUTER_EMBED_MODEL=nvidia/nemotron-3-embed-1b:free`, `OPENROUTER_THINKING_MODEL=nvidia/nemotron-3-ultra-550b-a55b:free` (agent.py:47-52, docker-compose.yml:17-18) — embed var currently unused; ranking uses 128-dim `_hash_vector`.
- `OPENROUTER_URL=https://openrouter.ai/api/v1` hardcoded (agent.py:20); `_call_thinking_model` fires only when key AND non-empty focus text exist (agent.py:207), 12s timeout, `temperature=0.2`.
- `SERVER_DATA_DIR` resolves to `/app/data` (Dockerfile:7, compose volume `./data:/app/data`) vs `/var/lib/workflow_server/data` (service file:12); DB file is always `<dir>/workflow_central.db` (db.py:19).
- CORS is fully open: `allow_origins=["*"]` (api.py:32-38).

## Quirks

- Three ID schemes coexist: REST create `task_{abs(hash(text + date_file))}` (api.py:113), WS `add_task` `task_{abs(hash(text))}` with no `date_file` (api.py:294), DB fallback `task_{abs(hash(text))}` (db.py:75) — Python `hash()` is salted per process, so IDs are unstable across restarts.
- Vault merge regexes at api.py:191-192: `TASK_LINE_REGEX=^(\s*[-*+]\s+\[)([ xX])(\]\s*)(.*)$`, `HEADER_REGEX=^(#+)\s+(.+)$`; default section is `f"## {date_file.replace('.md', '')}"` (api.py:200); `conflicts_resolved` is hardcoded `0` (api.py:251).
- `GET /api/tasks` ordering is `line_number ASC, priority_score DESC, updated_at DESC` (db.py:149); `status=open` maps to `completed = 0`, `status=completed` to `completed = 1` (db.py:140-143).
- `@app.on_event("startup")` (api.py:76) starts `agent.start(interval_seconds=30)`; shutdown calls `agent.stop()` (api.py:81-83) — keep the pair; lifespan migration breaks the cancel path in `stop()` (agent.py:258-266).
- `evaluate_and_dispatch` reads only `devices[0]["active_focus"]` (agent.py:283; same pattern api.py:162), broadcasts `task_reminder` solely when `immediate_focus` text differs from `_last_top_task_id` (agent.py:290) — repeated evaluates are silent.
- Scoring constants (agent.py:128-140): base `50.0`, `+35.0` prereq cue (`(first)` per agent.py:31-34), `+15.0` per dependent, `-20.0` per dependency, position `max(0, 10.0 - line*0.5)`, focus boost `sim * 25.0` with `context_aligned` flag above `5.0`.
