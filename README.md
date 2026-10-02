# central_server — Workflow Central 24/7 Sync Hub

Canonical hub of the qrew workflow-sync system. FastAPI REST + WebSocket on `:8765`, SQLite WAL storage, OpenRouter reasoning agent with DAG-heuristic fallback, embedded live dashboard at `/`.

## Architecture

| File (absolute path) | Purpose |
|---|---|
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/api.py` | FastAPI `app`, `WebSocketHub`, all REST + WS routes, vault merge, inline dashboard HTML at `GET /` |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/db.py` | `ServerDB` — SQLite WAL at `$SERVER_DATA_DIR/workflow_central.db`, tables `tasks`, `device_registry`, `sync_journal` |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/agent.py` | `ServerWorkflowAgent` — DAG ranking + OpenRouter thinking model (`nemotron`), 30s background loop |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/models.py` | Pydantic schemas: `TaskItem`, `TaskCreateRequest`, `TaskUpdateRequest`, `FocusReport`, `VaultSyncPayload/Response`, `PrioritizationResponse` |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/requirements.txt` | `fastapi>=0.100.0`, `uvicorn[standard]>=0.23.0`, `pydantic>=2.0.0` |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/Dockerfile` | `python:3.11-slim`, `WORKDIR /app`, copies repo to `/app/central_server`, exposes `8765`, healthchecks `/api/health` |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/docker-compose.yml` | Service `workflow-central-server`, compose context is `.` (this folder), volume `./data:/app/data` |
| `/Users/rahman/Downloads/complete_intellect_ecosystem/central_server/systemd/workflow-server.service` | systemd unit: `WorkingDirectory=/opt/workflow_server`, `PYTHONPATH=/opt/workflow_server` |

## Run options

Run:

```bash
# Dev — CWD must be repo root (package-relative imports break otherwise)
cd /Users/rahman/Downloads/complete_intellect_ecosystem
python3 -m uvicorn central_server.api:app --host 0.0.0.0 --port 8765
```

```bash
# Docker — CWD must be central_server/ (compose context is .)
cd /Users/rahman/Downloads/complete_intellect_ecosystem/central_server && docker compose up --build
```

This starts `workflow_central_server` on `8765:8765` with `SERVER_DATA_DIR=/app/data`.

Install systemd (Linux):

```bash
sudo cp /Users/rahman/Downloads/complete_intellect_ecosystem/central_server/systemd/workflow-server.service /etc/systemd/system/ \
  && sudo systemctl enable --now workflow-server
```

The unit runs `/usr/local/bin/uvicorn central_server.api:app --host 0.0.0.0 --port 8765` as `root` with `SERVER_DATA_DIR=/var/lib/workflow_server/data`.

## Environment variables

| Var | Default | Effect |
|---|---|---|
| `OPENROUTER_API_KEY` | `""` | Empty disables the thinking model; `agent.py` returns `mode=dag_topological_heuristic` |
| `OPENROUTER_EMBED_MODEL` | `nvidia/nemotron-3-embed-1b:free` | Reserved; current ranking uses local hash vectors, not this model |
| `OPENROUTER_THINKING_MODEL` | `nvidia/nemotron-3-ultra-550b-a55b:free` | Model called at `https://openrouter.ai/api/v1/chat/completions` when key + focus text are present |
| `SERVER_DATA_DIR` | `/tmp/workflow_server/data` (`/app/data` in Docker, `/var/lib/workflow_server/data` in systemd) | Directory for `workflow_central.db` |

No `.env` loader — set vars in the shell, compose file, or unit file.

## Key endpoints

| Method + path | Handler (`api.py`) | Notes |
|---|---|---|
| `GET /` | `web_ui()` | Embedded dashboard (inline HTML/JS, connects to `/ws/web_client`) |
| `GET /api/health` | `health_check()` | Returns `{status, active_clients, db_path}`; Docker `HEALTHCHECK` hits this |
| `GET /api/tasks` | `get_tasks()` | Query: `date_file`, `status=all\|open\|completed`, `section` (LIKE match) |
| `POST /api/tasks` | `create_task()` | Body `TaskCreateRequest`; broadcasts `task_added` |
| `PATCH /api/tasks/{task_id}` | `update_task()` | Partial update `text/completed/section/priority_score`; broadcasts `task_updated` |
| `DELETE /api/tasks/{task_id}` | `delete_task()` | Query `device_id`; broadcasts `task_deleted` |
| `GET /api/tasks/prioritize` | `prioritize_tasks()` | Query `date_file`; returns `PrioritizationResponse` with `immediate_focus`, `ranked_tasks`, `clusters`, `mode` |
| `POST /api/focus/report` | `report_focus()` | Body `FocusReport`; broadcasts `focus_sync` |
| `GET /api/devices` | `get_devices()` | All rows from `device_registry`, newest first |
| `POST /api/sync/vault` | `sync_vault_file()` | Body `VaultSyncPayload{device_id, date_file, content}`; returns `VaultSyncResponse{merged_content, server_changes_applied}` |
| `WS /ws`, `WS /ws/{device_id}` | `websocket_route()` | Sends `initial_state` on connect; actions `ping/toggle_task/add_task/report_focus`; server pushes `task_added/task_updated/task_deleted/focus_sync/task_reminder` |

Vault merge is line-oriented: task lines matched by `TASK_LINE_REGEX`, headers by `HEADER_REGEX`; server-completed state wins on conflict; server-only tasks are appended as `- [ ]/[x]` lines.
