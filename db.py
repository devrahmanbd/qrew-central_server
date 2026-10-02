"""SQLite Database Manager with WAL Mode for Central Server."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("central_server.db")


class ServerDB:
    def __init__(self, db_path: Optional[str] = None) -> None:
        if db_path is None:
            data_dir = os.environ.get("SERVER_DATA_DIR", "/tmp/workflow_server/data")
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, "workflow_central.db")
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        # Enable Write-Ahead Logging (WAL) for high concurrency
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.OperationalError:
            conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0,
                section TEXT NOT NULL DEFAULT 'General',
                date_file TEXT NOT NULL DEFAULT '',
                line_number INTEGER NOT NULL DEFAULT 0,
                priority_score REAL NOT NULL DEFAULT 0.0,
                updated_at TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                domain TEXT NOT NULL DEFAULT 'general',
                tags TEXT NOT NULL DEFAULT '[]'
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_date ON tasks(date_file);
            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(completed);

            CREATE TABLE IF NOT EXISTS device_registry (
                device_id TEXT PRIMARY KEY,
                device_type TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                active_focus TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS sync_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                task_id TEXT NOT NULL,
                date_file TEXT NOT NULL DEFAULT '',
                timestamp TEXT NOT NULL,
                device_id TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_journal_time ON sync_journal(timestamp);
            """)

    def upsert_task(self, task_data: Dict[str, Any], device_id: str = "system") -> Dict[str, Any]:
        task_id = task_data.get("id") or f"task_{abs(hash(task_data.get('text', '')))}"
        text = task_data.get("text", "").strip()
        completed = 1 if task_data.get("completed") else 0
        section = task_data.get("section", "General")
        date_file = task_data.get("date_file", "")
        line_number = int(task_data.get("line_number", 0))
        priority_score = float(task_data.get("priority_score", 0.0))
        domain = task_data.get("domain", "general")
        tags = json.dumps(task_data.get("tags", []))
        now = datetime.utcnow().isoformat()

        with self._get_connection() as conn:
            cur = conn.execute("SELECT version FROM tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            if row:
                new_version = row["version"] + 1
                conn.execute("""
                    UPDATE tasks SET
                        text = ?, completed = ?, section = ?, date_file = ?,
                        line_number = ?, priority_score = ?, updated_at = ?,
                        version = ?, domain = ?, tags = ?
                    WHERE id = ?
                """, (text, completed, section, date_file, line_number, priority_score, now, new_version, domain, tags, task_id))
                action = "UPDATE"
            else:
                new_version = 1
                conn.execute("""
                    INSERT INTO tasks (
                        id, text, completed, section, date_file, line_number,
                        priority_score, updated_at, version, domain, tags
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (task_id, text, completed, section, date_file, line_number, priority_score, now, new_version, domain, tags))
                action = "INSERT"

            conn.execute("""
                INSERT INTO sync_journal (action, task_id, date_file, timestamp, device_id, details)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (action, task_id, date_file, now, device_id, json.dumps({"text": text, "completed": completed, "version": new_version})))

        return self.get_task_by_id(task_id) or {}

    def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            if row:
                d = dict(row)
                d["completed"] = bool(d["completed"])
                d["tags"] = json.loads(d["tags"])
                return d
        return None

    def get_tasks(
        self,
        date_file: Optional[str] = None,
        status: str = "all",
        section: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM tasks WHERE 1=1"
        params: List[Any] = []

        if date_file:
            query += " AND (date_file = ? OR date_file LIKE ?)"
            params.extend([date_file, f"%{date_file}%"])

        if status.lower() in ("open", "pending"):
            query += " AND completed = 0"
        elif status.lower() in ("completed", "done"):
            query += " AND completed = 1"

        if section:
            query += " AND section LIKE ?"
            params.append(f"%{section}%")

        query += " ORDER BY line_number ASC, priority_score DESC, updated_at DESC"

        with self._get_connection() as conn:
            cur = conn.execute(query, params)
            results = []
            for row in cur.fetchall():
                d = dict(row)
                d["completed"] = bool(d["completed"])
                d["tags"] = json.loads(d["tags"])
                results.append(d)
            return results

    def delete_task(self, task_id: str, device_id: str = "system") -> bool:
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cur = conn.execute("SELECT date_file, text FROM tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            if not row:
                return False
            date_file = row["date_file"]
            text = row["text"]

            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.execute("""
                INSERT INTO sync_journal (action, task_id, date_file, timestamp, device_id, details)
                VALUES ('DELETE', ?, ?, ?, ?, ?)
            """, (task_id, date_file, now, device_id, json.dumps({"text": text})))
            return True

    def register_device(self, device_id: str, device_type: str, focus: Optional[Dict[str, Any]] = None) -> None:
        now = datetime.utcnow().isoformat()
        focus_json = json.dumps(focus or {})
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO device_registry (device_id, device_type, last_seen, active_focus)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(device_id) DO UPDATE SET
                    device_type = excluded.device_type,
                    last_seen = excluded.last_seen,
                    active_focus = CASE WHEN ? != '{}' THEN ? ELSE device_registry.active_focus END
            """, (device_id, device_type, now, focus_json, focus_json, focus_json))

    def update_device_focus(self, device_id: str, focus: Dict[str, Any], device_type: str = "client") -> None:
        now = datetime.utcnow().isoformat()
        focus_json = json.dumps(focus)
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO device_registry (device_id, device_type, last_seen, active_focus)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(device_id) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    active_focus = excluded.active_focus
            """, (device_id, device_type, now, focus_json))

    def get_active_devices(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM device_registry ORDER BY last_seen DESC")
            devices = []
            for row in cur.fetchall():
                d = dict(row)
                d["active_focus"] = json.loads(d["active_focus"])
                devices.append(d)
            return devices

    def get_journal_since(self, timestamp: str) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.execute("""
                SELECT * FROM sync_journal WHERE timestamp > ? ORDER BY id ASC
            """, (timestamp,))
            journal = []
            for row in cur.fetchall():
                d = dict(row)
                d["details"] = json.loads(d["details"])
                journal.append(d)
            return journal
