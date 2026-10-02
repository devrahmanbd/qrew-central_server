"""Pydantic data models for Central Workflow Server."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TaskItem(BaseModel):
    id: str
    text: str
    completed: bool = False
    section: str = "General"
    date_file: str = ""
    line_number: int = 0
    priority_score: float = 0.0
    domain: str = "general"
    tags: List[str] = Field(default_factory=list)
    version: int = 1
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class TaskCreateRequest(BaseModel):
    text: str
    section: Optional[str] = "General"
    date_file: Optional[str] = ""
    domain: Optional[str] = "general"
    device_id: Optional[str] = "unknown"


class TaskUpdateRequest(BaseModel):
    completed: Optional[bool] = None
    text: Optional[str] = None
    section: Optional[str] = None
    priority_score: Optional[float] = None
    device_id: Optional[str] = "unknown"


class DeviceRegistration(BaseModel):
    device_id: str
    device_type: str = "web"  # "macos", "android", "web", "extension"
    last_seen: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    active_focus: Dict[str, Any] = Field(default_factory=dict)


class FocusReport(BaseModel):
    device_id: str
    app_name: str = "Desktop"
    window_title: str = ""
    url: Optional[str] = ""
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class VaultSyncPayload(BaseModel):
    device_id: str = "macos"
    date_file: str  # e.g., "17-Sep.md"
    content: str    # Raw markdown content from local Obsidian note
    last_synced_at: Optional[str] = None


class VaultSyncResponse(BaseModel):
    date_file: str
    merged_content: str
    conflicts_resolved: int = 0
    server_changes_applied: int = 0
    synced_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class PrioritizationResponse(BaseModel):
    immediate_focus: Optional[str] = None
    rezoning_suggestion: str
    ranked_tasks: List[Dict[str, Any]] = Field(default_factory=list)
    clusters: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)
    mode: str = "ai_thinking"
