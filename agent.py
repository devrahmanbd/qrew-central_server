"""24/7 Autonomous Workflow & Reasoning Agent."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Set

from .db import ServerDB

logger = logging.getLogger("central_server.agent")


class ServerWorkflowAgent:
    OPENROUTER_URL = "https://openrouter.ai/api/v1"

    DOMAIN_KEYWORDS = {
        "video": ["video", "render", "youtube", "edit", "share", "stream"],
        "account": ["account", "login", "recovery", "flynn", "shaheen", "signup", "auth"],
        "database": ["db", "database", "postgres", "sql", "text", "migration"],
        "social": ["facebook", "twitter", "x", "linkedin", "social", "telegram"],
        "development": ["framique", "ghost", "code", "agent", "api", "backend", "go", "react"],
        "microsoft": ["microsoft", "ms", "azure", "office", "windows"],
    }

    PREREQUISITE_PATTERNS = [
        re.compile(r"\(first\)", re.IGNORECASE),
        re.compile(r"(first|step 1|phase 1|prerequisite)", re.IGNORECASE),
    ]

    def __init__(
        self,
        db: Optional[ServerDB] = None,
        broadcast_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        api_key: Optional[str] = None,
        embed_model: Optional[str] = None,
        thinking_model: Optional[str] = None,
    ) -> None:
        self.db = db if db is not None else ServerDB()
        self.broadcast_callback = broadcast_callback
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        self.embed_model = embed_model or os.environ.get(
            "OPENROUTER_EMBED_MODEL", "nvidia/nemotron-3-embed-1b:free"
        )
        self.thinking_model = thinking_model or os.environ.get(
            "OPENROUTER_THINKING_MODEL", "nvidia/nemotron-3-ultra-550b-a55b:free"
        )
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_top_task_id: Optional[str] = None

    def _infer_domain(self, text: str) -> str:
        text_lower = text.lower()
        for domain, keywords in self.DOMAIN_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                return domain
        return "general"

    def _has_prereq_cue(self, text: str) -> bool:
        return any(p.search(text) is not None for p in self.PREREQUISITE_PATTERNS)

    def _hash_vector(self, text: str, dim: int = 128) -> List[float]:
        vec = [0.0] * dim
        words = re.findall(r"\w+", text.lower())
        if not words:
            return vec
        for word in words:
            h = int(hashlib.md5(word.encode("utf-8")).hexdigest(), 16)
            idx = h % dim
            sign = 1.0 if (h >> 8) % 2 == 0 else -1.0
            vec[idx] += sign
        norm = math.sqrt(sum(x * x for x in vec))
        return [x / norm for x in vec] if norm > 0 else vec

    def _cosine_similarity(self, v1: List[float], v2: List[float]) -> float:
        if not v1 or not v2 or len(v1) != len(v2):
            return 0.0
        dot = sum(a * b for a, b in zip(v1, v2))
        n1 = math.sqrt(sum(a * a for a in v1))
        n2 = math.sqrt(sum(b * b for b in v2))
        return dot / (n1 * n2) if (n1 > 0 and n2 > 0) else 0.0

    def build_dag_and_ranks(self, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Constructs DAG over tasks, computes topological priority weights."""
        open_tasks = [t for t in tasks if not t.get("completed", False)]
        if not open_tasks:
            return {"nodes": [], "dependencies": {}, "dependents": {}}

        nodes: Dict[str, Dict[str, Any]] = {}
        dependencies: Dict[str, Set[str]] = {t["id"]: set() for t in open_tasks}
        dependents: Dict[str, Set[str]] = {t["id"]: set() for t in open_tasks}

        for t in open_tasks:
            domain = self._infer_domain(t.get("text", ""))
            is_prereq = self._has_prereq_cue(t.get("text", ""))
            nodes[t["id"]] = {
                **t,
                "domain": domain,
                "is_prerequisite": is_prereq,
                "base_score": 50.0,
            }

        # Dependencies resolution
        for t_id, node in nodes.items():
            text_i = node.get("text", "").lower()

            # Rule 1: Explicit (first) priority marks it as prerequisite to siblings
            if node["is_prerequisite"]:
                for other_id, other_node in nodes.items():
                    if other_id != t_id and (other_node["domain"] == node["domain"] or "recovery" in other_node["text"].lower()):
                        dependencies[other_id].add(t_id)
                        dependents[t_id].add(other_id)

            # Rule 2: Sequential pipeline (Create -> Share)
            if "create" in text_i:
                for other_id, other_node in nodes.items():
                    text_other = other_node.get("text", "").lower()
                    if other_id != t_id and ("share" in text_other or "publish" in text_other):
                        if node["domain"] == other_node["domain"]:
                            dependencies[other_id].add(t_id)
                            dependents[t_id].add(other_id)

        # Topological scoring
        ranked_nodes = []
        for t_id, node in nodes.items():
            score = node["base_score"]
            if node["is_prerequisite"]:
                score += 35.0
            # Out-degree bonus: unblocks others
            score += len(dependents[t_id]) * 15.0
            # In-degree penalty: blocked by pending tasks
            score -= len(dependencies[t_id]) * 20.0
            # Position bias
            line_no = node.get("line_number", 0)
            score += max(0, 10.0 - (line_no * 0.5))

            node["priority_score"] = round(max(0.0, score), 1)
            node["dependencies"] = list(dependencies[t_id])
            node["dependents"] = list(dependents[t_id])
            node["is_blocked"] = len(dependencies[t_id]) > 0
            ranked_nodes.append(node)

        ranked_nodes.sort(key=lambda n: n["priority_score"], reverse=True)
        return {
            "nodes": ranked_nodes,
            "dependencies": {k: list(v) for k, v in dependencies.items()},
            "dependents": {k: list(v) for k, v in dependents.items()},
        }

    def prioritize(
        self,
        tasks: List[Dict[str, Any]],
        current_focus: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Evaluates tasks through graph reasoning and AI thinking model."""
        open_tasks = [t for t in tasks if not t.get("completed", False)]
        if not open_tasks:
            return {
                "immediate_focus": None,
                "rezoning_suggestion": "All tasks are complete.",
                "ranked_tasks": [],
                "clusters": {},
                "mode": "completed",
            }

        dag = self.build_dag_and_ranks(open_tasks)
        ranked_nodes = dag["nodes"]

        # Context alignment with active focus
        focus_dict = current_focus or {}
        focus_text = f"{focus_dict.get('app_name', '')} {focus_dict.get('window_title', '')}".strip()
        focus_vec = self._hash_vector(focus_text) if focus_text else [0.0] * 128

        for node in ranked_nodes:
            t_vec = self._hash_vector(node.get("text", ""))
            sim = self._cosine_similarity(focus_vec, t_vec) if focus_text else 0.0
            context_boost = max(0.0, sim * 25.0)
            node["priority_score"] = round(node["priority_score"] + context_boost, 1)
            if context_boost > 5.0:
                node["context_aligned"] = True

        ranked_nodes.sort(key=lambda n: n["priority_score"], reverse=True)

        # Cluster grouping
        clusters: Dict[str, List[Dict[str, Any]]] = {}
        for n in ranked_nodes:
            dom = n.get("domain", "general").capitalize()
            clusters.setdefault(dom, []).append(n)

        # Immediate actionable focus
        immediate_task = ranked_nodes[0] if ranked_nodes else None
        immediate_text = immediate_task["text"] if immediate_task else None

        reason = "Topological graph ordering"
        if immediate_task and immediate_task.get("is_prerequisite"):
            reason = "Tagged as explicit prerequisite '(first)'"
        elif immediate_task and immediate_task.get("dependents"):
            reason = f"Unblocks {len(immediate_task['dependents'])} downstream tasks"

        # Check if remote thinking model is available
        if self.api_key and focus_text:
            try:
                llm_response = self._call_thinking_model(ranked_nodes, focus_dict)
                if llm_response:
                    return llm_response
            except Exception as e:
                logger.debug(f"Thinking model fallback used: {e}")

        return {
            "immediate_focus": immediate_text,
            "rezoning_suggestion": reason,
            "ranked_tasks": ranked_nodes,
            "clusters": clusters,
            "mode": "dag_topological_heuristic",
        }

    def _call_thinking_model(self, ranked_nodes: List[Dict[str, Any]], focus: Dict[str, str]) -> Optional[Dict[str, Any]]:
        url = f"{self.OPENROUTER_URL}/chat/completions"
        prompt = (
            "Analyze current workflow context and prioritized task graph.\n"
            f"Active Focus: {json.dumps(focus)}\n"
            f"Top Tasks: {json.dumps([n['text'] for n in ranked_nodes[:5]])}\n"
            "Return valid JSON only with keys: immediate_focus, rezoning_suggestion, ranked_tasks."
        )
        payload = {
            "model": self.thinking_model,
            "messages": [
                {"role": "system", "content": "You are a workflow reasoning engine. Output valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.2,
        }
        req = urllib.request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            parsed["mode"] = "nemotron_thinking_live"
            return parsed

    async def start(self, interval_seconds: int = 30) -> None:
        """Starts 24/7 background evaluation loop."""
        self._running = True
        self._task = asyncio.create_task(self._background_loop(interval_seconds))
        logger.info("24/7 ServerWorkflowAgent loop started.")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ServerWorkflowAgent loop stopped.")

    async def _background_loop(self, interval_seconds: int) -> None:
        while self._running:
            try:
                await self.evaluate_and_dispatch()
            except Exception as e:
                logger.error(f"Error in agent evaluation loop: {e}")
            await asyncio.sleep(interval_seconds)

    async def evaluate_and_dispatch(self) -> None:
        """Evaluates open tasks and dispatches alerts when top priority changes."""
        tasks = self.db.get_tasks(status="open")
        if not tasks:
            return

        # Find latest active focus from any registered device
        devices = self.db.get_active_devices()
        focus = devices[0]["active_focus"] if devices else {}

        priorities = self.prioritize(tasks, focus)
        top_task = priorities.get("immediate_focus")

        # If top task has changed, dispatch real-time reminder to connected devices
        if top_task and top_task != self._last_top_task_id:
            self._last_top_task_id = top_task
            if self.broadcast_callback:
                msg = {
                    "event": "task_reminder",
                    "title": "Top Focus Alert",
                    "message": f"Immediate Priority: {top_task}",
                    "reason": priorities.get("rezoning_suggestion", "Topological priority"),
                    "priority": "high",
                }
                res = self.broadcast_callback(msg)
                if hasattr(res, "__await__"):
                    await res
