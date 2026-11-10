"""ASGI application for Soup Mini Experiment Tracker.

Implements all REST and live SSE endpoints with:
- Multi-tenant API key authentication, key rotation, and query-param key blocking
- Indistinguishable 404 responses for cross-project and non-existent run UUIDs
- Row-locked (`FOR UPDATE`) atomic batch metric ingestion with gapless per-run `seq`
- Subscribe-before-query SSE streaming with `Last-Event-ID` catch-up and bounded queues
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple
from urllib.parse import parse_qs
import uuid

from app.auth import (
    AuthContext,
    AuthError,
    DUMMY_UUID,
    generate_api_key,
    normalize_uuid,
    redact_secrets,
    verify_api_key,
)
from app.db import Database
from app.pubsub import DEFAULT_QUEUE_MAXSIZE, PubSubBroker

STATIC_DIR = Path(__file__).parent / "static"


class _RedactingLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.args:
            record.msg = redact_secrets(str(record.msg))
            record.args = tuple(
                redact_secrets(str(a)) if isinstance(a, str) else a for a in record.args
            )
        else:
            record.msg = redact_secrets(str(record.msg))
        return True


logger = logging.getLogger("soup_tracker")
logger.addFilter(_RedactingLogFilter())

SECURITY_HEADERS: List[Tuple[bytes, bytes]] = [
    (b"cache-control", b"private, no-store"),
    (b"vary", b"X-API-Key, Authorization"),
    (b"x-content-type-options", b"nosniff"),
]


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _is_finite_number(val: Any) -> bool:
    return isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(float(val))


def _floats_equal(a: Optional[float], b: Optional[float]) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-12)


def _validate_and_canonicalize_batch(body: Any) -> Tuple[Optional[str], List[Dict[str, Any]], str]:
    """Validate batch payload before any database write (all-or-nothing)."""
    if not isinstance(body, dict):
        raise HTTPException(422, "Request body must be a JSON object")

    batch_id = body.get("batch_id")
    if batch_id is not None:
        if not isinstance(batch_id, str) or not batch_id.strip():
            raise HTTPException(422, "batch_id must be a non-empty string when provided")
        batch_id = batch_id.strip()

    raw_metrics = body.get("metrics")
    if not isinstance(raw_metrics, list) or len(raw_metrics) == 0:
        raise HTTPException(422, "metrics must be a non-empty list")

    dedup_by_step: Dict[int, Dict[str, Any]] = {}
    ordered_unique: List[Dict[str, Any]] = []

    for idx, item in enumerate(raw_metrics):
        if not isinstance(item, dict):
            raise HTTPException(422, f"Metric at index {idx} must be an object")

        step = item.get("step")
        if isinstance(step, bool) or not isinstance(step, int) or step < 0:
            raise HTTPException(422, f"Invalid step at index {idx}: must be integer >= 0")

        loss = item.get("loss")
        if not _is_finite_number(loss):
            raise HTTPException(422, f"Invalid loss at step {step}: must be a finite number")
        loss_f = float(loss)

        lr = item.get("lr")
        if lr is not None and not _is_finite_number(lr):
            raise HTTPException(422, f"Invalid lr at step {step}: must be a finite number or null")
        lr_f = float(lr) if lr is not None else None

        grad_norm = item.get("grad_norm")
        if grad_norm is not None and not _is_finite_number(grad_norm):
            raise HTTPException(
                422, f"Invalid grad_norm at step {step}: must be a finite number or null"
            )
        grad_norm_f = float(grad_norm) if grad_norm is not None else None

        custom_metrics = item.get("custom_metrics")
        if custom_metrics is None:
            custom_metrics = {}
        if not isinstance(custom_metrics, dict):
            raise HTTPException(422, f"custom_metrics at step {step} must be an object")
        for k, v in custom_metrics.items():
            if not isinstance(k, str) or (
                v is not None and not isinstance(v, (str, bool)) and not _is_finite_number(v)
            ):
                raise HTTPException(422, f"Invalid custom_metrics value at step {step}")

        client_ts = item.get("client_ts")
        if client_ts is not None and not isinstance(client_ts, str):
            raise HTTPException(422, f"client_ts at step {step} must be an ISO-8601 string")

        normalized = {
            "step": step,
            "loss": loss_f,
            "lr": lr_f,
            "grad_norm": grad_norm_f,
            "custom_metrics": custom_metrics,
            "client_ts": client_ts,
        }

        if step in dedup_by_step:
            prev = dedup_by_step[step]
            if (
                not _floats_equal(prev["loss"], normalized["loss"])
                or not _floats_equal(prev["lr"], normalized["lr"])
                or not _floats_equal(prev["grad_norm"], normalized["grad_norm"])
                or prev["custom_metrics"] != normalized["custom_metrics"]
            ):
                raise HTTPException(
                    422, f"Conflicting values for duplicate step {step} within the same batch"
                )
        else:
            dedup_by_step[step] = normalized
            ordered_unique.append(normalized)

    canonical_json = json.dumps(
        [
            {
                "step": m["step"],
                "loss": m["loss"],
                "lr": m["lr"],
                "grad_norm": m["grad_norm"],
                "custom_metrics": m["custom_metrics"],
            }
            for m in ordered_unique
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    payload_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return batch_id, ordered_unique, payload_hash


class TrackerApp:
    """Main ASGI application for the Soup Mini Experiment Tracker."""

    def __init__(self, db: Optional[Database] = None, broker: Optional[PubSubBroker] = None) -> None:
        self.db = db or Database()
        self.broker = broker or PubSubBroker()
        self.audit_logs: List[str] = []

    def _log(self, message: str) -> None:
        safe_msg = redact_secrets(message)
        self.audit_logs.append(safe_msg)
        if len(self.audit_logs) > 1000:
            del self.audit_logs[:500]
        logger.info(safe_msg)

    async def __call__(self, scope: Dict[str, Any], receive: Callable, send: Callable) -> None:
        if scope["type"] == "lifespan":
            while True:
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    try:
                        await self.db.connect()
                        await send({"type": "lifespan.startup.complete"})
                    except Exception as exc:
                        await send({"type": "lifespan.startup.failed", "message": str(exc)})
                        return
                elif msg["type"] == "lifespan.shutdown":
                    await self.db.close()
                    await send({"type": "lifespan.shutdown.complete"})
                    return
            return

        if scope["type"] != "http":
            return

        if not self.db._initialized:
            await self.db.connect()

        method: str = scope["method"].upper()
        path: str = scope["path"]
        raw_query: bytes = scope.get("query_string", b"")
        query_parsed = parse_qs(raw_query.decode("utf-8", errors="replace"), keep_blank_values=True)
        query_params: Dict[str, str] = {k: v[-1] for k, v in query_parsed.items()}

        headers: Dict[str, str] = {}
        for k_bytes, v_bytes in scope.get("headers", ()):
            headers[k_bytes.decode("latin1").lower()] = v_bytes.decode("latin1")

        try:
            # --- Static & Health Routes ---
            if method == "GET" and path == "/health":
                await self._send_json(send, 200, {"status": "ok"})
                return

            if method == "GET" and path in ("/", "/index.html"):
                await self._send_file(send, STATIC_DIR / "index.html", b"text/html; charset=utf-8")
                return

            if method == "GET" and path == "/static/app.js":
                await self._send_file(
                    send, STATIC_DIR / "app.js", b"application/javascript; charset=utf-8"
                )
                return

            # --- Internal Test Control Routes ---
            if method == "POST" and path.startswith("/_test/disconnect/"):
                run_id = path.split("/_test/disconnect/", 1)[1].strip("/")
                dropped = self.broker.force_disconnect_run(run_id)
                await self._send_json(send, 200, {"run_id": run_id, "disconnected": dropped})
                return

            if method == "GET" and path == "/_test/stats":
                await self._send_json(
                    send,
                    200,
                    {
                        "overflow_drops": self.broker.overflow_drops,
                        "forced_disconnects": self.broker.forced_disconnects,
                        "active_subscribers": self.broker.active_subscriber_count(),
                        "audit_logs": self.audit_logs[-100:],
                    },
                )
                return

            # --- Project Creation (Bootstrap) ---
            if method == "POST" and path == "/projects":
                body = await self._read_json(receive)
                name = (body.get("name") if isinstance(body, dict) else None) or "default-project"
                if not isinstance(name, str) or not name.strip():
                    raise HTTPException(422, "Project name must be a non-empty string")
                project_id = str(uuid.uuid4())
                key_id = str(uuid.uuid4())
                raw_key, key_hash = generate_api_key()
                async with self.db.transaction() as tx:
                    await tx.execute(
                        "INSERT INTO projects (id, name) VALUES ($1, $2)",
                        project_id,
                        name.strip(),
                    )
                    await tx.execute(
                        "INSERT INTO api_keys (id, project_id, key_hash) VALUES ($1, $2, $3)",
                        key_id,
                        project_id,
                        key_hash,
                    )
                self._log(f"Created project {project_id} with key_id {key_id}")
                await self._send_json(
                    send,
                    201,
                    {
                        "project_id": project_id,
                        "name": name.strip(),
                        "key_id": key_id,
                        "api_key": raw_key,
                    },
                )
                return

            # --- All Remaining Routes Require Valid API Key ---
            auth = await verify_api_key(
                self.db, headers, query_params, self.broker.revoked_key_hashes
            )

            # POST /projects/{project_id}/keys/rotate
            if method == "POST" and path.startswith("/projects/") and path.endswith("/keys/rotate"):
                parts = [p for p in path.split("/") if p]
                if len(parts) == 3 and parts[0] == "projects" and parts[2] == "rotate":
                    pass
                target_project_id = path[len("/projects/") : -len("/keys/rotate")].strip("/")
                norm_proj = normalize_uuid(target_project_id)
                if norm_proj is None or norm_proj != auth.project_id:
                    await self.db.fetch_one(
                        "SELECT id FROM projects WHERE id = $1", DUMMY_UUID
                    )
                    raise HTTPException(404, "Project not found")

                new_key_id = str(uuid.uuid4())
                new_raw_key, new_key_hash = generate_api_key()
                async with self.db.transaction() as tx:
                    await tx.execute(
                        "UPDATE api_keys SET revoked_at = NOW() WHERE id = $1 AND project_id = $2",
                        auth.key_id,
                        auth.project_id,
                    )
                    await tx.execute(
                        "INSERT INTO api_keys (id, project_id, key_hash) VALUES ($1, $2, $3)",
                        new_key_id,
                        auth.project_id,
                        new_key_hash,
                    )
                closed_streams = self.broker.revoke_key(auth.key_hash)
                self._log(
                    f"Rotated key {auth.key_id} -> {new_key_id} for project {auth.project_id}"
                )
                await self._send_json(
                    send,
                    200,
                    {
                        "project_id": auth.project_id,
                        "revoked_key_id": auth.key_id,
                        "key_id": new_key_id,
                        "api_key": new_raw_key,
                        "closed_streams": closed_streams,
                    },
                )
                return

            # POST /runs
            if method == "POST" and path == "/runs":
                body = await self._read_json(receive, allow_empty=True)
                if not isinstance(body, dict):
                    raise HTTPException(422, "Request body must be a JSON object")
                run_name = body.get("name") or f"run-{uuid.uuid4().hex[:8]}"
                if not isinstance(run_name, str) or not run_name.strip():
                    raise HTTPException(422, "Run name must be a non-empty string")
                config = body.get("config") or {}
                if not isinstance(config, dict):
                    raise HTTPException(422, "config must be a JSON object")

                run_id = str(uuid.uuid4())
                row = await self.db.fetch_one(
                    """
                    INSERT INTO runs (id, project_id, name, status, config, last_seq)
                    VALUES ($1, $2, $3, 'running', $4::jsonb, 0)
                    RETURNING id, project_id, name, status, config, last_seq, created_at
                    """,
                    run_id,
                    auth.project_id,
                    run_name.strip(),
                    config,
                )
                assert row is not None
                await self._send_json(
                    send,
                    201,
                    {
                        "run_id": str(row["id"]),
                        "project_id": str(row["project_id"]),
                        "name": row["name"],
                        "status": row["status"],
                        "config": row["config"],
                        "last_seq": int(row["last_seq"]),
                        "created_at": str(row["created_at"]),
                    },
                )
                return

            # GET /runs
            if method == "GET" and path == "/runs":
                rows = await self.db.fetch_all(
                    """
                    SELECT id, project_id, name, status, config, last_seq, created_at
                    FROM runs
                    WHERE project_id = $1
                    ORDER BY created_at DESC
                    """,
                    auth.project_id,
                )
                runs_out = [
                    {
                        "run_id": str(r["id"]),
                        "project_id": str(r["project_id"]),
                        "name": r["name"],
                        "status": r["status"],
                        "config": r["config"],
                        "last_seq": int(r["last_seq"]),
                        "created_at": str(r["created_at"]),
                    }
                    for r in rows
                ]
                await self._send_json(send, 200, {"runs": runs_out})
                return

            # Routes under /runs/{run_id}...
            if path.startswith("/runs/"):
                subpath = path[len("/runs/") :].strip("/")
                parts = subpath.split("/")
                raw_run_id = parts[0]
                action = parts[1] if len(parts) > 1 else ""

                if len(parts) > 2 or action not in ("", "metrics", "stream"):
                    raise HTTPException(404, "Not found")

                if action == "" and method == "GET":
                    await self._handle_get_run(send, auth, raw_run_id)
                    return

                if action == "metrics" and method == "POST":
                    await self._handle_post_metrics(receive, send, auth, raw_run_id)
                    return

                if action == "metrics" and method == "GET":
                    await self._handle_get_metrics(send, auth, raw_run_id, query_params)
                    return

                if action == "stream" and method == "GET":
                    await self._handle_stream_sse(
                        receive, send, auth, raw_run_id, headers, query_params
                    )
                    return

            raise HTTPException(404, "Not found")

        except AuthError as exc:
            self._log(f"Auth failure ({exc.status_code}): {exc.detail}")
            await self._send_json(send, exc.status_code, {"detail": exc.detail})
        except HTTPException as exc:
            await self._send_json(send, exc.status_code, {"detail": exc.detail})
        except Exception as exc:
            self._log(f"Unhandled server error: {type(exc).__name__}")
            await self._send_json(send, 500, {"detail": "Internal server error"})

    async def _lookup_run_scoped(
        self, conn: Any, project_id: str, raw_run_id: str, for_update: bool = False
    ) -> Dict[str, Any]:
        """Look up a run by `(run_id, project_id)` with constant query execution."""
        norm_run_id = normalize_uuid(raw_run_id)
        lookup_id = norm_run_id if norm_run_id is not None else DUMMY_UUID
        lock_clause = " FOR UPDATE" if for_update else ""
        row = await conn.fetch_one(
            f"""
            SELECT id, project_id, name, status, config, last_seq, created_at
            FROM runs
            WHERE id = $1 AND project_id = $2{lock_clause}
            """,
            lookup_id,
            project_id,
        )
        if not row or norm_run_id is None:
            raise HTTPException(404, "Run not found")
        return row

    async def _handle_get_run(
        self, send: Callable, auth: AuthContext, raw_run_id: str
    ) -> None:
        row = await self._lookup_run_scoped(self.db, auth.project_id, raw_run_id)
        await self._send_json(
            send,
            200,
            {
                "run_id": str(row["id"]),
                "project_id": str(row["project_id"]),
                "name": row["name"],
                "status": row["status"],
                "config": row["config"],
                "last_seq": int(row["last_seq"]),
                "created_at": str(row["created_at"]),
            },
        )

    async def _handle_get_metrics(
        self,
        send: Callable,
        auth: AuthContext,
        raw_run_id: str,
        query_params: Mapping[str, str],
    ) -> None:
        run_row = await self._lookup_run_scoped(self.db, auth.project_id, raw_run_id)
        run_id = str(run_row["id"])
        after_seq = 0
        if "after_seq" in query_params:
            try:
                after_seq = max(0, int(query_params["after_seq"]))
            except ValueError:
                raise HTTPException(422, "after_seq must be an integer")

        rows = await self.db.fetch_all(
            """
            SELECT step, seq, loss, lr, grad_norm, custom_metrics, client_ts
            FROM metrics
            WHERE run_id = $1 AND seq > $2
            ORDER BY step ASC
            """,
            run_id,
            after_seq,
        )
        metrics_out = [
            {
                "step": int(r["step"]),
                "seq": int(r["seq"]),
                "loss": float(r["loss"]),
                "lr": float(r["lr"]) if r["lr"] is not None else None,
                "grad_norm": float(r["grad_norm"]) if r["grad_norm"] is not None else None,
                "custom_metrics": r["custom_metrics"] or {},
                "client_ts": str(r["client_ts"]) if r["client_ts"] is not None else None,
            }
            for r in rows
        ]
        await self._send_json(
            send,
            200,
            {
                "run_id": run_id,
                "last_seq": int(run_row["last_seq"]),
                "metrics": metrics_out,
            },
        )

    async def _handle_post_metrics(
        self,
        receive: Callable,
        send: Callable,
        auth: AuthContext,
        raw_run_id: str,
    ) -> None:
        body = await self._read_json(receive)
        # Validate run ownership even before or alongside payload check so cross-tenant
        # probes always receive 404 Run not found.
        norm_run_id = normalize_uuid(raw_run_id)
        if norm_run_id is None:
            await self.db.fetch_one(
                "SELECT id FROM runs WHERE id = $1 AND project_id = $2",
                DUMMY_UUID,
                auth.project_id,
            )
            raise HTTPException(404, "Run not found")

        batch_id, unique_metrics, payload_hash = _validate_and_canonicalize_batch(body)

        new_events: List[Dict[str, Any]] = []
        inserted_count = 0
        duplicate_count = 0

        async with self.db.transaction() as tx:
            run_row = await self._lookup_run_scoped(
                tx, auth.project_id, norm_run_id, for_update=True
            )
            run_id = str(run_row["id"])
            last_seq = int(run_row["last_seq"])

            if batch_id is not None:
                existing_batch = await tx.fetch_one(
                    "SELECT payload_hash FROM ingested_batches WHERE run_id = $1 AND batch_id = $2",
                    run_id,
                    batch_id,
                )
                if existing_batch is not None:
                    if existing_batch["payload_hash"] != payload_hash:
                        raise HTTPException(
                            409, "Batch ID already used with a different metric payload"
                        )
                    await self._send_json(
                        send,
                        200,
                        {
                            "run_id": run_id,
                            "inserted": 0,
                            "duplicates": len(unique_metrics),
                            "last_seq": last_seq,
                            "idempotent_replay": True,
                        },
                    )
                    return

            # Fetch any existing steps for this run among the incoming steps
            steps_list = [m["step"] for m in unique_metrics]
            placeholders = ", ".join(f"${i + 2}" for i in range(len(steps_list)))
            existing_rows = await tx.fetch_all(
                f"""
                SELECT step, seq, loss, lr, grad_norm, custom_metrics
                FROM metrics
                WHERE run_id = $1 AND step IN ({placeholders})
                """,
                run_id,
                *steps_list,
            )
            existing_by_step = {int(r["step"]): r for r in existing_rows}

            rows_to_insert: List[Dict[str, Any]] = []
            for m in unique_metrics:
                step = m["step"]
                if step in existing_by_step:
                    ex = existing_by_step[step]
                    if (
                        not _floats_equal(float(ex["loss"]), m["loss"])
                        or not _floats_equal(
                            float(ex["lr"]) if ex["lr"] is not None else None, m["lr"]
                        )
                        or not _floats_equal(
                            float(ex["grad_norm"]) if ex["grad_norm"] is not None else None,
                            m["grad_norm"],
                        )
                        or (ex["custom_metrics"] or {}) != m["custom_metrics"]
                    ):
                        raise HTTPException(
                            409, f"Conflicting metric values for existing step {step}"
                        )
                    duplicate_count += 1
                else:
                    last_seq += 1
                    seq = last_seq
                    inserted_count += 1
                    row_payload = {
                        "run_id": run_id,
                        "step": step,
                        "seq": seq,
                        "loss": m["loss"],
                        "lr": m["lr"],
                        "grad_norm": m["grad_norm"],
                        "custom_metrics": m["custom_metrics"],
                        "client_ts": m["client_ts"],
                    }
                    rows_to_insert.append(row_payload)
                    new_events.append(row_payload)

            if rows_to_insert:
                value_clauses: List[str] = []
                insert_params: List[Any] = []
                p_idx = 1
                for r_ins in rows_to_insert:
                    value_clauses.append(
                        f"(${p_idx}, ${p_idx+1}, ${p_idx+2}, ${p_idx+3}, "
                        f"${p_idx+4}, ${p_idx+5}, ${p_idx+6}::jsonb, ${p_idx+7}::timestamptz)"
                    )
                    insert_params.extend(
                        [
                            r_ins["run_id"],
                            r_ins["step"],
                            r_ins["seq"],
                            r_ins["loss"],
                            r_ins["lr"],
                            r_ins["grad_norm"],
                            r_ins["custom_metrics"],
                            r_ins["client_ts"],
                        ]
                    )
                    p_idx += 8

                await tx.execute(
                    f"""
                    INSERT INTO metrics (
                        run_id, step, seq, loss, lr, grad_norm, custom_metrics, client_ts
                    )
                    VALUES {", ".join(value_clauses)}
                    """,
                    *insert_params,
                )
                await tx.execute(
                    "UPDATE runs SET last_seq = $1 WHERE id = $2",
                    last_seq,
                    run_id,
                )

            if batch_id is not None:
                await tx.execute(
                    """
                    INSERT INTO ingested_batches (run_id, batch_id, payload_hash)
                    VALUES ($1, $2, $3)
                    """,
                    run_id,
                    batch_id,
                    payload_hash,
                )

        if new_events:
            self.broker.publish(run_id, auth.project_id, new_events)

        await self._send_json(
            send,
            200,
            {
                "run_id": run_id,
                "inserted": inserted_count,
                "duplicates": duplicate_count,
                "last_seq": last_seq,
                "idempotent_replay": False,
            },
        )

    async def _handle_stream_sse(
        self,
        receive: Callable,
        send: Callable,
        auth: AuthContext,
        raw_run_id: str,
        headers: Mapping[str, str],
        query_params: Mapping[str, str],
    ) -> None:
        run_row = await self._lookup_run_scoped(self.db, auth.project_id, raw_run_id)
        run_id = str(run_row["id"])

        last_event_id = 0
        raw_last_id = headers.get("last-event-id") or query_params.get("last_event_id")
        if raw_last_id is not None and str(raw_last_id).strip() != "":
            try:
                last_event_id = max(0, int(str(raw_last_id).strip()))
            except ValueError:
                raise HTTPException(422, "Invalid Last-Event-ID header")

        queue_size = DEFAULT_QUEUE_MAXSIZE
        if "queue_size" in query_params:
            try:
                queue_size = max(1, min(4096, int(query_params["queue_size"])))
            except ValueError:
                raise HTTPException(422, "Invalid queue_size parameter")

        # 1. Subscribe to live queue BEFORE querying DB backlog (race-free handoff)
        async with self.broker.subscribe(
            run_id=run_id,
            project_id=auth.project_id,
            key_hash=auth.key_hash,
            maxsize=queue_size,
        ) as subscriber:
            # 2. Fetch missed events from PostgreSQL ordered by `seq ASC`
            backlog_rows = await self.db.fetch_all(
                """
                SELECT step, seq, loss, lr, grad_norm, custom_metrics, client_ts
                FROM metrics
                WHERE run_id = $1 AND seq > $2
                ORDER BY seq ASC
                """,
                run_id,
                last_event_id,
            )

            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"text/event-stream; charset=utf-8"),
                        (b"cache-control", b"private, no-cache, no-store"),
                        (b"vary", b"X-API-Key, Authorization"),
                        (b"connection", b"keep-alive"),
                        (b"x-accel-buffering", b"no"),
                    ],
                }
            )

            async def _send_chunk(text: str, more_body: bool = True) -> None:
                await send(
                    {
                        "type": "http.response.body",
                        "body": text.encode("utf-8"),
                        "more_body": more_body,
                    }
                )

            await _send_chunk(f": connected run_id={run_id} from_seq={last_event_id}\n\n")

            high_water_seq = last_event_id
            for r in backlog_rows:
                if auth.key_hash in self.broker.revoked_key_hashes:
                    await _send_chunk(
                        'event: error\ndata: {"reason": "api_key_revoked"}\n\n',
                        more_body=False,
                    )
                    return
                seq = int(r["seq"])
                payload = {
                    "run_id": run_id,
                    "step": int(r["step"]),
                    "seq": seq,
                    "loss": float(r["loss"]),
                    "lr": float(r["lr"]) if r["lr"] is not None else None,
                    "grad_norm": float(r["grad_norm"]) if r["grad_norm"] is not None else None,
                    "custom_metrics": r["custom_metrics"] or {},
                    "client_ts": str(r["client_ts"]) if r["client_ts"] is not None else None,
                }
                await _send_chunk(
                    f"id: {seq}\nevent: metric\ndata: {json.dumps(payload)}\n\n"
                )
                high_water_seq = max(high_water_seq, seq)

            # 3. Drain live queue, deduplicating any event with `seq <= high_water_seq`
            disconnect_task = asyncio.create_task(self._wait_for_disconnect(receive))
            try:
                while not disconnect_task.done():
                    if auth.key_hash in self.broker.revoked_key_hashes:
                        await _send_chunk(
                            'event: error\ndata: {"reason": "api_key_revoked"}\n\n',
                            more_body=False,
                        )
                        return

                    queue_task = asyncio.create_task(subscriber.queue.get())
                    done, _ = await asyncio.wait(
                        {queue_task, disconnect_task},
                        timeout=10.0,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if disconnect_task in done:
                        queue_task.cancel()
                        break

                    if not done:
                        queue_task.cancel()
                        await _send_chunk(": heartbeat\n\n")
                        continue

                    item = queue_task.result()

                    if item == "REVOKED" or auth.key_hash in self.broker.revoked_key_hashes:
                        await _send_chunk(
                            'event: error\ndata: {"reason": "api_key_revoked"}\n\n',
                            more_body=False,
                        )
                        return

                    if item == "OVERFLOW":
                        err_data = json.dumps(
                            {"reason": "slow_consumer", "last_seq": high_water_seq}
                        )
                        await _send_chunk(
                            f"event: overflow\ndata: {err_data}\n\n",
                            more_body=False,
                        )
                        return

                    if item == "FORCE_DISCONNECT":
                        await _send_chunk("", more_body=False)
                        return

                    if isinstance(item, dict):
                        seq = int(item["seq"])
                        if seq <= high_water_seq:
                            continue
                        await _send_chunk(
                            f"id: {seq}\nevent: metric\ndata: {json.dumps(item)}\n\n"
                        )
                        high_water_seq = seq
            finally:
                disconnect_task.cancel()
                with contextlib.suppress(BaseException):
                    await disconnect_task

    @staticmethod
    async def _wait_for_disconnect(receive: Callable) -> None:
        while True:
            msg = await receive()
            if msg["type"] == "http.disconnect":
                return

    @staticmethod
    async def _read_json(receive: Callable, allow_empty: bool = False) -> Any:
        chunks: List[bytes] = []
        while True:
            msg = await receive()
            body_piece = msg.get("body", b"")
            if body_piece:
                chunks.append(body_piece)
            if not msg.get("more_body", False):
                break
        raw = b"".join(chunks).strip()
        if not raw:
            if allow_empty:
                return {}
            raise HTTPException(422, "Empty request body")
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            raise HTTPException(422, "Invalid JSON payload")

    @staticmethod
    async def _send_json(send: Callable, status: int, data: Any) -> None:
        payload = json.dumps(data).encode("utf-8")
        headers = [
            (b"content-type", b"application/json; charset=utf-8"),
            (b"content-length", str(len(payload)).encode("ascii")),
            *SECURITY_HEADERS,
        ]
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": payload, "more_body": False})

    @staticmethod
    async def _send_file(send: Callable, file_path: Path, content_type: bytes) -> None:
        if not file_path.is_file():
            await TrackerApp._send_json(send, 404, {"detail": "File not found"})
            return
        body = file_path.read_bytes()
        headers = [
            (b"content-type", content_type),
            (b"content-length", str(len(body)).encode("ascii")),
            *SECURITY_HEADERS,
        ]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": body, "more_body": False})


import contextlib  # noqa: E402

app = TrackerApp()
