"""PostgreSQL database layer with connection pooling and atomic transactions.

Supports `psycopg` (in Docker containers) and native `libpq` via `ctypes` (for
host execution without C-extension build dependencies), executing identical
PostgreSQL SQL, row-level `FOR UPDATE` locks, and ACID transactions.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import glob
import json
import os
import re
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ NULL
);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);

CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_seq BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_runs_project_created ON runs(project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ingested_batches (
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    batch_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, batch_id)
);

CREATE TABLE IF NOT EXISTS metrics (
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step INTEGER NOT NULL CHECK (step >= 0),
    seq BIGINT NOT NULL,
    loss DOUBLE PRECISION NOT NULL,
    lr DOUBLE PRECISION,
    grad_norm DOUBLE PRECISION,
    custom_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    client_ts TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, step),
    UNIQUE (run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_metrics_run_seq ON metrics(run_id, seq ASC);
"""


class DatabaseError(RuntimeError):
    """Raised when a PostgreSQL query or connection fails."""


# --- Native libpq bindings (used when psycopg is not installed) ---

_CONNECTION_OK = 0
_PGRES_COMMAND_OK = 1
_PGRES_TUPLES_OK = 2

_INT_OIDS = {20, 21, 23}          # int8, int2, int4
_FLOAT_OIDS = {700, 701, 1700}   # float4, float8, numeric
_BOOL_OIDS = {16}                # bool
_JSON_OIDS = {114, 3802}         # json, jsonb


def _load_libpq() -> ctypes.CDLL:
    candidates = [
        "libpq.so.5",
        "libpq.so",
        "/usr/lib64/libpq.so.5",
        "/usr/lib/x86_64-linux-gnu/libpq.so.5",
    ] + sorted(glob.glob("/usr/lib64/libpq.so*"), reverse=True)
    last_err: Optional[Exception] = None
    for path in candidates:
        try:
            pq = ctypes.CDLL(path)
            pq.PQconnectdb.argtypes = [ctypes.c_char_p]
            pq.PQconnectdb.restype = ctypes.c_void_p
            pq.PQstatus.argtypes = [ctypes.c_void_p]
            pq.PQstatus.restype = ctypes.c_int
            pq.PQerrorMessage.argtypes = [ctypes.c_void_p]
            pq.PQerrorMessage.restype = ctypes.c_char_p
            pq.PQfinish.argtypes = [ctypes.c_void_p]
            pq.PQfinish.restype = None
            pq.PQexec.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            pq.PQexec.restype = ctypes.c_void_p
            pq.PQexecParams.argtypes = [
                ctypes.c_void_p,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_char_p),
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_int,
            ]
            pq.PQexecParams.restype = ctypes.c_void_p
            pq.PQresultStatus.argtypes = [ctypes.c_void_p]
            pq.PQresultStatus.restype = ctypes.c_int
            pq.PQresultErrorMessage.argtypes = [ctypes.c_void_p]
            pq.PQresultErrorMessage.restype = ctypes.c_char_p
            pq.PQntuples.argtypes = [ctypes.c_void_p]
            pq.PQntuples.restype = ctypes.c_int
            pq.PQnfields.argtypes = [ctypes.c_void_p]
            pq.PQnfields.restype = ctypes.c_int
            pq.PQfname.argtypes = [ctypes.c_void_p, ctypes.c_int]
            pq.PQfname.restype = ctypes.c_char_p
            pq.PQftype.argtypes = [ctypes.c_void_p, ctypes.c_int]
            pq.PQftype.restype = ctypes.c_uint
            pq.PQgetvalue.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
            pq.PQgetvalue.restype = ctypes.c_char_p
            pq.PQgetisnull.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
            pq.PQgetisnull.restype = ctypes.c_int
            pq.PQclear.argtypes = [ctypes.c_void_p]
            pq.PQclear.restype = None
            return pq
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"Could not load libpq shared library: {last_err}")


def _decode_value(raw: bytes, oid: int) -> Any:
    text = raw.decode("utf-8")
    if oid in _INT_OIDS:
        return int(text)
    if oid in _FLOAT_OIDS:
        return float(text)
    if oid in _BOOL_OIDS:
        return text in ("t", "true", "1")
    if oid in _JSON_OIDS:
        return json.loads(text)
    return text


class _LibpqConnection:
    """Synchronous PostgreSQL connection backed by native libpq."""

    def __init__(self, pq: ctypes.CDLL, dsn: str) -> None:
        self._pq = pq
        self._dsn = dsn
        self._conn = pq.PQconnectdb(dsn.encode("utf-8"))
        if not self._conn or pq.PQstatus(self._conn) != _CONNECTION_OK:
            err = (
                pq.PQerrorMessage(self._conn).decode("utf-8", errors="replace")
                if self._conn
                else "PQconnectdb returned NULL"
            )
            if self._conn:
                pq.PQfinish(self._conn)
                self._conn = None
            raise DatabaseError(f"PostgreSQL connection failed: {err.strip()}")

    def close(self) -> None:
        if self._conn:
            self._pq.PQfinish(self._conn)
            self._conn = None

    def exec_raw(self, sql: str) -> None:
        res = self._pq.PQexec(self._conn, sql.encode("utf-8"))
        if not res:
            raise DatabaseError("PQexec returned NULL")
        try:
            status = self._pq.PQresultStatus(res)
            if status not in (_PGRES_COMMAND_OK, _PGRES_TUPLES_OK):
                err = self._pq.PQresultErrorMessage(res).decode("utf-8", errors="replace")
                raise DatabaseError(err.strip())
        finally:
            self._pq.PQclear(res)

    def query(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        n_params = len(params)
        c_params_Type = ctypes.c_char_p * max(n_params, 1)
        c_params = c_params_Type()
        encoded_Holder: List[bytes] = []
        for idx, val in enumerate(params):
            if val is None:
                c_params[idx] = None
            elif isinstance(val, (dict, list)):
                b = json.dumps(val).encode("utf-8")
                encoded_Holder.append(b)
                c_params[idx] = ctypes.c_char_p(b)
            elif isinstance(val, bool):
                b = (b"true" if val else b"false")
                encoded_Holder.append(b)
                c_params[idx] = ctypes.c_char_p(b)
            else:
                b = str(val).encode("utf-8")
                encoded_Holder.append(b)
                c_params[idx] = ctypes.c_char_p(b)

        res = self._pq.PQexecParams(
            self._conn,
            sql.encode("utf-8"),
            n_params,
            None,
            c_params if n_params > 0 else None,
            None,
            None,
            0,
        )
        if not res:
            raise DatabaseError("PQexecParams returned NULL")
        try:
            status = self._pq.PQresultStatus(res)
            if status == _PGRES_COMMAND_OK:
                return []
            if status != _PGRES_TUPLES_OK:
                err = self._pq.PQresultErrorMessage(res).decode("utf-8", errors="replace")
                raise DatabaseError(err.strip())

            n_rows = self._pq.PQntuples(res)
            n_cols = self._pq.PQnfields(res)
            col_names = [
                self._pq.PQfname(res, col).decode("utf-8") for col in range(n_cols)
            ]
            col_oids = [int(self._pq.PQftype(res, col)) for col in range(n_cols)]

            rows: List[Dict[str, Any]] = []
            for r in range(n_rows):
                row_dict: Dict[str, Any] = {}
                for c in range(n_cols):
                    if self._pq.PQgetisnull(res, r, c):
                        row_dict[col_names[c]] = None
                    else:
                        raw_val = self._pq.PQgetvalue(res, r, c)
                        row_dict[col_names[c]] = _decode_value(raw_val, col_oids[c])
                rows.append(row_dict)
            return rows
        finally:
            self._pq.PQclear(res)


_PARAM_RE = re.compile(r"\$(\d+)")


def _dollar_to_percent_s(sql: str, params: Sequence[Any]) -> tuple[str, List[Any]]:
    """Convert PostgreSQL `$1, $2` placeholders to `%s` for psycopg."""
    ordered_params: List[Any] = []

    def _repl(match: re.Match[str]) -> str:
        idx = int(match.group(1)) - 1
        val = params[idx]
        if isinstance(val, (dict, list)):
            val = json.dumps(val)
        ordered_params.append(val)
        return "%s"

    converted = _PARAM_RE.sub(_repl, sql)
    return converted, ordered_params


class Transaction:
    """Represents an active database connection or transaction."""

    def __init__(self, conn: Any, backend: str, lock: asyncio.Lock) -> None:
        self._conn = conn
        self._backend = backend
        self._lock = lock

    async def execute(self, sql: str, *params: Any) -> None:
        await self.fetch_all(sql, *params)

    async def fetch_one(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        rows = await self.fetch_all(sql, *params)
        return rows[0] if rows else None

    async def fetch_all(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        async with self._lock:
            if self._backend == "psycopg":
                pg_sql, pg_params = _dollar_to_percent_s(sql, params)
                async with self._conn.cursor() as cur:
                    await cur.execute(pg_sql, pg_params)
                    if cur.description is None:
                        return []
                    cols = [desc.name for desc in cur.description]
                    raw_rows = await cur.fetchall()
                    return [dict(zip(cols, r)) for r in raw_rows]
            else:
                return await asyncio.to_thread(self._conn.query, sql, params)


class Database:
    """Async PostgreSQL connection pool supporting psycopg and native libpq."""

    def __init__(self, dsn: Optional[str] = None, pool_size: int = 24) -> None:
        self.dsn = dsn or os.environ.get(
            "DATABASE_URL", "postgresql://soup:soup@localhost:5432/soup_tracker"
        )
        self.pool_size = pool_size
        self._backend = "libpq"
        self._psycopg_pool: Any = None
        self._libpq: Optional[ctypes.CDLL] = None
        self._queue: Optional[asyncio.Queue[_LibpqConnection]] = None
        self._all_conns: List[_LibpqConnection] = []
        self._initialized = False

    async def connect(self) -> None:
        if self._initialized:
            return
        try:
            import psycopg_pool  # type: ignore

            self._backend = "psycopg"
            self._psycopg_pool = psycopg_pool.AsyncConnectionPool(
                conninfo=self.dsn,
                min_size=4,
                max_size=self.pool_size,
                open=False,
            )
            await self._psycopg_pool.open()
        except ImportError:
            self._backend = "libpq"
            self._libpq = _load_libpq()
            self._queue = asyncio.Queue(maxsize=self.pool_size)
            for _ in range(self.pool_size):
                conn = await asyncio.to_thread(_LibpqConnection, self._libpq, self.dsn)
                self._all_conns.append(conn)
                self._queue.put_nowait(conn)

        self._initialized = True
        await self.init_schema()

    async def init_schema(self) -> None:
        if self._backend == "psycopg":
            async with self._psycopg_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(SCHEMA_SQL)
                await conn.commit()
        else:
            assert self._queue is not None
            conn = await self._queue.get()
            try:
                await asyncio.to_thread(conn.exec_raw, SCHEMA_SQL)
            finally:
                self._queue.put_nowait(conn)

    async def close(self) -> None:
        if not self._initialized:
            return
        if self._backend == "psycopg" and self._psycopg_pool is not None:
            await self._psycopg_pool.close()
        elif self._backend == "libpq":
            for conn in self._all_conns:
                await asyncio.to_thread(conn.close)
            self._all_conns.clear()
        self._initialized = False

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[Transaction]:
        """Borrow a connection from the pool in autocommit mode for reads."""
        if not self._initialized:
            await self.connect()
        if self._backend == "psycopg":
            async with self._psycopg_pool.connection() as conn:
                await conn.set_autocommit(True)
                yield Transaction(conn, "psycopg", asyncio.Lock())
        else:
            assert self._queue is not None
            conn = await self._queue.get()
            try:
                yield Transaction(conn, "libpq", asyncio.Lock())
            finally:
                self._queue.put_nowait(conn)

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        """Execute an atomic PostgreSQL transaction (`BEGIN` ... `COMMIT`/`ROLLBACK`)."""
        if not self._initialized:
            await self.connect()
        if self._backend == "psycopg":
            async with self._psycopg_pool.connection() as conn:
                await conn.set_autocommit(False)
                tx = Transaction(conn, "psycopg", asyncio.Lock())
                try:
                    yield tx
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise
        else:
            assert self._queue is not None
            conn = await self._queue.get()
            tx = Transaction(conn, "libpq", asyncio.Lock())
            try:
                await asyncio.to_thread(conn.exec_raw, "BEGIN")
                try:
                    yield tx
                except Exception:
                    await asyncio.to_thread(conn.exec_raw, "ROLLBACK")
                    raise
                else:
                    await asyncio.to_thread(conn.exec_raw, "COMMIT")
            finally:
                self._queue.put_nowait(conn)

    async def fetch_all(self, sql: str, *params: Any) -> List[Dict[str, Any]]:
        async with self.connection() as conn:
            return await conn.fetch_all(sql, *params)

    async def fetch_one(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        async with self.connection() as conn:
            return await conn.fetch_one(sql, *params)

    async def execute(self, sql: str, *params: Any) -> None:
        async with self.connection() as conn:
            await conn.execute(sql, *params)
