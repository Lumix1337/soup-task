"""Shared test fixtures and real PostgreSQL + Uvicorn server harness.

Works seamlessly with both `pytest` (inside Docker Compose) and standard
`python3 -m unittest` (on bare hosts). If `DATABASE_URL` is not already set to a
running PostgreSQL server, automatically initializes an ephemeral PostgreSQL
instance using `initdb` and `pg_ctl`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, AsyncIterator, Dict, Optional
import urllib.parse

import httpx
import uvicorn

from app.db import Database
from app.main import TrackerApp
from app.pubsub import PubSubBroker


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class EphemeralPostgres:
    """Manages a temporary PostgreSQL cluster when no external DB is running."""

    def __init__(self) -> None:
        self.pg_dir: Optional[str] = None
        self.port: int = 0
        self.dsn: str = ""
        self._external: bool = False

    def start(self) -> str:
        env_dsn = os.environ.get("DATABASE_URL")
        if env_dsn and self._can_connect(env_dsn):
            self._external = True
            self.dsn = env_dsn
            return self.dsn

        self.pg_dir = tempfile.mkdtemp(prefix="soup_pg_")
        self.port = get_free_port()
        subprocess.run(
            ["initdb", "-D", self.pg_dir, "-A", "trust", "-U", "soup", "--no-instructions"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "pg_ctl",
                "-D",
                self.pg_dir,
                "-l",
                os.path.join(self.pg_dir, "postgres.log"),
                "-o",
                f"-k /tmp -p {self.port} -h 127.0.0.1 -N 100",
                "-w",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "psql",
                "-h",
                "127.0.0.1",
                "-p",
                str(self.port),
                "-U",
                "soup",
                "-d",
                "postgres",
                "-c",
                "CREATE DATABASE soup_tracker;",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.dsn = f"postgresql://soup@127.0.0.1:{self.port}/soup_tracker"
        os.environ["DATABASE_URL"] = self.dsn
        return self.dsn

    @staticmethod
    def _can_connect(dsn: str) -> bool:
        try:
            parsed = urllib.parse.urlparse(dsn)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 5432
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            return False

    def stop(self) -> None:
        if self._external or not self.pg_dir:
            return
        subprocess.run(
            ["pg_ctl", "-D", self.pg_dir, "-m", "immediate", "stop"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        shutil.rmtree(self.pg_dir, ignore_errors=True)
        self.pg_dir = None


class LiveTrackerServer:
    """Runs TrackerApp inside a real background Uvicorn HTTP/SSE server."""

    def __init__(self, dsn: str, pool_size: int = 32) -> None:
        self.dsn = dsn
        self.port = get_free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.db = Database(dsn=dsn, pool_size=pool_size)
        self.broker = PubSubBroker()
        self.app = TrackerApp(db=self.db, broker=self.broker)
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        config = uvicorn.Config(
            self.app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._server.serve())

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                r = httpx.get(f"{self.base_url}/health", timeout=0.5)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.05)
        raise RuntimeError("LiveTrackerServer failed to start within 10s")

    async def reset_tables(self) -> None:
        await self.db.execute(
            "TRUNCATE TABLE metrics, ingested_batches, runs, api_keys, projects CASCADE"
        )
        self.broker.revoked_key_hashes.clear()
        self.broker.overflow_drops = 0
        self.broker.forced_disconnects = 0
        self.app.audit_logs.clear()

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5.0)


@contextlib.asynccontextmanager
async def running_test_server(pool_size: int = 32) -> AsyncIterator[LiveTrackerServer]:
    pg = EphemeralPostgres()
    dsn = pg.start()
    server = LiveTrackerServer(dsn=dsn, pool_size=pool_size)
    server.start()
    try:
        await server.reset_tables()
        yield server
    finally:
        server.stop()
        pg.stop()
