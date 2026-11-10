"""In-memory pub/sub broker for live SSE streams with bounded queues.

Provides:
- Subscribe-before-DB-query registration to prevent missed events on catch-up.
- Bounded per-subscriber queues (`asyncio.Queue(maxsize=...)`) with non-blocking
  publish (`put_nowait`) so slow consumers never block ingestion or exhaust RAM.
- Immediate termination of open SSE streams when an API key is rotated/revoked.
- Deterministic disconnect signaling for E2E network drop testing.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterable, List, Set

DEFAULT_QUEUE_MAXSIZE = 64


@dataclass(eq=False)
class Subscriber:
    run_id: str
    project_id: str
    key_hash: str
    maxsize: int = DEFAULT_QUEUE_MAXSIZE
    queue: asyncio.Queue[Any] = field(init=False)
    closed: bool = False
    overflowed: bool = False

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=max(1, self.maxsize))

    def send_signal(self, signal_name: str) -> None:
        """Immediately halt metric delivery and enqueue a terminal control signal.

        Clearing unsent queued metrics before inserting `"OVERFLOW"` guarantees
        we never drop the head of the FIFO queue while delivering a later `seq`
        at the tail (which would advance the client's `Last-Event-ID` past an
        undelivered sequence number).
        """
        if self.closed:
            return
        self.closed = True
        while not self.queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
        with contextlib.suppress(asyncio.QueueFull):
            self.queue.put_nowait(signal_name)


class PubSubBroker:
    """Manages active SSE subscribers scoped by `run_id` and `key_hash`."""

    def __init__(self) -> None:
        self._by_run: Dict[str, Set[Subscriber]] = {}
        self._by_key: Dict[str, Set[Subscriber]] = {}
        self.revoked_key_hashes: Set[str] = set()
        self.overflow_drops: int = 0
        self.forced_disconnects: int = 0

    @contextlib.asynccontextmanager
    async def subscribe(
        self,
        run_id: str,
        project_id: str,
        key_hash: str,
        maxsize: int = DEFAULT_QUEUE_MAXSIZE,
    ) -> AsyncIterator[Subscriber]:
        sub = Subscriber(
            run_id=run_id,
            project_id=project_id,
            key_hash=key_hash,
            maxsize=maxsize,
        )
        self._by_run.setdefault(run_id, set()).add(sub)
        self._by_key.setdefault(key_hash, set()).add(sub)
        if key_hash in self.revoked_key_hashes:
            sub.send_signal("REVOKED")
        try:
            yield sub
        finally:
            sub.closed = True
            run_subs = self._by_run.get(run_id)
            if run_subs is not None:
                run_subs.discard(sub)
                if not run_subs:
                    self._by_run.pop(run_id, None)
            key_subs = self._by_key.get(key_hash)
            if key_subs is not None:
                key_subs.discard(sub)
                if not key_subs:
                    self._by_key.pop(key_hash, None)

    def publish(self, run_id: str, project_id: str, events: Iterable[Dict[str, Any]]) -> int:
        """Non-blockingly push newly committed metric events to subscribers of `run_id`."""
        event_list: List[Dict[str, Any]] = list(events)
        if not event_list:
            return 0
        subs = list(self._by_run.get(run_id, ()))
        delivered = 0
        for sub in subs:
            if sub.closed or sub.project_id != project_id:
                continue
            if sub.key_hash in self.revoked_key_hashes:
                sub.send_signal("REVOKED")
                continue
            for ev in event_list:
                try:
                    sub.queue.put_nowait(ev)
                    delivered += 1
                except asyncio.QueueFull:
                    sub.overflowed = True
                    self.overflow_drops += 1
                    sub.send_signal("OVERFLOW")
                    break
        return delivered

    def revoke_key(self, key_hash: str) -> int:
        """Mark `key_hash` as revoked and immediately signal all its open SSE streams."""
        self.revoked_key_hashes.add(key_hash)
        subs = list(self._by_key.get(key_hash, ()))
        for sub in subs:
            sub.send_signal("REVOKED")
        return len(subs)

    def force_disconnect_run(self, run_id: str) -> int:
        """Forcibly drop active SSE streams for `run_id` (used in E2E drop/reconnect tests)."""
        subs = list(self._by_run.get(run_id, ()))
        for sub in subs:
            self.forced_disconnects += 1
            sub.send_signal("FORCE_DISCONNECT")
        return len(subs)

    def active_subscriber_count(self, run_id: str | None = None) -> int:
        if run_id is not None:
            return len(self._by_run.get(run_id, ()))
        return sum(len(s) for s in self._by_run.values())
