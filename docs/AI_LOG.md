# AI Tools Usage & Verification Log

Per the Soup take-home instructions (*"Note where you used them, which suggestions you accepted, and what you verified, changed, or redid yourself"*):

## 1. Where AI Tools Were Used
- Drafting initial boilerplate for the PostgreSQL schema, ASGI request routing, and SVG chart rendering in `app/static/app.js`.
- Structuring the test scenarios for out-of-order/duplicate batch arrival, tenant isolation checks, and load benchmarking.

## 2. AI Suggestions Accepted
- **Per-run row locking (`SELECT ... FOR UPDATE` on `runs`):** Accepted the suggestion to maintain a per-run `last_seq` counter on the `runs` table locked during batch ingestion. This prevents cross-run lock contention across 50 concurrent runs while serializing `seq` allocation within a single run.
- **Subscribe-before-query SSE catch-up:** Accepted registering the in-memory `asyncio.Queue` subscriber *before* querying the PostgreSQL backlog (`seq > Last-Event-ID`), then deduplicating queue items with `seq <= high_water_seq`.

## 3. AI Suggestions Rejected, Verified, or Rewritten
1. **Rejected naive catch-up cursor by `step > last_seen_step`:**
   - *Initial AI suggestion:* Filter missed SSE events on reconnect using `WHERE step > last_step`.
   - *Why it was broken:* When metrics arrive out of order (e.g., `step=10` arrives before a disconnect, and delayed `step=5` arrives during the disconnect), `step > 10` permanently drops `step=5`, causing UI state to diverge from database state.
   - *Fix implemented:* Decoupled arrival sequence (`seq`, strictly monotonic per run) from domain order (`step`). Catch-up queries `WHERE seq > Last-Event-ID ORDER BY seq ASC`, while the UI sorts by `step ASC`.
2. **Rejected passing API keys in SSE query parameters (`?api_key=...`) with native `EventSource`:**
   - *Initial AI suggestion:* Because browser `EventSource` does not support custom headers, pass `?api_key=...` in the stream URL.
   - *Why it was broken:* URL query strings are logged in plaintext by reverse proxies and ASGI access logs, violating Requirement 3 (*"What else could leak: logs"*).
   - *Fix implemented:* Replaced native `EventSource` with a `fetch` + `ReadableStream` SSE client in `app/static/app.js` that sends `X-API-Key` and `Last-Event-ID` via HTTP headers, and explicitly blocked `?api_key=` on the server with `400 Bad Request`.
3. **Fixed PostgreSQL `SERIAL` gap & MVCC visibility bug on duplicate batches:**
   - *Initial AI suggestion:* Use a table-wide `BIGSERIAL` column with `ON CONFLICT (run_id, step) DO NOTHING`.
   - *Why it was broken:* In PostgreSQL, `ON CONFLICT DO NOTHING` still increments `SEQUENCE`, creating gaps in `seq` on every duplicate retry, and concurrent transactions can commit `seq=11` before `seq=10` becomes visible to a reader.
   - *Fix implemented:* Assigned `seq` only to newly inserted points using `runs.last_seq` under the per-run `FOR UPDATE` lock. Duplicate steps do not advance `last_seq`, making `seq` strictly gapless (`1..N`) per run so clients can detect any lost event via `seq != prev_seq + 1`.
4. **Fixed active SSE stream survival after API key rotation:**
   - *Initial AI suggestion:* Check API key validity only once when the HTTP request starts.
   - *Why it was broken:* An already-open SSE stream would continue receiving live metrics after its API key was rotated.
   - *Fix implemented:* `PubSubBroker.revoke_key(key_hash)` tracks revoked key hashes in memory and immediately pushes a `"REVOKED"` control signal into every open subscriber queue bound to that key, terminating the stream before any subsequent metric can be emitted.
5. **Caught & fixed FIFO `asyncio.Queue` head-drop bug on slow-client overflow (`app/pubsub.py`):**
   - *Initial AI suggestion:* When `queue.put_nowait("OVERFLOW")` encounters `QueueFull`, pop one item via `queue.get_nowait()` to make room for the `"OVERFLOW"` sentinel.
   - *Why it was broken:* `asyncio.Queue` is FIFO. Calling `get_nowait()` drops the *oldest* unsent event (`seq = k + 1`) at the head of the queue while leaving later events (`seq = k + 2, k + 3`) ahead of `"OVERFLOW"`. The SSE loop would deliver `seq = k + 2` and advance the client's `Last-Event-ID` past the dropped `seq = k + 1`, permanently losing an event!
   - *Fix implemented:* On `"OVERFLOW"` (or `"REVOKED"`), `Subscriber.send_signal()` clears all unsent items from the queue before enqueuing `"OVERFLOW"`, ensuring the client's `Last-Event-ID` stays at the exact contiguous `high_water_seq = k` already delivered, so reconnecting fetches `seq > k` with zero gaps.
6. **Caught Python 3.8+ `asyncio.CancelledError` inheritance in SSE cleanup (`app/main.py`):**
   - *Initial AI suggestion:* `with contextlib.suppress(Exception): await disconnect_task`.
   - *Why it was broken:* In modern Python, `asyncio.CancelledError` inherits from `BaseException`, not `Exception`, causing an unhandled traceback in Uvicorn when a server-terminated SSE stream cleaned up its disconnect listener.
   - *Fix implemented:* Changed to `with contextlib.suppress(BaseException): await disconnect_task`.
