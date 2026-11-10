"""Requirement 2: End-to-End Consistency Test (UI + API + Database).

Scenario tested:
1. Metrics arrive out of order (steps [0,1,2], then future steps [6,7,8]).
2. Duplicate batch retry (`batch-A` resent) arrives.
3. Live SSE connection drops mid-stream (`POST /_test/disconnect/{run_id}`).
4. While disconnected, late metrics (steps [3,4,5] < step 8) and overlapping
   metrics (steps [4,5,9,10]) arrive at the server.
5. Client reconnects automatically using `Last-Event-ID: <seq>`, catches up on
   missed events (including late steps [3,4,5]), and receives final live steps [11,12].
6. Final UI page state (`window.__trackerState` + rendered DOM `<tr>` rows + SVG
   polyline) is compared against direct PostgreSQL `SELECT ... ORDER BY step ASC`.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import httpx

from tests.conftest import running_test_server

STATIC_APP_JS = Path(__file__).resolve().parent.parent / "app" / "static" / "app.js"

NODE_UI_DRIVER = r"""
const fs = require('fs');

// Minimal DOM shim so `app/static/app.js` executes its exact browser rendering logic
const elements = {};
function getEl(id) {
  if (!elements[id]) {
    elements[id] = { id, className: '', textContent: '', innerHTML: '', children: [] };
  }
  return elements[id];
}
global.window = global;
global.document = {
  getElementById: (id) => getEl(id),
  createElement: (tag) => ({
    tagName: tag.toUpperCase(),
    className: '',
    innerHTML: '',
    addEventListener: () => {},
  }),
  addEventListener: () => {},
};

const appJsCode = fs.readFileSync(process.env.APP_JS_PATH, 'utf-8');
eval(appJsCode);

async function runUiSession() {
  const baseUrl = process.env.BASE_URL;
  const apiKey = process.env.API_KEY;
  const runId = process.env.RUN_ID;
  const stateFile = process.env.STATE_FILE;

  const client = new global.window.SoupTrackerClient({
    baseUrl,
    apiKey,
    reconnectDelayMs: 80,
  });

  const dumpSnapshot = () => {
    const tbodyHtml = getEl('metrics-tbody').innerHTML || '';
    const trMatches = [...tbodyHtml.matchAll(/<tr data-step="(\d+)" data-seq="(\d+)"><td>(\d+)<\/td><td>(\d+)<\/td><td>([0-9.]+)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><\/tr>/g)];
    const domRows = trMatches.map((m) => ({
      step: Number(m[1]),
      seq: Number(m[2]),
      loss: Number(m[5]),
      lr_text: m[6],
      grad_norm_text: m[7],
    }));

    const snapshot = {
      runId: client.state.runId,
      connected: client.state.connected,
      reconnectCount: client.state.reconnectCount,
      lastSeq: client.state.lastSeq,
      seqGapsDetected: client.state.seqGapsDetected,
      orderedMetrics: client.state.orderedMetrics,
      domRows: domRows,
      svgHtml: getEl('loss-chart').innerHTML || '',
      badgeText: getEl('stream-status').textContent || '',
    };
    fs.writeFileSync(stateFile + '.tmp', JSON.stringify(snapshot, null, 2));
    fs.renameSync(stateFile + '.tmp', stateFile);
  };

  const timer = setInterval(dumpSnapshot, 40);
  process.on('SIGTERM', () => {
    clearInterval(timer);
    dumpSnapshot();
    client.stopStream();
    process.exit(0);
  });

  await client.startStream(runId, { resetState: true });
}

runUiSession().catch((err) => {
  console.error(err);
  process.exit(1);
});
"""


def _expected_metric(step: int) -> dict:
    return {
        "step": step,
        "loss": round(2.5 * (0.92 ** step), 6),
        "lr": round(1e-4 * (0.98 ** step), 8),
        "grad_norm": round(0.5 + (step % 4) * 0.11, 4),
    }


async def run_consistency_e2e_scenario() -> dict:
    async with running_test_server() as server:
        async with httpx.AsyncClient(base_url=server.base_url, timeout=10.0) as client:
            # 1. Create Project & Run
            proj_resp = await client.post("/projects", json={"name": "consistency-e2e-proj"})
            assert proj_resp.status_code == 201, proj_resp.text
            proj = proj_resp.json()
            api_key = proj["api_key"]
            headers = {"X-API-Key": api_key}

            run_resp = await client.post(
                "/runs",
                json={"name": "out-of-order-reconnect-run", "config": {"model": "llama-3-8b"}},
                headers=headers,
            )
            assert run_resp.status_code == 201, run_resp.text
            run_id = run_resp.json()["run_id"]

            with tempfile.TemporaryDirectory(prefix="soup_ui_state_") as tmpdir:
                state_file = Path(tmpdir) / "ui_state.json"
                driver_file = Path(tmpdir) / "driver.js"
                driver_file.write_text(NODE_UI_DRIVER, encoding="utf-8")

                env = {
                    **dict(subprocess.os.environ),
                    "APP_JS_PATH": str(STATIC_APP_JS),
                    "BASE_URL": server.base_url,
                    "API_KEY": api_key,
                    "RUN_ID": run_id,
                    "STATE_FILE": str(state_file),
                }
                ui_proc = subprocess.Popen(
                    ["node", str(driver_file)],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                async def wait_for_ui_condition(predicate, timeout: float = 5.0) -> dict:
                    deadline = asyncio.get_event_loop().time() + timeout
                    last_state = {}
                    while asyncio.get_event_loop().time() < deadline:
                        if state_file.exists():
                            try:
                                last_state = json.loads(state_file.read_text(encoding="utf-8"))
                                if predicate(last_state):
                                    return last_state
                            except Exception:
                                pass
                        await asyncio.sleep(0.04)
                    raise AssertionError(f"UI condition timed out. Last state: {last_state}")

                try:
                    # Wait for UI SSE stream to connect
                    await wait_for_ui_condition(lambda s: s.get("connected") is True)

                    # 2. Wave 1: Batch A (steps 0, 1, 2) + Out-of-order Batch B (steps 6, 7, 8)
                    batch_a = [_expected_metric(s) for s in (0, 1, 2)]
                    r_a1 = await client.post(
                        f"/runs/{run_id}/metrics",
                        json={"batch_id": "batch-A", "metrics": batch_a},
                        headers=headers,
                    )
                    assert r_a1.status_code == 200 and r_a1.json()["inserted"] == 3

                    batch_b = [_expected_metric(s) for s in (6, 7, 8)]
                    r_b = await client.post(
                        f"/runs/{run_id}/metrics",
                        json={"batch_id": "batch-B", "metrics": batch_b},
                        headers=headers,
                    )
                    assert r_b.status_code == 200 and r_b.json()["inserted"] == 3

                    # Resend duplicate Batch A (idempotent retry)
                    r_a2 = await client.post(
                        f"/runs/{run_id}/metrics",
                        json={"batch_id": "batch-A", "metrics": batch_a},
                        headers=headers,
                    )
                    assert r_a2.status_code == 200
                    assert r_a2.json()["inserted"] == 0
                    assert r_a2.json()["idempotent_replay"] is True

                    # Verify UI received first 6 unique events (steps 0,1,2,6,7,8; lastSeq == 6)
                    mid_state = await wait_for_ui_condition(
                        lambda s: s.get("lastSeq") == 6 and len(s.get("orderedMetrics", [])) == 6
                    )
                    assert [m["step"] for m in mid_state["orderedMetrics"]] == [0, 1, 2, 6, 7, 8]

                    # 3. Drop the active SSE connection mid-stream
                    drop_resp = await client.post(f"/_test/disconnect/{run_id}")
                    assert drop_resp.status_code == 200
                    assert drop_resp.json()["disconnected"] >= 1

                    # 4. While stream is recovering/disconnected, send LATE steps [3, 4, 5]
                    # and overlapping duplicate + new steps [4, 5, 9, 10]
                    batch_c_late = [_expected_metric(s) for s in (3, 4, 5)]
                    r_c = await client.post(
                        f"/runs/{run_id}/metrics",
                        json={"batch_id": "batch-C-late", "metrics": batch_c_late},
                        headers=headers,
                    )
                    assert r_c.status_code == 200 and r_c.json()["inserted"] == 3

                    batch_d_overlap = [_expected_metric(s) for s in (4, 5, 9, 10)]
                    r_d = await client.post(
                        f"/runs/{run_id}/metrics",
                        json={"batch_id": "batch-D-overlap", "metrics": batch_d_overlap},
                        headers=headers,
                    )
                    assert r_d.status_code == 200
                    assert r_d.json()["inserted"] == 2
                    assert r_d.json()["duplicates"] == 2

                    # 5. Wait for UI to reconnect and catch up to seq 11
                    await wait_for_ui_condition(
                        lambda s: s.get("reconnectCount", 0) >= 1 and s.get("lastSeq") == 11
                    )

                    # Send final live Batch E (steps [11, 12]) after stream recovery
                    batch_e_final = [_expected_metric(s) for s in (11, 12)]
                    r_e = await client.post(
                        f"/runs/{run_id}/metrics",
                        json={"batch_id": "batch-E-final", "metrics": batch_e_final},
                        headers=headers,
                    )
                    assert r_e.status_code == 200 and r_e.json()["inserted"] == 2

                    # 6. Wait for final UI state (all 13 steps 0..12, lastSeq == 13)
                    final_ui = await wait_for_ui_condition(
                        lambda s: s.get("lastSeq") == 13
                        and len(s.get("orderedMetrics", [])) == 13
                        and len(s.get("domRows", [])) == 13
                    )
                finally:
                    ui_proc.terminate()
                    ui_proc.wait(timeout=3.0)
                    if ui_proc.stdout:
                        ui_proc.stdout.close()
                    if ui_proc.stderr:
                        ui_proc.stderr.close()

            # 7. Query PostgreSQL directly and compare against UI state & DOM rows
            db_rows = await server.db.fetch_all(
                """
                SELECT step, seq, loss, lr, grad_norm
                FROM metrics
                WHERE run_id = $1
                ORDER BY step ASC
                """,
                run_id,
            )

            assert len(db_rows) == 13, f"Expected 13 rows in DB, got {len(db_rows)}"
            assert final_ui["seqGapsDetected"] == 0, "Detected sequence gaps in UI stream!"
            assert final_ui["reconnectCount"] >= 1, "Expected at least 1 mid-stream reconnect"
            assert "<polyline" in final_ui["svgHtml"], "SVG loss chart polyline was not rendered"

            for idx, db_row in enumerate(db_rows):
                ui_row = final_ui["orderedMetrics"][idx]
                dom_row = final_ui["domRows"][idx]
                assert int(db_row["step"]) == idx
                assert ui_row["step"] == int(db_row["step"])
                assert dom_row["step"] == int(db_row["step"])
                assert ui_row["seq"] == int(db_row["seq"])
                assert dom_row["seq"] == int(db_row["seq"])
                assert abs(ui_row["loss"] - float(db_row["loss"])) < 1e-9
                assert abs(dom_row["loss"] - float(db_row["loss"])) < 1e-6
                assert abs(ui_row["lr"] - float(db_row["lr"])) < 1e-12
                assert abs(ui_row["grad_norm"] - float(db_row["grad_norm"])) < 1e-9

            return {
                "run_id": run_id,
                "db_row_count": len(db_rows),
                "ui_row_count": len(final_ui["orderedMetrics"]),
                "dom_row_count": len(final_ui["domRows"]),
                "reconnect_count": final_ui["reconnectCount"],
                "seq_gaps_detected": final_ui["seqGapsDetected"],
                "last_seq": final_ui["lastSeq"],
                "db_rows": [
                    {
                        "step": int(r["step"]),
                        "seq": int(r["seq"]),
                        "loss": float(r["loss"]),
                        "lr": float(r["lr"]),
                        "grad_norm": float(r["grad_norm"]),
                    }
                    for r in db_rows
                ],
                "ui_rows": [
                    {
                        "step": m["step"],
                        "seq": m["seq"],
                        "loss": m["loss"],
                        "lr": m["lr"],
                        "grad_norm": m["grad_norm"],
                    }
                    for m in final_ui["orderedMetrics"]
                ],
            }


class TestConsistencyE2E(unittest.IsolatedAsyncioTestCase):
    async def test_out_of_order_duplicates_late_and_midstream_reconnect(self) -> None:
        result = await run_consistency_e2e_scenario()
        print("\n=== E2E CONSISTENCY PROOF SUMMARY ===")
        print(f"Run ID          : {result['run_id']}")
        print(f"DB Row Count    : {result['db_row_count']} (steps 0..12)")
        print(f"UI State Count  : {result['ui_row_count']} (steps 0..12)")
        print(f"DOM <tr> Count  : {result['dom_row_count']} (steps 0..12)")
        print(f"SSE Reconnects  : {result['reconnect_count']}")
        print(f"Seq Gaps Found  : {result['seq_gaps_detected']}")
        print(f"Final Last-Seq  : {result['last_seq']}")
        print("\nStep-by-Step Comparison (PostgreSQL DB vs. Final UI State):")
        print("Step | Arrival Seq | DB Loss    | UI Loss    | Match")
        print("-----+-------------+------------+------------+------")
        for db_r, ui_r in zip(result["db_rows"], result["ui_rows"]):
            match = db_r == ui_r
            print(
                f"{db_r['step']:4d} | {db_r['seq']:11d} | {db_r['loss']:10.6f} | "
                f"{ui_r['loss']:10.6f} | {'OK' if match else 'MISMATCH'}"
            )
        self.assertEqual(result["db_rows"], result["ui_rows"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
