"""Simulates a training client sending metric batches over an unreliable network.

Usage:
    python3 scripts/simulate_client.py [--run-id <UUID>] [--api-key <KEY>] [--steps 30] [--delay 0.5]
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate training client sending metric batches.")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Tracker base URL")
    parser.add_argument("--api-key", default=None, help="Project API key (soup_sk_...)")
    parser.add_argument("--run-id", default=None, help="Target run ID (UUID)")
    parser.add_argument("--steps", type=int, default=30, help="Total steps to simulate")
    parser.add_argument("--batch-size", type=int, default=5, help="Metrics per batch")
    parser.add_argument("--delay", type=float, default=0.6, help="Delay between batches in seconds")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    # If no api_key or run_id given, fetch projects / create bootstrap
    api_key = args.api_key
    run_id = args.run_id

    if not api_key:
        print(f"[*] No API key provided. Bootstrapping a demo project at {base_url}/projects ...")
        req = urllib.request.Request(
            f"{base_url}/projects",
            data=json.dumps({"name": "client-sim-project"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            api_key = data["api_key"]
            print(f"[+] Created Project: {data['project_id']}")
            print(f"[+] API Key        : {api_key}")

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
    }

    if not run_id:
        print("[*] Fetching runs for project...")
        req = urllib.request.Request(f"{base_url}/runs", headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                runs = data.get("runs", [])
                if runs:
                    run_id = runs[0]["run_id"]
                    print(f"[+] Using existing run: {run_id} ({runs[0]['name']})")
        except Exception:
            pass

    if not run_id:
        print("[*] Creating a new training run: llama3-8b-client-sim ...")
        req = urllib.request.Request(
            f"{base_url}/runs",
            data=json.dumps({"name": "llama3-8b-client-sim", "config": {"lr": 1e-4}}).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            run_id = data["run_id"]
            print(f"[+] Created Run: {run_id}")

    print(f"\n[*] Streaming {args.steps} training steps to run {run_id} (batch size: {args.batch_size})...")
    print(f"[*] Open your browser at {base_url} to watch the live loss curve!\n")

    current_step = 0
    batch_num = 1
    while current_step < args.steps:
        batch_metrics = []
        for _ in range(args.batch_size):
            if current_step >= args.steps:
                break
            step = current_step
            # Exponential decay loss with realistic training jitter
            loss = 2.4 * math.exp(-0.08 * step) + 0.35 + (random.random() * 0.06 - 0.03)
            lr = 1e-4 * (0.99 ** step)
            grad_norm = 0.5 + random.random() * 0.2
            batch_metrics.append({
                "step": step,
                "loss": round(max(0.05, loss), 6),
                "lr": round(lr, 8),
                "grad_norm": round(grad_norm, 4),
            })
            current_step += 1

        batch_id = f"batch-{batch_num:03d}-{int(time.time()*1000)}"
        payload = {"batch_id": batch_id, "metrics": batch_metrics}

        req = urllib.request.Request(
            f"{base_url}/runs/{run_id}/metrics",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        t0 = time.perf_counter()
        with urllib.request.urlopen(req) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            print(
                f"  -> Batch #{batch_num:02d} sent: steps {batch_metrics[0]['step']:02d}..{batch_metrics[-1]['step']:02d} "
                f"| inserted: {res.get('inserted')} | last_seq: {res.get('last_seq')} | {elapsed_ms:.1f}ms"
            )

        batch_num += 1
        if current_step < args.steps:
            time.sleep(args.delay)

    print("\n[OK] Done! All metrics successfully ingested and stored in database.")


if __name__ == "__main__":
    main()
