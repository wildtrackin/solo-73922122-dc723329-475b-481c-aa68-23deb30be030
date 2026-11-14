#!/usr/bin/env python3
"""Failover / lease-reclaim test, runnable without Docker.

It drives the two shared-database API processes over HTTP and uses a plain
SQL connection only to simulate a hard crash (SIGKILL) of the owning worker:
a killed process stops heart-beating, so its leases expire. Expiring them in
the database is the exact post-crash state another worker observes.

Usage:
    python3/scripts/lease_failover_test.py BASE1 BASE2 DSAN [N]
Example:
    python3 scripts/lease_failover_test.py \
        http://127.0.0.1:8021 http://127.0.0.1:8022 \
        postgresql://postgres@127.0.0.1:55432/rctest11 40
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request

import asyncpg

BASE1, BASE2, DSN = sys.argv[1], sys.argv[2], sys.argv[3]
N = int(sys.argv[4]) if len(sys.argv) > 4 else 40


def http(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        return exc.code, json.loads(exc.read())


async def main() -> None:
    # 1. bulk dataset through the API
    apps = [{"application_id": f"F{i:04d}", "customer_id": f"CF{i:04d}",
             "application_time": "2026-06-01T00:00:00Z"} for i in range(N)]
    events = [{"event_id": f"EF{i:04d}", "customer_id": f"CF{i:04d}",
               "business_time": "2026-05-01T00:00:00Z",
               "known_time": "2026-05-02T00:00:00Z",
               "income": 10000 + i, "withdrawn": False} for i in range(N)]
    assert http("POST", f"{BASE1}/api/v1/imports/applications", {"applications": apps})[0] == 201
    assert http("POST", f"{BASE1}/api/v1/imports/credit-events", {"events": events})[0] == 201

    _, batch = http("POST", f"{BASE1}/api/v1/batches",
                    {"application_ids": [f"F{i:04d}" for i in range(N)]})
    bid = batch["batch_id"]
    print("batch", bid, "created")

    conn = await asyncpg.connect(DSN)
    # 2. wait until the batch is RUNNING with partial progress
    owner = None
    for _ in range(100):
        row = await conn.fetchrow("SELECT status, worker_id FROM batches WHERE batch_id=$1", bid)
        done = await conn.fetchval(
            "SELECT count(*) FROM batch_items WHERE batch_id=$1 AND status='DONE'", bid)
        if row["status"] == "RUNNING" and 0 < done < N:
            owner = row["worker_id"]
            print(f"mid-flight: owner={owner}, done={done}/{N} — simulating SIGKILL")
            break
        time.sleep(0.1)
    assert owner, "batch never reached a partial RUNNING state"

    # 3. exact post-SIGKILL state: every lease held by the dead worker expires
    await conn.execute(
        "UPDATE batches SET lease_expires_at = now() - interval '1 minute' WHERE batch_id=$1", bid)
    await conn.execute(
        "UPDATE batch_items SET lease_expires_at = now() - interval '1 minute' "
        "WHERE batch_id=$1 AND status='RUNNING'", bid)

    # 4. the surviving worker must reclaim and finish; owner changes at least once
    owners_seen = {owner}
    deadline = time.time() + 60
    while time.time() < deadline:
        row = await conn.fetchrow("SELECT status, worker_id FROM batches WHERE batch_id=$1", bid)
        if row["worker_id"]:
            owners_seen.add(row["worker_id"])
        if row["status"] == "COMPLETED":
            break
        time.sleep(0.2)
    assert row["status"] == "COMPLETED", row
    print("owners observed across the failover:", owners_seen)
    assert len(owners_seen) >= 2, "the surviving process did not take over"

    # 5. no missing / duplicated rows; every result annotated with its source
    stats = await conn.fetchrow(
        "SELECT count(*) rows, count(DISTINCT application_id) distinct_apps, "
        "count(*) FILTER (WHERE income IS NULL) null_incomes, "
        "count(*) FILTER (WHERE source_event_id IS NULL) missing_source "
        "FROM batch_results WHERE batch_id=$1", bid)
    print(dict(stats))
    assert stats["rows"] == N and stats["distinct_apps"] == N
    assert stats["null_incomes"] == 0 and stats["missing_source"] == 0

    # 6. paged read through the API returns every row exactly once, in order
    seen, cursor = [], None
    while True:
        url = f"{BASE2}/api/v1/batches/{bid}/results?limit=7"
        if cursor:
            url += f"&cursor={cursor}"
        _, page = http("GET", url)
        seen += [i["application_id"] for i in page["items"]]
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert len(seen) == N and len(set(seen)) == N and seen == sorted(seen)
    await conn.close()
    print("FAILover TEST OK: no missing rows, no duplicates, paged read complete")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
