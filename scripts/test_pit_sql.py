#!/usr/bin/env python3
"""Point-in-time reconstruction unit tests against a real PostgreSQL.

These exercise the exact SQL contract from app/worker.py directly, covering
edge cases that the seed demo only touches partially:
  - revision applied only when known_time <= application_time;
  - withdrawal removes an event; later re-introduction restores it;
  - future business_time events are invisible;
  - same-business-time tie breaks by smallest event_id;
  - the ingest fence (frozen_ingest_at) hides late imports;
  - no usable event -> NULL income.

Usage: DATABASE_URL=postgresql://... python3 scripts/test_pit_sql.py
The script creates and drops its own throwaway schema objects.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg

DSN = os.getenv("DATABASE_URL", "postgresql://postgres@127.0.0.1:55433/c3")
UTC = timezone.utc


def dt(month: int, day: int, hour: int = 0) -> datetime:
    return datetime(2026, month, day, hour, tzinfo=UTC)


# Same shape as the worker query, parameterised by application + fence.
PIT_SQL = """
WITH visible_versions AS (
    SELECT cev.event_id, cev.known_time, cev.business_time, cev.income,
           cev.withdrawn,
           ROW_NUMBER() OVER (PARTITION BY cev.event_id ORDER BY cev.known_time DESC) rn
    FROM credit_event_versions cev
    WHERE cev.customer_id = $1
      AND cev.known_time <= $2
      AND cev.ingested_at <= $3
),
current_versions AS (SELECT * FROM visible_versions WHERE rn = 1),
candidates AS (
    SELECT event_id, known_time, business_time, income,
           ROW_NUMBER() OVER (ORDER BY business_time DESC, event_id ASC) cand_rn
    FROM current_versions
    WHERE withdrawn = FALSE AND business_time <= $2
)
SELECT event_id, known_time, business_time, income
FROM candidates WHERE cand_rn = 1;
"""


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute("""
        CREATE TEMP TABLE t_app (application_id text, customer_id text, application_time timestamptz);
        CREATE TEMP TABLE t_cev (LIKE credit_event_versions INCLUDING ALL);
    """)

    async def reset() -> None:
        await conn.execute("TRUNCATE t_cev")

    async def add(event, customer, biz, known, income, withdrawn, fence=None):
        await conn.execute(
            """INSERT INTO t_cev (event_id, customer_id, business_time, known_time,
                                  income, withdrawn, ingested_at, ingest_batch_id)
               VALUES ($1,$2,$3,$4,$5,$6,COALESCE($7, now()), $8)""",
            event, customer, biz, known, income, withdrawn, fence, uuid.uuid4())

    async def pick(customer, app_time, fence):
        # adapt PIT SQL to the temp table name
        sql = PIT_SQL.replace("credit_event_versions", "t_cev")
        return await conn.fetchrow(sql, customer, app_time, fence)

    fence = datetime(2026, 12, 31, tzinfo=UTC)
    cases: list[tuple[str, bool]] = []

    # 1. revision known after application time is not visible
    await reset()
    await add("E", "C", dt(1, 1), dt(1, 2), 100, False)
    await add("E", "C", dt(1, 1), dt(1, 10), 110, False)
    r = await pick("C", dt(1, 5), fence)
    cases.append(("revision after app time hidden", r["income"] == 100))

    # 2. revision known before application time is visible
    r = await pick("C", dt(1, 12), fence)
    cases.append(("revision before app time applied", r["income"] == 110))

    # 3. withdrawal removes the only event -> NULL
    await reset()
    await add("E", "C", dt(1, 1), dt(1, 2), 100, False)
    await add("E", "C", dt(1, 1), dt(1, 3), None, True)
    r = await pick("C", dt(1, 5), fence)
    cases.append(("withdrawn event yields NULL", r is None))

    # 4. later re-introduction after withdrawal restores an income
    await add("E", "C", dt(1, 1), dt(1, 8), 120, False)
    r = await pick("C", dt(1, 9), fence)
    cases.append(("re-introduced version restores income", r is not None and r["income"] == 120))

    # 5. future business_time excluded even if already known
    await reset()
    await add("FUT", "C", dt(3, 1), dt(2, 1), 500, False)
    r = await pick("C", dt(2, 20), fence)
    cases.append(("future business_time excluded", r is None))

    # 6. same business_time tie -> smallest event_id
    await reset()
    await add("ZB", "C", dt(2, 1), dt(2, 2), 33, False)
    await add("AA", "C", dt(2, 1), dt(2, 2), 30, False)
    r = await pick("C", dt(2, 20), fence)
    cases.append(("tie broken by smallest event_id", r["event_id"] == "AA"))

    # 7. latest business_time wins over an older event
    await add("OLD", "C", dt(1, 1), dt(1, 2), 10, False)
    r = await pick("C", dt(2, 20), fence)
    cases.append(("newest business_time wins", r["event_id"] == "AA"))

    # 8. ingest fence hides a version imported after the batch was accepted
    await reset()
    early_fence = dt(1, 5)
    await add("E", "C", dt(1, 1), dt(1, 2), 100, False, fence=dt(1, 3))
    await add("E", "C", dt(1, 1), dt(1, 3), 105, False,
              fence=dt(1, 10))  # ingested only on Jan 10
    r = await pick("C", dt(1, 20), early_fence)
    cases.append(("ingest fence hides late import", r["income"] == 100))
    r2 = await pick("C", dt(1, 20), dt(1, 15))
    cases.append(("later batch sees the imported correction", r2["income"] == 105))

    # 9. no event at all -> NULL
    await reset()
    r = await pick("STRANGER", dt(1, 1), fence)
    cases.append(("unknown customer yields NULL", r is None))

    failed = 0
    for name, ok in cases:
        print(("PASS" if ok else "FAIL"), "-", name)
        failed += not ok
    await conn.close()
    if failed:
        raise SystemExit(f"{failed} point-in-time test(s) failed")
    print(f"all {len(cases)} point-in-time SQL tests passed")


if __name__ == "__main__":
    asyncio.run(main())
