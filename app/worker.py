"""Background worker: claims batches/items and computes point-in-time income.

Concurrency model
-----------------
Every API process runs one worker loop against the shared database.
- A PENDING (or lease-expired RUNNING) batch is claimed with a row lock
  (SELECT ... FOR UPDATE SKIP LOCKED), so exactly one worker owns a batch.
- Items are claimed in chunks the same way. A worker renews the leases with a
  heartbeat; if it dies, another process reclaims the expired leases and
  continues with the *same frozen input*. DONE items are never recomputed.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

import asyncpg

from . import config

logger = logging.getLogger("income.worker")


# ---------------------------------------------------------------------------
# The point-in-time reconstruction, expressed in one SQL statement per chunk.
#
# For every claimed application a:
#   1. Restrict event versions to those known at application time AND already
#      ingested when the batch was accepted (frozen_ingest_at) -> reconstruct
#      exactly the versions visible at application time.
#   2. Per event, keep the newest visible version (DISTINCT ON ... ORDER BY
#      known_time DESC) -> later revisions of the same event are applied,
#      including withdrawals.
#   3. Drop withdrawn versions and events whose business_time is after the
#      application time; pick the remaining event with the latest
#      business_time, ties broken by the smallest event_id.
#   4. Left join so applications with no usable event stay empty.
# ---------------------------------------------------------------------------
_COMPUTE_CHUNK_SQL = """
WITH picked AS (
    SELECT bi.batch_id, bi.application_id, bi.item_seq
    FROM batch_items bi
    WHERE bi.batch_id = $1
      AND bi.status = 'RUNNING'
      AND bi.worker_id = $2
      AND bi.lease_expires_at > now()
    ORDER BY bi.item_seq
    LIMIT $3
),
visible_versions AS (
    SELECT
        p.application_id,
        p.item_seq,
        cev.event_id,
        cev.known_time,
        cev.business_time,
        cev.income,
        cev.withdrawn,
        cev.ingest_batch_id AS source_ingest_batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY p.application_id, cev.event_id
            ORDER BY cev.known_time DESC
        ) AS rn
    FROM picked p
    JOIN applications app ON app.application_id = p.application_id
    JOIN credit_event_versions cev
      ON cev.customer_id = app.customer_id
     AND cev.known_time <= app.application_time
     AND cev.ingested_at <= (SELECT frozen_ingest_at FROM batches WHERE batch_id = p.batch_id)
),
current_versions AS (
    SELECT * FROM visible_versions WHERE rn = 1
),
candidates AS (
    SELECT
        application_id,
        item_seq,
        event_id,
        known_time,
        business_time,
        income,
        source_ingest_batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY application_id
            ORDER BY business_time DESC, event_id ASC
        ) AS cand_rn
    FROM current_versions
    WHERE withdrawn = FALSE
      AND business_time <= (
          SELECT application_time FROM applications ap
          WHERE ap.application_id = current_versions.application_id
      )
),
best AS (
    SELECT * FROM candidates WHERE cand_rn = 1
),
inserted AS (
    INSERT INTO batch_results
        (batch_id, application_id, customer_id, application_time, income,
         source_event_id, source_version_known_time, source_ingest_batch_id,
         source_business_time, item_seq)
    SELECT
        p.batch_id, p.application_id, app.customer_id, app.application_time,
        b.income, b.event_id, b.known_time, b.source_ingest_batch_id,
        b.business_time, p.item_seq
    FROM picked p
    JOIN applications app ON app.application_id = p.application_id
    LEFT JOIN best b ON b.application_id = p.application_id
    ON CONFLICT (batch_id, application_id) DO NOTHING
    RETURNING batch_id, application_id
)
UPDATE batch_items bi
SET status = 'DONE', worker_id = NULL, lease_expires_at = NULL
FROM inserted i
WHERE bi.batch_id = i.batch_id AND bi.application_id = i.application_id;
"""

_CLAIM_BATCH_SQL = """
UPDATE batches b
SET status = 'RUNNING',
    worker_id = $1,
    lease_expires_at = now() + make_interval(secs => $2)
FROM (
    SELECT batch_id
    FROM batches
    WHERE status = 'PENDING'
       OR (status = 'RUNNING' AND lease_expires_at < now())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
) c
WHERE b.batch_id = c.batch_id
RETURNING b.batch_id;
"""

_CLAIM_ITEMS_SQL = """
UPDATE batch_items
SET status = 'RUNNING', worker_id = $2,
    lease_expires_at = now() + make_interval(secs => $3),
    attempts = attempts + 1
WHERE (batch_id, application_id) IN (
    SELECT batch_id, application_id
    FROM batch_items
    WHERE batch_id = $1
      AND (status = 'PENDING' OR (status = 'RUNNING' AND lease_expires_at < now()))
    ORDER BY item_seq
    FOR UPDATE SKIP LOCKED
    LIMIT $4
)
RETURNING application_id;
"""

# After a failed chunk transaction the rows are still RUNNING with an active
# lease; hand them back so another claim cycle (or worker) can retry them.
_RELEASE_CHUNK_SQL = """
UPDATE batch_items
SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
WHERE batch_id = $1 AND status = 'RUNNING' AND worker_id = $2;
"""

_MAX_ATTEMPTS_SQL = """
SELECT max(attempts) AS max_attempts
FROM batch_items WHERE batch_id = $1 AND status <> 'DONE';
"""

_HEARTBEAT_SQL = """
UPDATE batches
SET lease_expires_at = now() + make_interval(secs => $2)
WHERE batch_id = $1 AND status = 'RUNNING' AND worker_id = $3;
"""
_HEARTBEAT_ITEMS_SQL = """
UPDATE batch_items
SET lease_expires_at = now() + make_interval(secs => $2)
WHERE batch_id = $1 AND status = 'RUNNING' AND worker_id = $3;
"""

_RECLAIM_ORPHAN_ITEMS_SQL = """
UPDATE batch_items
SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
WHERE batch_id = $1 AND status = 'RUNNING' AND lease_expires_at < now();
"""

_COMPLETE_SQL = """
UPDATE batches b
SET status = 'COMPLETED',
    processed_items = d.done_count,
    worker_id = NULL,
    lease_expires_at = NULL,
    error = NULL
FROM (
    SELECT count(*) AS done_count
    FROM batch_items WHERE batch_id = $1 AND status = 'DONE'
) d
WHERE b.batch_id = $1
  AND NOT EXISTS (
      SELECT 1 FROM batch_items WHERE batch_id = $1 AND status <> 'DONE'
  )
RETURNING b.batch_id;
"""

_FAIL_SQL = """
UPDATE batches
SET status = 'FAILED', error = $2
WHERE batch_id = $1;
"""


class Worker:
    def __init__(self, pool: asyncpg.Pool, worker_id: str) -> None:
        self.pool = pool
        self.worker_id = worker_id
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._current_batch: uuid.UUID | None = None
        self._heartbeat_task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"worker-{self.worker_id}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        logger.info("worker %s started", self.worker_id)
        while not self._stop.is_set():
            try:
                worked = await self._tick()
            except Exception:  # keep the worker alive through transient errors
                logger.exception("worker tick failed")
                worked = False
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=config.WORK_POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        logger.info("worker %s stopped", self.worker_id)

    async def _tick(self) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(_CLAIM_BATCH_SQL, self.worker_id, config.LEASE_SECONDS)
        if row is None:
            return False
        batch_id = row["batch_id"]
        self._current_batch = batch_id
        logger.info("worker %s claimed batch %s", self.worker_id, batch_id)
        self._heartbeat_task = asyncio.create_task(self._heartbeat(batch_id))
        try:
            await self._process_batch(batch_id)
        finally:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat_task = None
            self._current_batch = None
        return True

    async def _heartbeat(self, batch_id: uuid.UUID) -> None:
        while True:
            await asyncio.sleep(config.HEARTBEAT_SECONDS)
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        _HEARTBEAT_SQL, batch_id, config.LEASE_SECONDS, self.worker_id
                    )
                    await conn.execute(
                        _HEARTBEAT_ITEMS_SQL, batch_id, config.LEASE_SECONDS, self.worker_id
                    )
            except Exception:
                logger.exception("heartbeat failed for batch %s", batch_id)

    async def _process_batch(self, batch_id: uuid.UUID) -> None:
        # Items leased by a dead worker are put back before we claim anything.
        async with self.pool.acquire() as conn:
            await conn.execute(_RECLAIM_ORPHAN_ITEMS_SQL, batch_id)

        while True:
            async with self.pool.acquire() as conn:
                claimed = await conn.fetch(
                    _CLAIM_ITEMS_SQL,
                    batch_id, self.worker_id, config.LEASE_SECONDS, config.CHUNK_SIZE,
                )
            if not claimed:
                break
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute(
                            _COMPUTE_CHUNK_SQL, batch_id, self.worker_id, config.CHUNK_SIZE
                        )
                if config.CHUNK_DELAY_SECONDS:
                    await asyncio.sleep(config.CHUNK_DELAY_SECONDS)
            except Exception:
                # The failed transaction rolled back, so the items remain
                # RUNNING under our lease. Release them for re-claim; the
                # persisted `attempts` counter survives worker restarts.
                logger.exception("chunk failed for batch %s", batch_id)
                async with self.pool.acquire() as conn:
                    await conn.execute(_RELEASE_CHUNK_SQL, batch_id, self.worker_id)
                    max_attempts = await conn.fetchval(_MAX_ATTEMPTS_SQL, batch_id)
                if max_attempts and max_attempts >= config.MAX_CHUNK_ATTEMPTS:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            _FAIL_SQL, batch_id,
                            f"an item failed after {max_attempts} attempts; see api logs",
                        )
                    return
                await asyncio.sleep(0.5)
                continue

        async with self.pool.acquire() as conn:
            completed = await conn.fetchrow(_COMPLETE_SQL, batch_id)
        if completed:
            logger.info("batch %s COMPLETED", batch_id)
        else:
            # Should not happen: items are leased but not done. Another worker
            # will reclaim them; this batch stays RUNNING until its lease
            # expires and is picked up again.
            logger.warning("batch %s not completable yet; releasing for reclaim", batch_id)
