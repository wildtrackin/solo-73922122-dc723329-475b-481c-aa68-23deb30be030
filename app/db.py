"""Asyncpg connection pool and schema bootstrap."""
from __future__ import annotations

import asyncio
import pathlib
import re

import asyncpg

from . import config

_pool: asyncpg.Pool | None = None
_SCHEMA_PATH = pathlib.Path(__file__).with_name("schema.sql")


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            config.DATABASE_URL,
            min_size=0,
            max_size=10,
            command_timeout=60,
        )
    return _pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("database pool is not initialised")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def init_schema() -> None:
    """Create tables idempotently. Safe to run concurrently from two
    processes: a transaction-scoped advisory lock serialises the DDL, and
    every statement is IF NOT EXISTS, so the second process applies no-ops."""
    pool = await init_pool()
    sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    # Drop full-line comments, then split on statement terminators.
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    statements = [s.strip() for s in re.split(r";\s*(?:\n|$)", "\n".join(lines)) if s.strip()]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(917245)")
            for stmt in statements:
                await conn.execute(stmt)
