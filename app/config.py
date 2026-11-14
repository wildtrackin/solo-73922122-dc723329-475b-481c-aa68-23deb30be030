"""Runtime configuration, all overridable through environment variables."""
from __future__ import annotations

import os
import uuid


def _required(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:  # pragma: no cover - misconfiguration fails loudly
        raise RuntimeError(f"environment variable {name} is required")
    return value


# Single PostgreSQL DSN shared by every API process.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/riskdb",
)

# Worker identity: hostname + pid makes the two compose processes distinguishable.
WORKER_ID = f"{os.getenv('HOSTNAME', 'host')}-pid{os.getpid()}-{uuid.uuid4().hex[:8]}"

# Item / batch leasing.
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "15"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "5"))
WORK_POLL_SECONDS = float(os.getenv("WORK_POLL_SECONDS", "0.5"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "200"))
# Optional delay between chunks; 0 in production, raised in demos so a crash
# can be injected while a batch is still RUNNING.
CHUNK_DELAY_SECONDS = float(os.getenv("CHUNK_DELAY_SECONDS", "0"))
MAX_CHUNK_ATTEMPTS = int(os.getenv("MAX_CHUNK_ATTEMPTS", "5"))

# Pagination.
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "100"))
MAX_PAGE_SIZE = int(os.getenv("MAX_PAGE_SIZE", "1000"))
