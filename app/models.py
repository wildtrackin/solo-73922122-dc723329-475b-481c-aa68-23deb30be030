"""Pydantic request/response models. All datetimes are timezone-aware UTC."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


def _check_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must include an explicit timezone (use UTC, e.g. 2026-01-10T08:00:00Z)")
    return value.astimezone(timezone.utc)


class ApplicationIn(BaseModel):
    application_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    application_time: datetime

    @field_validator("application_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return _check_utc(v)


class ApplicationsImport(BaseModel):
    applications: list[ApplicationIn] = Field(min_length=1)


class CreditEventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    business_time: datetime
    known_time: datetime
    income: int | None = Field(default=None)
    withdrawn: bool = False

    @field_validator("business_time", "known_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return _check_utc(v)

    @field_validator("withdrawn")
    @classmethod
    def _income_consistency(cls, withdrawn: bool, info) -> bool:
        income = info.data.get("income")
        if withdrawn and income is not None:
            raise ValueError("withdrawn event must not carry an income")
        if not withdrawn and income is None:
            raise ValueError("non-withdrawn event requires an integer income")
        return withdrawn


class CreditEventsImport(BaseModel):
    events: list[CreditEventIn] = Field(min_length=1)


class RejectedResponse(BaseModel):
    detail: str
    ingest_batch_id: uuid.UUID
    conflicts: list[dict] | None = None


class BatchCreateRequest(BaseModel):
    application_ids: list[str] = Field(min_length=1)


class BatchResponse(BaseModel):
    batch_id: uuid.UUID
    status: str
    total_items: int
    processed_items: int
    frozen_ingest_at: datetime
    created_at: datetime
    worker_id: str | None = None
    error: str | None = None


class ResultRow(BaseModel):
    application_id: str
    customer_id: str
    application_time: datetime
    income: int | None
    source_event_id: str | None
    source_version_known_time: datetime | None
    source_business_time: datetime | None
    source_ingest_batch_id: uuid.UUID | None
    basis: str  # "event" or "no_known_event"


class ResultsPage(BaseModel):
    batch_id: uuid.UUID
    status: str
    limit: int
    next_cursor: str | None
    items: list[ResultRow]


class ImportAccepted(BaseModel):
    ingest_batch_id: uuid.UUID
    imported: int
    kind: str
