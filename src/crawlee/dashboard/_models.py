"""Pydantic models for the Crawlee dashboard API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Status of a crawl task."""

    PENDING = 'pending'
    RUNNING = 'running'
    PAUSED = 'paused'
    DONE = 'done'
    FAILED = 'failed'


class TaskCreate(BaseModel):
    """Request body for creating a new crawl task."""

    name: str = Field(..., description='Human-readable task name')
    start_urls: list[str] = Field(..., description='Seed URLs for the crawl')
    crawler_type: str = Field(default='http', description='Crawler type: http, beautifulsoup, parsel, playwright')
    max_requests: int | None = Field(default=None, description='Maximum number of requests to process')
    extra: dict[str, Any] = Field(default_factory=dict, description='Extra crawler configuration')


class TaskInfo(BaseModel):
    """Runtime information about a crawl task."""

    id: str
    name: str
    status: TaskStatus
    crawler_type: str
    start_urls: list[str]
    max_requests: int | None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    requests_finished: int = 0
    requests_failed: int = 0
    requests_total: int = 0
    requests_per_minute: float = 0.0
    extra: dict[str, Any] = Field(default_factory=dict)


class QueueStats(BaseModel):
    """Stats for a request queue."""

    queue_id: str
    total_count: int
    handled_count: int
    pending_count: int


class WorkerInfo(BaseModel):
    """Information about a registered worker node."""

    worker_id: str
    hostname: str
    registered_at: datetime
    last_heartbeat: datetime
    tasks_processed: int = 0
    status: str = 'active'


class PublishTaskRequest(BaseModel):
    """Request body for publishing a task to distributed workers."""

    task: TaskCreate
    target_worker_id: str | None = Field(default=None, description='Pin task to a specific worker; None = any worker')
