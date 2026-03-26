"""FastAPI-based admin dashboard for Crawlee.

Start the dashboard server::

    from crawlee.dashboard import CrawleeDashboard

    dashboard = CrawleeDashboard()
    await dashboard.serve()          # blocks; visits http://localhost:8080

Or integrate with an existing ASGI app::

    app = dashboard.app              # FastAPI instance
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

from crawlee.dashboard._models import (
    PublishTaskRequest,
    QueueStats,
    TaskCreate,
    TaskInfo,
    TaskStatus,
    WorkerInfo,
)
from crawlee.dashboard._static import DASHBOARD_HTML

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional FastAPI import guard
# ---------------------------------------------------------------------------
try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse
    from sse_starlette.sse import EventSourceResponse

    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False

try:
    from crawlee.distributed import TaskPublisher

    _DISTRIBUTED_AVAILABLE = True
except Exception:
    _DISTRIBUTED_AVAILABLE = False


# ---------------------------------------------------------------------------
# In-process task registry (single-node mode)
# ---------------------------------------------------------------------------


class _TaskRegistry:
    """Simple in-memory registry of crawl tasks for the dashboard."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskInfo] = {}
        self._lock = asyncio.Lock()
        self._event_queue: asyncio.Queue[dict] = asyncio.Queue()

    async def create(self, req: TaskCreate) -> TaskInfo:
        task = TaskInfo(
            id=str(uuid4()),
            name=req.name,
            status=TaskStatus.PENDING,
            crawler_type=req.crawler_type,
            start_urls=req.start_urls,
            max_requests=req.max_requests,
            created_at=datetime.now(timezone.utc),
            extra=req.extra,
        )
        async with self._lock:
            self._tasks[task.id] = task
        await self._emit({'type': 'tasks_updated'})
        return task

    async def get_all(self) -> list[TaskInfo]:
        async with self._lock:
            return list(self._tasks.values())

    async def get(self, task_id: str) -> TaskInfo | None:
        async with self._lock:
            return self._tasks.get(task_id)

    async def update(self, task_id: str, **kwargs: object) -> TaskInfo | None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            updated = task.model_copy(update=kwargs)
            self._tasks[task_id] = updated
        await self._emit({'type': 'tasks_updated'})
        return updated

    async def delete(self, task_id: str) -> bool:
        async with self._lock:
            existed = task_id in self._tasks
            self._tasks.pop(task_id, None)
        if existed:
            await self._emit({'type': 'tasks_updated'})
        return existed

    async def stats(self) -> dict:
        async with self._lock:
            tasks = list(self._tasks.values())
        return {
            'total': len(tasks),
            'running': sum(1 for t in tasks if t.status == TaskStatus.RUNNING),
            'done': sum(1 for t in tasks if t.status == TaskStatus.DONE),
            'failed': sum(1 for t in tasks if t.status == TaskStatus.FAILED),
            'pending': sum(1 for t in tasks if t.status == TaskStatus.PENDING),
        }

    async def _emit(self, event: dict) -> None:
        await self._event_queue.put(event)

    async def events(self) -> AsyncGenerator[dict, None]:
        """Yield SSE events as they arrive."""
        while True:
            event = await self._event_queue.get()
            yield event


# ---------------------------------------------------------------------------
# Worker registry
# ---------------------------------------------------------------------------


class _WorkerRegistry:
    """Tracks registered worker nodes."""

    def __init__(self) -> None:
        self._workers: dict[str, WorkerInfo] = {}
        self._lock = asyncio.Lock()

    async def register(self, worker_id: str, hostname: str) -> WorkerInfo:
        info = WorkerInfo(
            worker_id=worker_id,
            hostname=hostname,
            registered_at=datetime.now(timezone.utc),
            last_heartbeat=datetime.now(timezone.utc),
        )
        async with self._lock:
            self._workers[worker_id] = info
        return info

    async def heartbeat(self, worker_id: str, tasks_processed: int = 0) -> None:
        async with self._lock:
            if worker_id in self._workers:
                w = self._workers[worker_id]
                self._workers[worker_id] = w.model_copy(
                    update={'last_heartbeat': datetime.now(timezone.utc), 'tasks_processed': tasks_processed}
                )

    async def get_all(self) -> list[WorkerInfo]:
        async with self._lock:
            return list(self._workers.values())

    async def count_active(self) -> int:
        workers = await self.get_all()
        now = datetime.now(timezone.utc)
        return sum(1 for w in workers if (now - w.last_heartbeat).total_seconds() < _WORKER_ACTIVE_TIMEOUT_SECS)


_WORKER_ACTIVE_TIMEOUT_SECS = 60


class CrawleeDashboard:
    """Admin web dashboard for Crawlee.

    Provides a FastAPI ASGI application that serves a browser-based UI and
    a REST + SSE API for managing crawl tasks and monitoring workers.

    Args:
        host: Bind host (default ``"0.0.0.0"``).
        port: Bind port (default ``8080``).
        redis_url: Optional Redis URL for distributed mode.  When provided the
            dashboard will publish tasks via :class:`~crawlee.distributed.TaskPublisher`.
    """

    def __init__(
        self,
        *,
        host: str = '0.0.0.0',  # noqa: S104
        port: int = 8080,
        redis_url: str | None = None,
    ) -> None:
        if not _FASTAPI_AVAILABLE:
            raise ImportError(
                "CrawleeDashboard requires 'fastapi' and 'sse-starlette'. "
                "Install them with: pip install 'crawlee[dashboard]'"
            )
        self._host = host
        self._port = port
        self._redis_url = redis_url
        self._tasks = _TaskRegistry()
        self._workers = _WorkerRegistry()
        self._publisher: TaskPublisher | None = None
        self.app = self._build_app()

    # ------------------------------------------------------------------
    # ASGI app construction
    # ------------------------------------------------------------------

    def _build_app(self) -> FastAPI:  # type: ignore[name-defined]
        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:  # type: ignore[name-defined]
            if self._redis_url and _DISTRIBUTED_AVAILABLE:
                self._publisher = TaskPublisher(redis_url=self._redis_url)
                await self._publisher.connect()
                logger.info('Distributed task publisher connected to Redis at %s', self._redis_url)
            yield
            if self._publisher:
                await self._publisher.close()

        app = FastAPI(title='Crawlee Dashboard', lifespan=lifespan)

        # --- UI ---
        @app.get('/', response_class=HTMLResponse, include_in_schema=False)
        async def index() -> str:
            return DASHBOARD_HTML

        # --- Stats ---
        @app.get('/api/stats')
        async def get_stats() -> dict:
            stats = await self._tasks.stats()
            stats['workers_online'] = await self._workers.count_active()
            return stats

        # --- Tasks ---
        @app.get('/api/tasks', response_model=list[TaskInfo])
        async def list_tasks() -> list[TaskInfo]:
            return await self._tasks.get_all()

        @app.post('/api/tasks', response_model=TaskInfo, status_code=201)
        async def create_task(req: TaskCreate) -> TaskInfo:
            task = await self._tasks.create(req)
            # Launch crawling in the background (single-node mode)
            background_task = asyncio.create_task(self._run_task(task.id))
            background_task.add_done_callback(lambda t: t.result() if not t.cancelled() else None)
            return task

        @app.get('/api/tasks/{task_id}', response_model=TaskInfo)
        async def get_task(task_id: str) -> TaskInfo:
            task = await self._tasks.get(task_id)
            if task is None:
                raise HTTPException(status_code=404, detail='Task not found')
            return task

        @app.post('/api/tasks/{task_id}/pause', response_model=TaskInfo)
        async def pause_task(task_id: str) -> TaskInfo:
            task = await self._tasks.get(task_id)
            if task is None:
                raise HTTPException(status_code=404, detail='Task not found')
            if task.status != TaskStatus.RUNNING:
                raise HTTPException(status_code=400, detail='Task is not running')
            return await self._tasks.update(task_id, status=TaskStatus.PAUSED)  # type: ignore[return-value]

        @app.post('/api/tasks/{task_id}/resume', response_model=TaskInfo)
        async def resume_task(task_id: str) -> TaskInfo:
            task = await self._tasks.get(task_id)
            if task is None:
                raise HTTPException(status_code=404, detail='Task not found')
            if task.status != TaskStatus.PAUSED:
                raise HTTPException(status_code=400, detail='Task is not paused')
            return await self._tasks.update(task_id, status=TaskStatus.RUNNING)  # type: ignore[return-value]

        @app.delete('/api/tasks/{task_id}')
        async def delete_task(task_id: str) -> dict:
            existed = await self._tasks.delete(task_id)
            if not existed:
                raise HTTPException(status_code=404, detail='Task not found')
            return {'deleted': task_id}

        # --- Queue stats (pass-through to storage) ---
        @app.get('/api/queues/{queue_id}', response_model=QueueStats)
        async def get_queue(queue_id: str) -> QueueStats:
            try:
                from crawlee.storages import RequestQueue  # noqa: PLC0415

                rq = await RequestQueue.open(name=queue_id)
                return QueueStats(
                    queue_id=queue_id,
                    total_count=rq.total_count,
                    handled_count=rq.handled_count,
                    pending_count=max(0, rq.total_count - rq.handled_count),
                )
            except Exception as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        # --- Distributed publishing ---
        @app.post('/api/publish', response_model=TaskInfo, status_code=201)
        async def publish_task(req: PublishTaskRequest) -> TaskInfo:
            task = await self._tasks.create(req.task)
            if self._publisher is not None:
                await self._publisher.publish(
                    task_id=task.id,
                    task_data=req.task.model_dump(),
                    target_worker_id=req.target_worker_id,
                )
                await self._tasks.update(task.id, status=TaskStatus.PENDING)
            else:
                # Fallback: run locally when no Redis is configured
                background_task = asyncio.create_task(self._run_task(task.id))
                background_task.add_done_callback(lambda t: t.result() if not t.cancelled() else None)
            return task

        # --- Workers ---
        @app.get('/api/workers', response_model=list[WorkerInfo])
        async def list_workers() -> list[WorkerInfo]:
            return await self._workers.get_all()

        @app.post('/api/workers/{worker_id}/register', response_model=WorkerInfo)
        async def register_worker(worker_id: str, hostname: str = 'unknown') -> WorkerInfo:
            return await self._workers.register(worker_id, hostname)

        @app.post('/api/workers/{worker_id}/heartbeat')
        async def worker_heartbeat(worker_id: str, tasks_processed: int = 0) -> dict:
            await self._workers.heartbeat(worker_id, tasks_processed)
            return {'ok': True}

        # --- SSE event stream ---
        @app.get('/api/events', include_in_schema=False)
        async def event_stream() -> EventSourceResponse:
            async def generator() -> AsyncGenerator[str, None]:
                # Send initial stats immediately
                stats = await self._tasks.stats()
                stats['workers_online'] = await self._workers.count_active()
                stats['type'] = 'stats'
                yield json.dumps(stats)
                # Then stream further events
                async for event in self._tasks.events():
                    yield json.dumps(event)

            return EventSourceResponse(generator())

        return app

    # ------------------------------------------------------------------
    # Background task runner (single-node mode)
    # ------------------------------------------------------------------

    async def _run_task(self, task_id: str) -> None:
        """Run a crawl task in the background using the appropriate crawler."""
        task = await self._tasks.get(task_id)
        if task is None:
            return

        await self._tasks.update(task_id, status=TaskStatus.RUNNING, started_at=datetime.now(timezone.utc))

        try:
            crawler = self._build_crawler(task)
            if crawler is None:
                await self._tasks.update(task_id, status=TaskStatus.FAILED, finished_at=datetime.now(timezone.utc))
                return

            from crawlee import Request as CrawleeRequest  # noqa: PLC0415

            requests = [CrawleeRequest.from_url(u) for u in task.start_urls]
            await crawler.run(requests)

            stats = crawler.statistics.state
            await self._tasks.update(
                task_id,
                status=TaskStatus.DONE,
                finished_at=datetime.now(timezone.utc),
                requests_finished=stats.requests_finished,
                requests_failed=stats.requests_failed,
                requests_total=stats.requests_total,
                requests_per_minute=stats.requests_finished_per_minute,
            )

        except Exception:
            logger.exception('Task %s failed', task_id)
            await self._tasks.update(task_id, status=TaskStatus.FAILED, finished_at=datetime.now(timezone.utc))

    def _build_crawler(self, task: TaskInfo) -> object | None:
        """Instantiate the correct crawler type for *task*."""
        kwargs: dict = {}
        if task.max_requests is not None:
            kwargs['max_requests_per_crawl'] = task.max_requests

        crawler_type = task.crawler_type.lower()
        try:
            if crawler_type == 'http':
                from crawlee.crawlers import HttpCrawler  # noqa: PLC0415

                crawler = HttpCrawler(**kwargs)

                @crawler.router.default_handler
                async def _handle(ctx: object) -> None:
                    pass

                return crawler

            if crawler_type == 'beautifulsoup':
                from crawlee.crawlers import BeautifulSoupCrawler  # noqa: PLC0415

                crawler = BeautifulSoupCrawler(**kwargs)

                @crawler.router.default_handler
                async def _handle_bs(ctx: object) -> None:
                    pass

                return crawler

            if crawler_type == 'parsel':
                from crawlee.crawlers import ParselCrawler  # noqa: PLC0415

                crawler = ParselCrawler(**kwargs)

                @crawler.router.default_handler
                async def _handle_parsel(ctx: object) -> None:
                    pass

                return crawler

            if crawler_type == 'playwright':
                from crawlee.crawlers import PlaywrightCrawler  # noqa: PLC0415

                crawler = PlaywrightCrawler(**kwargs)

                @crawler.router.default_handler
                async def _handle_pw(ctx: object) -> None:
                    pass

                return crawler

        except ImportError as exc:
            logger.warning('Crawler type %r is not available: %s', crawler_type, exc)
        return None

    # ------------------------------------------------------------------
    # Convenience run method
    # ------------------------------------------------------------------

    async def serve(self) -> None:
        """Start the Uvicorn server and block until shutdown.

        Requires ``uvicorn`` to be installed::

            pip install uvicorn[standard]
        """
        try:
            import uvicorn  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError("'uvicorn' is required to call serve(). Install it with: pip install uvicorn") from exc

        config = uvicorn.Config(self.app, host=self._host, port=self._port, log_level='info')
        server = uvicorn.Server(config)
        logger.info('Crawlee Dashboard running at http://%s:%d', self._host, self._port)
        await server.serve()
