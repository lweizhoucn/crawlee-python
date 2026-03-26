"""Redis-backed distributed task publisher and worker for Crawlee.

Architecture
------------
* ``TaskPublisher`` - publishes crawl task descriptors to a Redis list
  (``crawlee:tasks`` by default).  Any connected :class:`Worker` node will
  pick them up and run the requested crawler.

* ``Worker`` - subscribes to the Redis task queue, processes tasks, and
  reports back status / heartbeats.

Usage - publisher side (e.g. inside the dashboard)::

    publisher = TaskPublisher(redis_url="redis://localhost:6379")
    await publisher.connect()
    await publisher.publish(task_id="t1", task_data={...})
    await publisher.close()

Usage - worker side::

    worker = Worker(redis_url="redis://localhost:6379", worker_id="w1")
    await worker.run()   # blocks; processes tasks until stopped
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

_TASK_QUEUE_KEY = 'crawlee:tasks'
_STATUS_KEY_PREFIX = 'crawlee:task_status:'
_WORKER_HEARTBEAT_PREFIX = 'crawlee:worker:'
_DEFAULT_HEARTBEAT_TTL = 90  # seconds


class TaskPublisher:
    """Publishes crawl tasks to a shared Redis list.

    Multiple :class:`Worker` nodes consume from the same list, giving a simple
    work-stealing distributed queue.

    Args:
        redis_url: Redis connection URL (``redis://host:port/db``).
        queue_key: Redis key used as the task queue (list).
    """

    def __init__(self, *, redis_url: str = 'redis://localhost:6379', queue_key: str = _TASK_QUEUE_KEY) -> None:
        self._redis_url = redis_url
        self._queue_key = queue_key
        self._redis: Any = None

    async def connect(self) -> None:
        """Open the Redis connection."""
        try:
            import redis.asyncio as aioredis  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "Distributed task publishing requires the 'redis' package. "
                "Install it with: pip install 'crawlee[redis]'"
            ) from exc

        self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        logger.info('TaskPublisher connected to Redis at %s', self._redis_url)

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def publish(
        self,
        *,
        task_id: str | None = None,
        task_data: dict[str, Any],
        target_worker_id: str | None = None,
    ) -> str:
        """Push a task onto the distributed queue.

        Args:
            task_id: Optional caller-supplied task ID; a UUID4 is generated if omitted.
            task_data: Serialisable task payload (e.g. from :class:`~crawlee.dashboard.TaskCreate`).
            target_worker_id: Pin the task to a specific worker.  ``None`` means any worker.

        Returns:
            The task ID that was enqueued.
        """
        if self._redis is None:
            await self.connect()

        tid = task_id or str(uuid4())
        payload = {
            'task_id': tid,
            'task_data': task_data,
            'target_worker_id': target_worker_id,
            'enqueued_at': datetime.now(timezone.utc).isoformat(),
        }
        queue_key = f'crawlee:tasks:{target_worker_id}' if target_worker_id else self._queue_key
        await self._redis.rpush(queue_key, json.dumps(payload))
        await self._redis.set(f'{_STATUS_KEY_PREFIX}{tid}', 'pending')
        logger.debug('Published task %s to queue %s', tid, queue_key)
        return tid

    async def get_task_status(self, task_id: str) -> str | None:
        """Return the current status string for *task_id*, or ``None`` if unknown."""
        if self._redis is None:
            return None
        return await self._redis.get(f'{_STATUS_KEY_PREFIX}{task_id}')

    async def set_task_status(self, task_id: str, status: str) -> None:
        """Update the status of a task in Redis."""
        if self._redis is None:
            await self.connect()
        await self._redis.set(f'{_STATUS_KEY_PREFIX}{task_id}', status)

    async def list_workers(self) -> list[dict[str, Any]]:
        """Return a list of recently-seen worker heartbeat records."""
        if self._redis is None:
            return []
        keys = await self._redis.keys(f'{_WORKER_HEARTBEAT_PREFIX}*')
        workers = []
        for key in keys:
            raw = await self._redis.get(key)
            if raw:
                import contextlib  # noqa: PLC0415

                with contextlib.suppress(json.JSONDecodeError):
                    workers.append(json.loads(raw))
        return workers


class Worker:
    """A worker node that consumes tasks from the distributed Redis queue.

    Each worker registers itself in Redis with a periodic heartbeat, then
    blocks on ``BLPOP`` waiting for tasks.  When a task arrives the worker
    instantiates the correct Crawlee crawler, runs it, and reports the result
    back to Redis.

    Args:
        redis_url: Redis connection URL.
        worker_id: Unique identifier for this worker.  Defaults to a UUID4.
        queue_key: Redis list key to consume tasks from.
        heartbeat_interval: Seconds between heartbeat writes to Redis.
        dashboard_url: If set, the worker will POST heartbeats to the
            :class:`~crawlee.dashboard.CrawleeDashboard` HTTP API.
    """

    def __init__(
        self,
        *,
        redis_url: str = 'redis://localhost:6379',
        worker_id: str | None = None,
        queue_key: str = _TASK_QUEUE_KEY,
        heartbeat_interval: float = 15.0,
        dashboard_url: str | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._worker_id = worker_id or str(uuid4())
        self._queue_key = queue_key
        self._heartbeat_interval = heartbeat_interval
        self._dashboard_url = dashboard_url
        self._redis: Any = None
        self._publisher: TaskPublisher | None = None
        self._running = False
        self._tasks_processed = 0

    @property
    def worker_id(self) -> str:
        """Unique ID of this worker."""
        return self._worker_id

    async def connect(self) -> None:
        """Open the Redis connection and register the worker."""
        try:
            import redis.asyncio as aioredis  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "Worker requires the 'redis' package. Install it with: pip install 'crawlee[redis]'"
            ) from exc

        self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        self._publisher = TaskPublisher(redis_url=self._redis_url, queue_key=self._queue_key)
        self._publisher._redis = self._redis  # noqa: SLF001 share connection

        await self._register()
        logger.info('Worker %s connected to Redis at %s', self._worker_id, self._redis_url)

    async def close(self) -> None:
        """Stop the worker and close the Redis connection."""
        self._running = False
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def run(self) -> None:
        """Start the worker event loop (blocks until :meth:`close` is called).

        Internally this spawns a heartbeat coroutine and a task-consumption
        loop that blocks on ``BLPOP`` with a 5-second timeout.
        """
        if self._redis is None:
            await self.connect()

        self._running = True
        logger.info('Worker %s started. Listening on queue %s', self._worker_id, self._queue_key)

        heartbeat_background_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._consume_loop()
        finally:
            heartbeat_background_task.cancel()
            import contextlib  # noqa: PLC0415

            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_background_task

    async def stop(self) -> None:
        """Signal the worker to stop after the current task finishes."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _register(self) -> None:
        info = {
            'worker_id': self._worker_id,
            'hostname': socket.gethostname(),
            'registered_at': datetime.now(timezone.utc).isoformat(),
            'last_heartbeat': datetime.now(timezone.utc).isoformat(),
            'tasks_processed': self._tasks_processed,
            'status': 'active',
        }
        await self._redis.set(
            f'{_WORKER_HEARTBEAT_PREFIX}{self._worker_id}',
            json.dumps(info),
            ex=_DEFAULT_HEARTBEAT_TTL,
        )

        # Also register via HTTP API if dashboard_url is provided
        if self._dashboard_url:
            await self._http_register()

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                info = {
                    'worker_id': self._worker_id,
                    'hostname': socket.gethostname(),
                    'registered_at': datetime.now(timezone.utc).isoformat(),
                    'last_heartbeat': datetime.now(timezone.utc).isoformat(),
                    'tasks_processed': self._tasks_processed,
                    'status': 'active',
                }
                await self._redis.set(
                    f'{_WORKER_HEARTBEAT_PREFIX}{self._worker_id}',
                    json.dumps(info),
                    ex=_DEFAULT_HEARTBEAT_TTL,
                )
                if self._dashboard_url:
                    await self._http_heartbeat()
            except Exception:
                logger.warning('Heartbeat failed for worker %s', self._worker_id, exc_info=True)
            await asyncio.sleep(self._heartbeat_interval)

    async def _consume_loop(self) -> None:
        # Listen on both the generic queue and the worker-specific queue
        keys = [self._queue_key, f'crawlee:tasks:{self._worker_id}']
        while self._running:
            try:
                result = await self._redis.blpop(keys, timeout=5)
            except Exception:
                logger.warning('BLPOP failed, retrying in 2 seconds', exc_info=True)
                await asyncio.sleep(2)
                continue

            if result is None:
                # timeout - loop again to check self._running
                continue

            _key, raw = result
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning('Invalid task payload: %r', raw)
                continue

            await self._process_task(payload)

    async def _process_task(self, payload: dict[str, Any]) -> None:
        task_id: str = payload.get('task_id', str(uuid4()))
        task_data: dict[str, Any] = payload.get('task_data', {})

        logger.info('Worker %s starting task %s', self._worker_id, task_id)
        if self._publisher:
            await self._publisher.set_task_status(task_id, 'running')

        try:
            crawler = self._build_crawler(task_data)
            if crawler is None:
                logger.error('Unsupported crawler type for task %s', task_id)
                if self._publisher:
                    await self._publisher.set_task_status(task_id, 'failed')
                return

            from crawlee import Request as CrawleeRequest  # noqa: PLC0415

            start_urls: list[str] = task_data.get('start_urls', [])
            requests = [CrawleeRequest.from_url(u) for u in start_urls]
            await crawler.run(requests)

            if self._publisher:
                await self._publisher.set_task_status(task_id, 'done')
            self._tasks_processed += 1
            logger.info('Worker %s finished task %s', self._worker_id, task_id)

        except Exception:
            logger.exception('Worker %s: task %s failed', self._worker_id, task_id)
            if self._publisher:
                await self._publisher.set_task_status(task_id, 'failed')

    @staticmethod
    def _build_crawler(task_data: dict[str, Any]) -> object | None:
        """Instantiate the appropriate Crawlee crawler from task data."""
        crawler_type: str = task_data.get('crawler_type', 'http').lower()
        max_requests: int | None = task_data.get('max_requests')
        kwargs: dict[str, Any] = {}
        if max_requests is not None:
            kwargs['max_requests_per_crawl'] = max_requests

        try:
            if crawler_type == 'http':
                from crawlee.crawlers import HttpCrawler  # noqa: PLC0415

                c = HttpCrawler(**kwargs)

                @c.router.default_handler
                async def _h(ctx: object) -> None:
                    pass

                return c

            if crawler_type == 'beautifulsoup':
                from crawlee.crawlers import BeautifulSoupCrawler  # noqa: PLC0415

                c = BeautifulSoupCrawler(**kwargs)

                @c.router.default_handler
                async def _h_bs(ctx: object) -> None:
                    pass

                return c

            if crawler_type == 'parsel':
                from crawlee.crawlers import ParselCrawler  # noqa: PLC0415

                c = ParselCrawler(**kwargs)

                @c.router.default_handler
                async def _h_p(ctx: object) -> None:
                    pass

                return c

            if crawler_type == 'playwright':
                from crawlee.crawlers import PlaywrightCrawler  # noqa: PLC0415

                c = PlaywrightCrawler(**kwargs)

                @c.router.default_handler
                async def _h_pw(ctx: object) -> None:
                    pass

                return c

        except ImportError as exc:
            logger.warning('Crawler type %r not available: %s', crawler_type, exc)
        return None

    # ------------------------------------------------------------------
    # HTTP dashboard integration helpers
    # ------------------------------------------------------------------

    async def _http_register(self) -> None:
        try:
            import httpx  # noqa: PLC0415

            async with httpx.AsyncClient() as client:
                await client.post(
                    f'{self._dashboard_url}/api/workers/{self._worker_id}/register',
                    params={'hostname': socket.gethostname()},
                    timeout=5,
                )
        except Exception:
            logger.debug('HTTP register failed', exc_info=True)

    async def _http_heartbeat(self) -> None:
        try:
            import httpx  # noqa: PLC0415

            async with httpx.AsyncClient() as client:
                await client.post(
                    f'{self._dashboard_url}/api/workers/{self._worker_id}/heartbeat',
                    params={'tasks_processed': self._tasks_processed},
                    timeout=5,
                )
        except Exception:
            logger.debug('HTTP heartbeat failed', exc_info=True)
