"""Crawlee admin dashboard.

Provides a web-based UI and REST API for managing and monitoring Crawlee crawl tasks,
with optional distributed task publishing via Redis.

Example usage::

    import asyncio
    from crawlee.dashboard import CrawleeDashboard

    async def main():
        dashboard = CrawleeDashboard(port=8080)
        await dashboard.serve()

    asyncio.run(main())
"""

from crawlee.dashboard._models import (
    PublishTaskRequest,
    QueueStats,
    TaskCreate,
    TaskInfo,
    TaskStatus,
    WorkerInfo,
)
from crawlee.dashboard._server import CrawleeDashboard

__all__ = [
    'CrawleeDashboard',
    'PublishTaskRequest',
    'QueueStats',
    'TaskCreate',
    'TaskInfo',
    'TaskStatus',
    'WorkerInfo',
]
