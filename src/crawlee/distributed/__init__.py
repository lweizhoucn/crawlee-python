"""Crawlee distributed task publishing.

Provides Redis-backed task publishing and worker node implementations for
running crawl tasks across multiple machines or processes.

Example - publish a task::

    from crawlee.distributed import TaskPublisher

    publisher = TaskPublisher(redis_url="redis://localhost:6379")
    await publisher.connect()
    await publisher.publish(task_data={"crawler_type": "http", "start_urls": ["https://example.com"]})
    await publisher.close()

Example - run a worker::

    from crawlee.distributed import Worker

    worker = Worker(redis_url="redis://localhost:6379")
    await worker.run()  # blocks until stopped
"""

from crawlee.distributed._task_publisher import TaskPublisher, Worker

__all__ = ['TaskPublisher', 'Worker']
