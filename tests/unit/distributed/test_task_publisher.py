from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawlee.distributed._task_publisher import TaskPublisher, Worker

# ---------------------------------------------------------------------------
# TaskPublisher tests (using a fake Redis client)
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis() -> MagicMock:
    redis = MagicMock()
    redis.ping = AsyncMock()
    redis.rpush = AsyncMock(return_value=1)
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value='pending')
    redis.keys = AsyncMock(return_value=[])
    redis.aclose = AsyncMock()
    return redis


async def test_publisher_publish(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher(redis_url='redis://localhost:6379')
    publisher._redis = fake_redis

    task_id = await publisher.publish(
        task_id='test-task-1',
        task_data={'crawler_type': 'http', 'start_urls': ['https://example.com']},
    )

    assert task_id == 'test-task-1'
    fake_redis.rpush.assert_awaited_once()
    # Verify the payload was JSON-encoded and pushed to correct queue
    call_args = fake_redis.rpush.call_args
    assert call_args[0][0] == 'crawlee:tasks'
    payload = json.loads(call_args[0][1])
    assert payload['task_id'] == 'test-task-1'


async def test_publisher_publish_generates_task_id(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis

    task_id = await publisher.publish(task_data={'crawler_type': 'http', 'start_urls': []})
    assert len(task_id) == 36  # UUID4 length


async def test_publisher_publish_target_worker(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis

    await publisher.publish(
        task_id='targeted',
        task_data={'crawler_type': 'http', 'start_urls': []},
        target_worker_id='worker-42',
    )

    call_args = fake_redis.rpush.call_args
    # Should be pushed to worker-specific queue
    assert call_args[0][0] == 'crawlee:tasks:worker-42'


async def test_publisher_get_task_status(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis

    status = await publisher.get_task_status('test-task-1')
    assert status == 'pending'
    fake_redis.get.assert_awaited_once_with('crawlee:task_status:test-task-1')


async def test_publisher_set_task_status(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis

    await publisher.set_task_status('t1', 'done')
    fake_redis.set.assert_awaited_once_with('crawlee:task_status:t1', 'done')


async def test_publisher_list_workers_empty(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis
    # keys returns empty list
    fake_redis.keys = AsyncMock(return_value=[])

    workers = await publisher.list_workers()
    assert workers == []


async def test_publisher_list_workers(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis

    worker_data = {'worker_id': 'w1', 'hostname': 'h1', 'status': 'active'}
    fake_redis.keys = AsyncMock(return_value=['crawlee:worker:w1'])
    fake_redis.get = AsyncMock(return_value=json.dumps(worker_data))

    workers = await publisher.list_workers()
    assert len(workers) == 1
    assert workers[0]['worker_id'] == 'w1'


async def test_publisher_close(fake_redis: MagicMock) -> None:
    publisher = TaskPublisher()
    publisher._redis = fake_redis

    await publisher.close()
    fake_redis.aclose.assert_awaited_once()
    assert publisher._redis is None


async def test_publisher_close_noop_when_not_connected() -> None:
    publisher = TaskPublisher()
    # Should not raise
    await publisher.close()


# ---------------------------------------------------------------------------
# Worker tests
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis_worker() -> MagicMock:
    redis = MagicMock()
    redis.ping = AsyncMock()
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value='pending')
    redis.rpush = AsyncMock(return_value=1)
    redis.keys = AsyncMock(return_value=[])
    redis.aclose = AsyncMock()
    # Return None immediately to simulate empty queue / timeout
    redis.blpop = AsyncMock(return_value=None)
    return redis


async def test_worker_register(fake_redis_worker: MagicMock) -> None:
    worker = Worker(redis_url='redis://localhost:6379', worker_id='w1')
    worker._redis = fake_redis_worker
    worker._publisher = TaskPublisher()
    worker._publisher._redis = fake_redis_worker

    await worker._register()
    fake_redis_worker.set.assert_awaited()
    # Verify the heartbeat key format
    call_args = fake_redis_worker.set.call_args
    assert call_args[0][0] == 'crawlee:worker:w1'


async def test_worker_id_generated_if_not_provided() -> None:
    worker = Worker()
    assert len(worker.worker_id) == 36


async def test_worker_stop() -> None:
    worker = Worker(worker_id='w1')
    worker._running = True
    await worker.stop()
    assert worker._running is False


async def test_worker_process_task_unknown_type(fake_redis_worker: MagicMock) -> None:
    """Worker should gracefully handle an unknown crawler type."""
    worker = Worker(worker_id='w1')
    worker._redis = fake_redis_worker
    worker._publisher = TaskPublisher()
    worker._publisher._redis = fake_redis_worker

    payload = {
        'task_id': 'x1',
        'task_data': {'crawler_type': 'unknown_xyz', 'start_urls': ['https://example.com']},
    }
    # Should not raise - just logs an error and sets status to failed
    await worker._process_task(payload)
    # Status should be set to failed
    fake_redis_worker.set.assert_awaited()


async def test_worker_build_crawler_http() -> None:
    crawler = Worker._build_crawler({'crawler_type': 'http', 'start_urls': []})
    assert crawler is not None


async def test_worker_build_crawler_unknown() -> None:
    crawler = Worker._build_crawler({'crawler_type': 'does_not_exist', 'start_urls': []})
    assert crawler is None
