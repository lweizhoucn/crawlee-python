from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from crawlee.dashboard._models import TaskCreate, TaskStatus
from crawlee.dashboard._server import CrawleeDashboard, _TaskRegistry, _WorkerRegistry


@pytest.fixture
def dashboard() -> CrawleeDashboard:
    return CrawleeDashboard(port=0)


@pytest.fixture
def client(dashboard: CrawleeDashboard) -> TestClient:
    return TestClient(dashboard.app)


# ---------------------------------------------------------------------------
# _TaskRegistry unit tests
# ---------------------------------------------------------------------------


async def test_task_registry_create_and_get_all() -> None:
    registry = _TaskRegistry()
    req = TaskCreate(name='test', start_urls=['https://example.com'], crawler_type='http')
    task = await registry.create(req)
    assert task.name == 'test'
    assert task.status == TaskStatus.PENDING

    all_tasks = await registry.get_all()
    assert len(all_tasks) == 1
    assert all_tasks[0].id == task.id


async def test_task_registry_update() -> None:
    registry = _TaskRegistry()
    req = TaskCreate(name='t', start_urls=['https://example.com'])
    task = await registry.create(req)

    updated = await registry.update(task.id, status=TaskStatus.RUNNING)
    assert updated is not None
    assert updated.status == TaskStatus.RUNNING


async def test_task_registry_delete() -> None:
    registry = _TaskRegistry()
    req = TaskCreate(name='t', start_urls=['https://example.com'])
    task = await registry.create(req)

    existed = await registry.delete(task.id)
    assert existed is True
    assert await registry.get(task.id) is None


async def test_task_registry_stats() -> None:
    registry = _TaskRegistry()
    for name, status in [('a', TaskStatus.RUNNING), ('b', TaskStatus.DONE), ('c', TaskStatus.FAILED)]:
        req = TaskCreate(name=name, start_urls=['https://example.com'])
        task = await registry.create(req)
        await registry.update(task.id, status=status)

    stats = await registry.stats()
    assert stats['running'] == 1
    assert stats['done'] == 1
    assert stats['failed'] == 1
    assert stats['total'] == 3


# ---------------------------------------------------------------------------
# _WorkerRegistry unit tests
# ---------------------------------------------------------------------------


async def test_worker_registry_register() -> None:
    registry = _WorkerRegistry()
    worker = await registry.register('w1', 'host1')
    assert worker.worker_id == 'w1'
    assert worker.hostname == 'host1'

    all_workers = await registry.get_all()
    assert len(all_workers) == 1


async def test_worker_registry_heartbeat() -> None:
    registry = _WorkerRegistry()
    await registry.register('w1', 'host1')
    await registry.heartbeat('w1', tasks_processed=5)

    workers = await registry.get_all()
    assert workers[0].tasks_processed == 5


async def test_worker_registry_count_active() -> None:
    registry = _WorkerRegistry()
    # Register a worker with a recent heartbeat
    await registry.register('w1', 'host1')
    count = await registry.count_active()
    assert count == 1


# ---------------------------------------------------------------------------
# REST API endpoint tests (via TestClient)
# ---------------------------------------------------------------------------


def test_dashboard_index(client: TestClient) -> None:
    resp = client.get('/')
    assert resp.status_code == 200
    assert 'Crawlee Dashboard' in resp.text


def test_api_stats_empty(client: TestClient) -> None:
    resp = client.get('/api/stats')
    assert resp.status_code == 200
    data = resp.json()
    assert data['total'] == 0
    assert data['running'] == 0


def test_api_tasks_list_empty(client: TestClient) -> None:
    resp = client.get('/api/tasks')
    assert resp.status_code == 200
    assert resp.json() == []


def test_api_task_create(client: TestClient) -> None:
    payload = {'name': 'my-task', 'start_urls': ['https://example.com'], 'crawler_type': 'http'}
    resp = client.post('/api/tasks', json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data['name'] == 'my-task'
    assert data['status'] == 'pending'
    assert 'id' in data


def test_api_task_get(client: TestClient) -> None:
    # First create a task
    payload = {'name': 'get-task', 'start_urls': ['https://example.com']}
    create_resp = client.post('/api/tasks', json=payload)
    task_id = create_resp.json()['id']

    # Then retrieve it
    get_resp = client.get(f'/api/tasks/{task_id}')
    assert get_resp.status_code == 200
    assert get_resp.json()['id'] == task_id


def test_api_task_get_not_found(client: TestClient) -> None:
    resp = client.get('/api/tasks/nonexistent-id')
    assert resp.status_code == 404


def test_api_task_delete(client: TestClient) -> None:
    payload = {'name': 'delete-me', 'start_urls': ['https://example.com']}
    create_resp = client.post('/api/tasks', json=payload)
    task_id = create_resp.json()['id']

    del_resp = client.delete(f'/api/tasks/{task_id}')
    assert del_resp.status_code == 200

    # Should be gone
    get_resp = client.get(f'/api/tasks/{task_id}')
    assert get_resp.status_code == 404


def test_api_task_delete_not_found(client: TestClient) -> None:
    resp = client.delete('/api/tasks/ghost-id')
    assert resp.status_code == 404


def test_api_workers_list_empty(client: TestClient) -> None:
    resp = client.get('/api/workers')
    assert resp.status_code == 200
    assert resp.json() == []


def test_api_worker_register(client: TestClient) -> None:
    resp = client.post('/api/workers/w42/register', params={'hostname': 'myhost'})
    assert resp.status_code == 200
    data = resp.json()
    assert data['worker_id'] == 'w42'
    assert data['hostname'] == 'myhost'


def test_api_worker_heartbeat(client: TestClient) -> None:
    # Register first
    client.post('/api/workers/w99/register', params={'hostname': 'host99'})
    # Heartbeat
    resp = client.post('/api/workers/w99/heartbeat', params={'tasks_processed': 3})
    assert resp.status_code == 200
    assert resp.json()['ok'] is True


def test_api_publish_without_redis(client: TestClient) -> None:
    """Publish should fallback to local run when no Redis is configured."""
    payload = {
        'task': {
            'name': 'pub-task',
            'start_urls': ['https://example.com'],
            'crawler_type': 'http',
        }
    }
    resp = client.post('/api/publish', json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data['name'] == 'pub-task'


def test_api_resume_not_paused(client: TestClient) -> None:
    """Resume a task that is not paused - should return 400."""
    payload = {'name': 'p-task', 'start_urls': ['https://example.com']}
    create_resp = client.post('/api/tasks', json=payload)
    task_id = create_resp.json()['id']

    # Task is either PENDING or RUNNING (background task may have started).
    # Try to resume it - it's not PAUSED so it should return 400.
    resp = client.post(f'/api/tasks/{task_id}/resume')
    assert resp.status_code == 400
