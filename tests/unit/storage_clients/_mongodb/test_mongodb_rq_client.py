from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.storage_clients import MongoDBStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from mongomock_motor import AsyncMongoMockClient

    from crawlee.storage_clients._mongodb import MongoDBRequestQueueClient


@pytest.fixture
def mongo_client() -> AsyncMongoMockClient:
    from mongomock_motor import AsyncMongoMockClient as _AsyncMongoMockClient  # noqa: PLC0415

    return _AsyncMongoMockClient()


@pytest.fixture
async def rq_client(
    mongo_client: AsyncMongoMockClient,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[MongoDBRequestQueueClient, None]:
    """A fixture for a MongoDB RQ client."""
    storage_client = MongoDBStorageClient(client=mongo_client)
    client = await storage_client.create_rq_client(name='test_request_queue')
    yield client
    await client.drop()


async def test_metadata_creation(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that MongoDB RQ client creates proper metadata."""
    metadata = await rq_client.get_metadata()

    assert metadata.id is not None
    assert metadata.handled_request_count == 0
    assert metadata.pending_request_count == 0
    assert metadata.total_request_count == 0
    assert metadata.created_at is not None


async def test_request_records_persistence(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that requests are properly persisted to MongoDB."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]

    response = await rq_client.add_batch_of_requests(requests)
    assert len(response.processed_requests) == 3
    assert all(not pr.was_already_present for pr in response.processed_requests)

    metadata = await rq_client.get_metadata()
    assert metadata.pending_request_count == 3
    assert metadata.total_request_count == 3


async def test_duplicate_request_deduplication(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that duplicate requests are deduplicated."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/1'),  # duplicate
    ]

    response = await rq_client.add_batch_of_requests(requests)
    # Input dedup by unique_key means only 1 is processed
    processed_new = [pr for pr in response.processed_requests if not pr.was_already_present]
    assert len(processed_new) == 1


async def test_add_already_existing_request(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that adding an existing request is reported as already present."""
    request = Request.from_url('https://example.com/1')
    await rq_client.add_batch_of_requests([request])

    # Add again
    response = await rq_client.add_batch_of_requests([request])
    assert response.processed_requests[0].was_already_present is True


async def test_fetch_next_request(rq_client: MongoDBRequestQueueClient) -> None:
    """Test fetching the next request from the queue."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    fetched = await rq_client.fetch_next_request()
    assert fetched is not None
    assert fetched.url in ['https://example.com/1', 'https://example.com/2']


async def test_fetch_from_empty_queue(rq_client: MongoDBRequestQueueClient) -> None:
    """Test fetching from an empty queue returns None."""
    fetched = await rq_client.fetch_next_request()
    assert fetched is None


async def test_get_request(rq_client: MongoDBRequestQueueClient) -> None:
    """Test getting a specific request by unique_key."""
    request = Request.from_url('https://example.com/1')
    await rq_client.add_batch_of_requests([request])

    fetched = await rq_client.get_request(request.unique_key)
    assert fetched is not None
    assert fetched.url == 'https://example.com/1'


async def test_get_nonexistent_request(rq_client: MongoDBRequestQueueClient) -> None:
    """Test getting a non-existent request returns None."""
    fetched = await rq_client.get_request('nonexistent-key')
    assert fetched is None


async def test_mark_request_as_handled(rq_client: MongoDBRequestQueueClient) -> None:
    """Test marking a request as handled."""
    request = Request.from_url('https://example.com/1')
    await rq_client.add_batch_of_requests([request])

    fetched = await rq_client.fetch_next_request()
    assert fetched is not None

    result = await rq_client.mark_request_as_handled(fetched)
    assert result is not None
    assert result.was_already_handled is True

    metadata = await rq_client.get_metadata()
    assert metadata.handled_request_count == 1
    assert metadata.pending_request_count == 0


async def test_reclaim_request(rq_client: MongoDBRequestQueueClient) -> None:
    """Test reclaiming a request back to the queue."""
    request = Request.from_url('https://example.com/1')
    await rq_client.add_batch_of_requests([request])

    fetched = await rq_client.fetch_next_request()
    assert fetched is not None

    result = await rq_client.reclaim_request(fetched)
    assert result is not None
    assert result.was_already_handled is False

    # The request should be fetchable again
    fetched_again = await rq_client.fetch_next_request()
    assert fetched_again is not None


async def test_is_empty(rq_client: MongoDBRequestQueueClient) -> None:
    """Test checking if the queue is empty."""
    assert await rq_client.is_empty() is True

    request = Request.from_url('https://example.com/1')
    await rq_client.add_batch_of_requests([request])
    assert await rq_client.is_empty() is False


async def test_drop_removes_records(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that dropping a queue removes all records."""
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    metadata = await rq_client.get_metadata()
    assert metadata.total_request_count == 1

    await rq_client.drop()

    # Verify metadata is removed
    metadata_col = rq_client.database['_metadata']
    doc = await metadata_col.find_one({'storage_type': 'request_queue', 'storage_name': 'test_request_queue'})
    assert doc is None

    # Verify data is removed
    data_col = rq_client.database['rq_requests']
    count = await data_col.count_documents({'storage_name': 'test_request_queue'})
    assert count == 0


async def test_purge_clears_queue(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that purging a queue removes requests but keeps metadata."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
    ]
    await rq_client.add_batch_of_requests(requests)

    await rq_client.purge()

    metadata = await rq_client.get_metadata()
    assert metadata.pending_request_count == 0
    assert metadata.handled_request_count == 0
    assert metadata.total_request_count == 0

    assert await rq_client.is_empty() is True


async def test_full_request_lifecycle(rq_client: MongoDBRequestQueueClient) -> None:
    """Test the full lifecycle: add → fetch → handle."""
    requests = [
        Request.from_url('https://example.com/1'),
        Request.from_url('https://example.com/2'),
        Request.from_url('https://example.com/3'),
    ]
    await rq_client.add_batch_of_requests(requests)

    handled_count = 0
    while True:
        fetched = await rq_client.fetch_next_request()
        if fetched is None:
            break
        await rq_client.mark_request_as_handled(fetched)
        handled_count += 1

    assert handled_count == 3

    metadata = await rq_client.get_metadata()
    assert metadata.handled_request_count == 3
    assert metadata.pending_request_count == 0

    assert await rq_client.is_empty() is True


async def test_metadata_timestamps_update(rq_client: MongoDBRequestQueueClient) -> None:
    """Test that metadata timestamps are updated on operations."""
    metadata_before = await rq_client.get_metadata()
    await asyncio.sleep(0.01)

    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])
    metadata_after = await rq_client.get_metadata()

    assert metadata_after.modified_at >= metadata_before.modified_at
    assert metadata_after.accessed_at >= metadata_before.accessed_at
