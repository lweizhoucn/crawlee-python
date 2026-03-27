from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.storage_clients import MongoDBStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from mongomock_motor import AsyncMongoMockClient

    from crawlee.storage_clients._mongodb import MongoDBDatasetClient


@pytest.fixture
def mongo_client() -> AsyncMongoMockClient:
    from mongomock_motor import AsyncMongoMockClient as _AsyncMongoMockClient  # noqa: PLC0415

    return _AsyncMongoMockClient()


@pytest.fixture
async def dataset_client(
    mongo_client: AsyncMongoMockClient,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[MongoDBDatasetClient, None]:
    """A fixture for a MongoDB dataset client."""
    storage_client = MongoDBStorageClient(client=mongo_client)
    client = await storage_client.create_dataset_client(name='test_dataset')
    yield client
    await client.drop()


async def test_metadata_creation(dataset_client: MongoDBDatasetClient) -> None:
    """Test that MongoDB dataset client creates proper metadata."""
    metadata = await dataset_client.get_metadata()

    assert metadata.id is not None
    assert metadata.item_count == 0
    assert metadata.created_at is not None
    assert metadata.accessed_at is not None
    assert metadata.modified_at is not None


async def test_record_and_content_verification(dataset_client: MongoDBDatasetClient) -> None:
    """Test that data is properly persisted to MongoDB with correct content."""
    item = {'key': 'value', 'number': 42}
    await dataset_client.push_data(item)

    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 1

    data = await dataset_client.get_data()
    assert data.count == 1
    assert data.items[0] == item

    # Test multiple records
    items = [{'id': 1, 'name': 'Item 1'}, {'id': 2, 'name': 'Item 2'}, {'id': 3, 'name': 'Item 3'}]
    await dataset_client.push_data(items)

    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 4

    data = await dataset_client.get_data()
    assert data.count == 4


async def test_get_data_with_pagination(dataset_client: MongoDBDatasetClient) -> None:
    """Test get_data with offset and limit."""
    items = [{'id': i} for i in range(10)]
    await dataset_client.push_data(items)

    # Test offset
    data = await dataset_client.get_data(offset=3)
    assert data.count == 7
    assert data.items[0] == {'id': 3}

    # Test limit
    data = await dataset_client.get_data(limit=3)
    assert data.count == 3
    assert data.items[0] == {'id': 0}

    # Test offset + limit
    data = await dataset_client.get_data(offset=2, limit=3)
    assert data.count == 3
    assert data.items[0] == {'id': 2}
    assert data.items[2] == {'id': 4}


async def test_get_data_descending(dataset_client: MongoDBDatasetClient) -> None:
    """Test get_data with descending order."""
    items = [{'id': i} for i in range(5)]
    await dataset_client.push_data(items)

    data = await dataset_client.get_data(desc=True)
    assert data.items[0] == {'id': 4}
    assert data.items[4] == {'id': 0}


async def test_get_data_skip_empty(dataset_client: MongoDBDatasetClient) -> None:
    """Test get_data with skip_empty flag."""
    items = [{'id': 1}, {}, {'id': 3}]
    await dataset_client.push_data(items)

    data = await dataset_client.get_data(skip_empty=True)
    assert data.count == 2


async def test_iterate_items(dataset_client: MongoDBDatasetClient) -> None:
    """Test iterating over dataset items."""
    items = [{'id': i} for i in range(5)]
    await dataset_client.push_data(items)

    collected = [item async for item in dataset_client.iterate_items()]

    assert len(collected) == 5
    assert collected[0] == {'id': 0}
    assert collected[4] == {'id': 4}


async def test_iterate_items_with_offset_and_limit(dataset_client: MongoDBDatasetClient) -> None:
    """Test iterating with offset and limit."""
    items = [{'id': i} for i in range(10)]
    await dataset_client.push_data(items)

    collected = [item async for item in dataset_client.iterate_items(offset=2, limit=3)]

    assert len(collected) == 3
    assert collected[0] == {'id': 2}


async def test_drop_removes_records(dataset_client: MongoDBDatasetClient) -> None:
    """Test that dropping a dataset removes all records from MongoDB."""
    await dataset_client.push_data({'test': 'data'})

    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 1

    await dataset_client.drop()

    # Verify metadata is removed
    metadata_col = dataset_client.database['_metadata']
    doc = await metadata_col.find_one({'storage_type': 'dataset', 'storage_name': 'test_dataset'})
    assert doc is None

    # Verify data is removed
    data_col = dataset_client.database['dataset_items']
    count = await data_col.count_documents({'storage_name': 'test_dataset'})
    assert count == 0


async def test_purge_clears_items(dataset_client: MongoDBDatasetClient) -> None:
    """Test that purging a dataset removes items but keeps metadata."""
    await dataset_client.push_data([{'id': 1}, {'id': 2}])

    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 2

    await dataset_client.purge()

    metadata = await dataset_client.get_metadata()
    assert metadata.item_count == 0

    data = await dataset_client.get_data()
    assert data.count == 0


async def test_metadata_timestamps_update(dataset_client: MongoDBDatasetClient) -> None:
    """Test that metadata timestamps are updated on operations."""
    metadata_before = await dataset_client.get_metadata()
    await asyncio.sleep(0.01)

    await dataset_client.push_data({'key': 'value'})
    metadata_after = await dataset_client.get_metadata()

    assert metadata_after.modified_at >= metadata_before.modified_at
    assert metadata_after.accessed_at >= metadata_before.accessed_at


async def test_reopen_persistence(
    mongo_client: AsyncMongoMockClient,
    suppress_user_warning: None,  # noqa: ARG001
) -> None:
    """Test that data persists when reopening with the same name."""
    storage = MongoDBStorageClient(client=mongo_client)
    client1 = await storage.create_dataset_client(name='persist_test')
    await client1.push_data({'hello': 'world'})

    # Reopen with the same name
    client2 = await storage.create_dataset_client(name='persist_test')
    data = await client2.get_data()
    assert data.count == 1
    assert data.items[0] == {'hello': 'world'}

    await client2.drop()
