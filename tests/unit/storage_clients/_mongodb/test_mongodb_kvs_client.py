from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee.storage_clients import MongoDBStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from mongomock_motor import AsyncMongoMockClient

    from crawlee.storage_clients._mongodb import MongoDBKeyValueStoreClient


@pytest.fixture
def mongo_client() -> AsyncMongoMockClient:
    from mongomock_motor import AsyncMongoMockClient as _AsyncMongoMockClient  # noqa: PLC0415

    return _AsyncMongoMockClient()


@pytest.fixture
async def kvs_client(
    mongo_client: AsyncMongoMockClient,
    suppress_user_warning: None,  # noqa: ARG001
) -> AsyncGenerator[MongoDBKeyValueStoreClient, None]:
    """A fixture for a MongoDB KVS client."""
    storage_client = MongoDBStorageClient(client=mongo_client)
    client = await storage_client.create_kvs_client(name='test_kvs')
    yield client
    await client.drop()


async def test_metadata_creation(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that MongoDB KVS client creates proper metadata."""
    metadata = await kvs_client.get_metadata()

    assert metadata.id is not None
    assert metadata.created_at is not None
    assert metadata.accessed_at is not None
    assert metadata.modified_at is not None


async def test_value_record_creation_and_content(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that values are properly persisted with correct content and metadata."""
    test_key = 'test-key'
    test_value = 'Hello, world!'
    await kvs_client.set_value(key=test_key, value=test_value)

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert record.key == test_key
    assert record.content_type == 'text/plain; charset=utf-8'
    assert record.size == len(test_value.encode('utf-8'))


async def test_binary_data_persistence(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that binary data is stored correctly without corruption."""
    test_key = 'test-binary'
    test_value = b'\x00\x01\x02\x03\x04'
    await kvs_client.set_value(key=test_key, value=test_value)

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert record.content_type == 'application/octet-stream'


async def test_json_serialization(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that JSON objects are properly serialized and deserialized."""
    test_key = 'test-json'
    test_value = {'name': 'John', 'age': 30, 'items': [1, 2, 3]}
    await kvs_client.set_value(key=test_key, value=test_value)

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == test_value
    assert 'application/json' in record.content_type


async def test_none_value(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that None values are properly stored and retrieved."""
    test_key = 'test-none'
    await kvs_client.set_value(key=test_key, value=None)

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value is None
    assert record.content_type == 'application/x-none'


async def test_value_overwrite(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that setting a value with an existing key overwrites it."""
    test_key = 'overwrite-key'
    await kvs_client.set_value(key=test_key, value='original')

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == 'original'

    await kvs_client.set_value(key=test_key, value='updated')

    record = await kvs_client.get_value(key=test_key)
    assert record is not None
    assert record.value == 'updated'


async def test_delete_value(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that deleting a value removes it from the store."""
    test_key = 'test-delete'
    await kvs_client.set_value(key=test_key, value='Delete me')

    record = await kvs_client.get_value(key=test_key)
    assert record is not None

    await kvs_client.delete_value(key=test_key)

    record = await kvs_client.get_value(key=test_key)
    assert record is None


async def test_get_nonexistent_value(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that getting a non-existent key returns None."""
    record = await kvs_client.get_value(key='nonexistent')
    assert record is None


async def test_iterate_keys(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test iterating over all keys in the store."""
    await kvs_client.set_value(key='key-a', value='a')
    await kvs_client.set_value(key='key-b', value='b')
    await kvs_client.set_value(key='key-c', value='c')

    keys = [meta.key async for meta in kvs_client.iterate_keys()]

    assert sorted(keys) == ['key-a', 'key-b', 'key-c']


async def test_iterate_keys_with_exclusive_start(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test iterating keys with exclusive_start_key."""
    await kvs_client.set_value(key='key-a', value='a')
    await kvs_client.set_value(key='key-b', value='b')
    await kvs_client.set_value(key='key-c', value='c')

    keys = [meta.key async for meta in kvs_client.iterate_keys(exclusive_start_key='key-a')]

    assert keys == ['key-b', 'key-c']


async def test_iterate_keys_with_limit(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test iterating keys with a limit."""
    await kvs_client.set_value(key='key-a', value='a')
    await kvs_client.set_value(key='key-b', value='b')
    await kvs_client.set_value(key='key-c', value='c')

    keys = [meta.key async for meta in kvs_client.iterate_keys(limit=2)]

    assert len(keys) == 2


async def test_record_exists(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test checking if a record exists."""
    test_key = 'exists-key'
    assert await kvs_client.record_exists(key=test_key) is False

    await kvs_client.set_value(key=test_key, value='value')
    assert await kvs_client.record_exists(key=test_key) is True


async def test_drop_removes_records(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that dropping a KVS removes all records."""
    await kvs_client.set_value(key='test', value='data')

    await kvs_client.drop()

    # Verify metadata is removed
    metadata_col = kvs_client.database['_metadata']
    doc = await metadata_col.find_one({'storage_type': 'key_value_store', 'storage_name': 'test_kvs'})
    assert doc is None


async def test_purge_clears_items(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that purging a KVS removes items but keeps metadata."""
    await kvs_client.set_value(key='key1', value='value1')
    await kvs_client.set_value(key='key2', value='value2')

    await kvs_client.purge()

    record = await kvs_client.get_value(key='key1')
    assert record is None

    # Metadata should still exist
    metadata = await kvs_client.get_metadata()
    assert metadata.id is not None


async def test_metadata_timestamps_update(kvs_client: MongoDBKeyValueStoreClient) -> None:
    """Test that metadata timestamps are updated on operations."""
    metadata_before = await kvs_client.get_metadata()
    await asyncio.sleep(0.01)

    await kvs_client.set_value(key='key', value='value')
    metadata_after = await kvs_client.get_metadata()

    assert metadata_after.modified_at >= metadata_before.modified_at
    assert metadata_after.accessed_at >= metadata_before.accessed_at
