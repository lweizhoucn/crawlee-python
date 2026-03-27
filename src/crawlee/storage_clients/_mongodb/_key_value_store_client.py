from __future__ import annotations

import json
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING
from typing_extensions import override

from crawlee._utils.file import infer_mime_type
from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

from ._client_mixin import MetadataUpdateParams, MongoDBClientMixin

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from motor.motor_asyncio import AsyncIOMotorDatabase

logger = getLogger(__name__)

_COLLECTION_NAME = 'kvs_records'


class MongoDBKeyValueStoreClient(KeyValueStoreClient, MongoDBClientMixin):
    """MongoDB implementation of the key-value store client.

    This client persists key-value data to MongoDB. Records are stored as documents in a shared collection,
    partitioned by ``storage_name``, with a unique compound index on ``(storage_name, key)``.

    The key-value store data is stored in the ``kvs_records`` collection with the following document structure::

        { "storage_name": str, "key": str, "value": bytes, "content_type": str, "size": int }

    Values are serialized based on their type: JSON objects are stored as UTF-8 encoded bytes,
    text values as UTF-8 encoded bytes, and binary data as-is.
    """

    _STORAGE_TYPE = 'key_value_store'
    _CLIENT_TYPE = 'Key-value store'

    def __init__(self, storage_name: str, storage_id: str, database: AsyncIOMotorDatabase) -> None:
        """Initialize a new instance.

        Preferably use the ``MongoDBKeyValueStoreClient.open`` class method to create a new instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, database=database)

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        database: AsyncIOMotorDatabase,
    ) -> MongoDBKeyValueStoreClient:
        """Open or create a new MongoDB key-value store client.

        Args:
            id: The ID of the key-value store.
            name: The name of the key-value store for named (global scope) storages.
            alias: The alias of the key-value store for unnamed (run scope) storages.
            database: MongoDB database instance.

        Returns:
            An instance for the opened or created storage client.
        """
        return await cls._open(
            id=id,
            name=name,
            alias=alias,
            database=database,
            metadata_model=KeyValueStoreMetadata,
            extra_metadata_fields={},
            instance_kwargs={},
        )

    @override
    async def _ensure_indexes(self) -> None:
        col = self._database[_COLLECTION_NAME]
        await col.create_index(
            [('storage_name', ASCENDING), ('key', ASCENDING)],
            unique=True,
        )

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        return await self._get_metadata(KeyValueStoreMetadata)

    @override
    async def drop(self) -> None:
        await self._drop(collection_name=_COLLECTION_NAME)

    @override
    async def purge(self) -> None:
        await self._purge(
            collection_name=_COLLECTION_NAME,
            metadata_kwargs=MetadataUpdateParams(update_accessed_at=True, update_modified_at=True),
        )

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        if value is None:
            content_type = 'application/x-none'
            value_bytes = b''
        else:
            content_type = content_type or infer_mime_type(value)

            if 'application/json' in content_type:
                value_bytes = json.dumps(value, default=str, ensure_ascii=False).encode('utf-8')
            elif isinstance(value, str):
                value_bytes = value.encode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                value_bytes = bytes(value)
            else:
                value_bytes = str(value).encode('utf-8')

        size = len(value_bytes)

        col = self._database[_COLLECTION_NAME]
        await col.update_one(
            {'storage_name': self._storage_name, 'key': key},
            {'$set': {
                'storage_name': self._storage_name,
                'key': key,
                'value': value_bytes,
                'content_type': content_type,
                'size': size,
            }},
            upsert=True,
        )
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        col = self._database[_COLLECTION_NAME]
        doc = await col.find_one(
            {'storage_name': self._storage_name, 'key': key},
            projection={'_id': 0},
        )

        await self._update_metadata(update_accessed_at=True)

        if doc is None:
            return None

        stored_content_type: str = doc['content_type']
        value_bytes: bytes = doc['value']

        # Deserialize based on content_type
        if stored_content_type == 'application/x-none':
            value = None
        elif 'application/json' in stored_content_type:
            try:
                value = json.loads(value_bytes.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f'Failed to decode JSON value for key "{key}"')
                return None
        elif stored_content_type.startswith('text/'):
            try:
                value = value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(f'Failed to decode text value for key "{key}"')
                return None
        else:
            value = value_bytes

        return KeyValueStoreRecord(
            key=doc['key'],
            value=value,
            content_type=stored_content_type,
            size=doc.get('size'),
        )

    @override
    async def delete_value(self, *, key: str) -> None:
        col = self._database[_COLLECTION_NAME]
        await col.delete_one({'storage_name': self._storage_name, 'key': key})
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        col = self._database[_COLLECTION_NAME]
        query: dict[str, Any] = {'storage_name': self._storage_name}
        if exclusive_start_key is not None:
            query['key'] = {'$gt': exclusive_start_key}

        cursor = col.find(
            query,
            projection={'_id': 0, 'key': 1, 'content_type': 1, 'size': 1},
        ).sort('key', ASCENDING)

        if limit is not None:
            cursor = cursor.limit(limit)

        async for doc in cursor:
            yield KeyValueStoreRecordMetadata(
                key=doc['key'],
                content_type=doc['content_type'],
                size=doc.get('size'),
            )

        await self._update_metadata(update_accessed_at=True)

    @override
    async def get_public_url(self, *, key: str) -> str:
        raise NotImplementedError('Public URLs are not supported for MongoDB key-value stores.')

    @override
    async def record_exists(self, *, key: str) -> bool:
        col = self._database[_COLLECTION_NAME]
        count = await col.count_documents(
            {'storage_name': self._storage_name, 'key': key},
            limit=1,
        )
        await self._update_metadata(update_accessed_at=True)
        return count > 0
