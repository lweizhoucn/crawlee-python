from __future__ import annotations

import warnings

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient

from ._dataset_client import MongoDBDatasetClient
from ._key_value_store_client import MongoDBKeyValueStoreClient
from ._request_queue_client import MongoDBRequestQueueClient


@docs_group('Storage clients')
class MongoDBStorageClient(StorageClient):
    """MongoDB implementation of the storage client.

    This storage client provides access to datasets, key-value stores, and request queues that persist data
    to a MongoDB database. Each storage type uses dedicated collections with appropriate indexes.

    The client accepts either a MongoDB connection string or a pre-configured ``AsyncIOMotorClient`` instance.
    Exactly one of these parameters must be provided during initialization.

    Warning:
        This is an experimental feature. The behavior and interface may change in future versions.
    """

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        client: AsyncIOMotorClient | None = None,
        database_name: str = 'crawlee',
    ) -> None:
        """Initialize the MongoDB storage client.

        Args:
            connection_string: MongoDB connection URI (e.g., ``"mongodb://localhost:27017"``).
            client: Pre-configured ``AsyncIOMotorClient`` instance.
            database_name: Name of the MongoDB database to use.
        """
        if client is None and connection_string is None:
            raise ValueError('Either client or connection_string must be provided.')

        if client is not None and connection_string is not None:
            raise ValueError('Either client or connection_string must be provided, not both.')

        if client is not None:
            self._client = client
        else:
            self._client = AsyncIOMotorClient(connection_string)

        self._database = self._client[database_name]
        self._initialized = False

        warnings.warn(
            'MongoDBStorageClient is experimental and its API, behavior, and collection structure may change '
            'in future releases.',
            category=UserWarning,
            stacklevel=2,
        )

    async def _ensure_initialized(self) -> None:
        """Create required indexes on the metadata collection if not already done."""
        if self._initialized:
            return
        metadata_col = self._database['_metadata']
        await metadata_col.create_index(
            [('storage_type', ASCENDING), ('storage_name', ASCENDING)],
            unique=True,
        )
        await metadata_col.create_index(
            [('storage_type', ASCENDING), ('storage_id', ASCENDING)],
            unique=True,
        )
        self._initialized = True

    @override
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> MongoDBDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()
        await self._ensure_initialized()

        client = await MongoDBDatasetClient.open(
            id=id,
            name=name,
            alias=alias,
            database=self._database,
        )
        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_kvs_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> MongoDBKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()
        await self._ensure_initialized()

        client = await MongoDBKeyValueStoreClient.open(
            id=id,
            name=name,
            alias=alias,
            database=self._database,
        )
        await self._purge_if_needed(client, configuration)
        return client

    @override
    async def create_rq_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> MongoDBRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()
        await self._ensure_initialized()

        client = await MongoDBRequestQueueClient.open(
            id=id,
            name=name,
            alias=alias,
            database=self._database,
        )
        await self._purge_if_needed(client, configuration)
        return client
