from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, DESCENDING
from typing_extensions import NotRequired, override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

from ._client_mixin import MetadataUpdateParams, MongoDBClientMixin

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from motor.motor_asyncio import AsyncIOMotorDatabase

logger = getLogger(__name__)

_COLLECTION_NAME = 'dataset_items'


class _DatasetMetadataUpdateParams(MetadataUpdateParams):
    """Parameters for updating dataset metadata."""

    new_item_count: NotRequired[int]
    delta_item_count: NotRequired[int]


class MongoDBDatasetClient(DatasetClient, MongoDBClientMixin):
    """MongoDB implementation of the dataset client.

    This client persists dataset items to MongoDB. Items are stored as documents in a shared collection,
    partitioned by ``storage_name``, with an ``order_no`` field to preserve insertion order.

    The dataset data is stored in the ``dataset_items`` collection with the following document structure::

        { "storage_name": str, "order_no": int, "data": dict }

    All operations provide consistency through MongoDB's atomic single-document operations
    and appropriate indexes for efficient querying.
    """

    _STORAGE_TYPE = 'dataset'
    _CLIENT_TYPE = 'Dataset'

    def __init__(self, storage_name: str, storage_id: str, database: AsyncIOMotorDatabase) -> None:
        """Initialize a new instance.

        Preferably use the ``MongoDBDatasetClient.open`` class method to create a new instance.
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
    ) -> MongoDBDatasetClient:
        """Open or create a new MongoDB dataset client.

        Args:
            id: The ID of the dataset. If not provided, a random ID will be generated.
            name: The name of the dataset for named (global scope) storages.
            alias: The alias of the dataset for unnamed (run scope) storages.
            database: MongoDB database instance.

        Returns:
            An instance for the opened or created storage client.
        """
        return await cls._open(
            id=id,
            name=name,
            alias=alias,
            database=database,
            metadata_model=DatasetMetadata,
            extra_metadata_fields={'item_count': 0},
            instance_kwargs={},
        )

    @override
    async def _ensure_indexes(self) -> None:
        col = self._database[_COLLECTION_NAME]
        await col.create_index([('storage_name', ASCENDING), ('order_no', ASCENDING)])

    @override
    async def get_metadata(self) -> DatasetMetadata:
        return await self._get_metadata(DatasetMetadata)

    @override
    async def drop(self) -> None:
        await self._drop(collection_name=_COLLECTION_NAME)

    @override
    async def purge(self) -> None:
        await self._purge(
            collection_name=_COLLECTION_NAME,
            metadata_kwargs=_DatasetMetadataUpdateParams(
                new_item_count=0, update_accessed_at=True, update_modified_at=True
            ),
        )
        # Reset item_count in metadata
        metadata_col = self._database[self._METADATA_COLLECTION]
        await metadata_col.update_one(
            {'storage_type': self._STORAGE_TYPE, 'storage_name': self._storage_name},
            {'$set': {'item_count': 0}},
        )

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        if isinstance(data, dict):
            data = [data]

        col = self._database[_COLLECTION_NAME]

        # Get the current max order_no for this storage
        last_doc = await col.find_one(
            {'storage_name': self._storage_name},
            sort=[('order_no', DESCENDING)],
            projection={'order_no': 1},
        )
        next_order = (last_doc['order_no'] + 1) if last_doc else 0

        docs = [
            {'storage_name': self._storage_name, 'order_no': next_order + i, 'data': item}
            for i, item in enumerate(data)
        ]
        await col.insert_many(docs)

        # Update metadata: increment item_count
        metadata_col = self._database[self._METADATA_COLLECTION]
        await metadata_col.update_one(
            {'storage_type': self._STORAGE_TYPE, 'storage_name': self._storage_name},
            {'$inc': {'item_count': len(data)}},
        )
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

    @override
    async def get_data(
        self,
        *,
        offset: int = 0,
        limit: int | None = 999_999_999_999,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
        flatten: list[str] | None = None,
        view: str | None = None,
    ) -> DatasetItemsListPage:
        unsupported_args: dict[str, Any] = {
            'clean': clean,
            'fields': fields,
            'omit': omit,
            'unwind': unwind,
            'skip_hidden': skip_hidden,
            'flatten': flatten,
            'view': view,
        }
        unsupported = {k: v for k, v in unsupported_args.items() if v not in (False, None)}
        if unsupported:
            logger.warning(
                f'The arguments {list(unsupported.keys())} of get_data are not supported '
                f'by the {self.__class__.__name__} client.'
            )

        metadata = await self.get_metadata()
        total = metadata.item_count

        col = self._database[_COLLECTION_NAME]
        sort_dir = DESCENDING if desc else ASCENDING
        cursor = (
            col.find({'storage_name': self._storage_name}, projection={'_id': 0, 'data': 1})
            .sort('order_no', sort_dir)
            .skip(offset)
        )
        if limit is not None:
            cursor = cursor.limit(limit)

        items: list[dict[str, Any]] = []
        async for doc in cursor:
            item = doc['data']
            if skip_empty and not item:
                continue
            items.append(item)

        await self._update_metadata(update_accessed_at=True)

        return DatasetItemsListPage(
            count=len(items),
            offset=offset,
            limit=limit or (total - offset),
            total=total,
            desc=desc,
            items=items,
        )

    @override
    async def iterate_items(
        self,
        *,
        offset: int = 0,
        limit: int | None = None,
        clean: bool = False,
        desc: bool = False,
        fields: list[str] | None = None,
        omit: list[str] | None = None,
        unwind: list[str] | None = None,
        skip_empty: bool = False,
        skip_hidden: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        unsupported_args: dict[str, Any] = {
            'clean': clean,
            'fields': fields,
            'omit': omit,
            'unwind': unwind,
            'skip_hidden': skip_hidden,
        }
        unsupported = {k: v for k, v in unsupported_args.items() if v not in (False, None)}
        if unsupported:
            logger.warning(
                f'The arguments {list(unsupported.keys())} of iterate_items are not supported '
                f'by the {self.__class__.__name__} client.'
            )

        col = self._database[_COLLECTION_NAME]
        sort_dir = DESCENDING if desc else ASCENDING
        cursor = (
            col.find({'storage_name': self._storage_name}, projection={'_id': 0, 'data': 1})
            .sort('order_no', sort_dir)
            .skip(offset)
            .batch_size(100)
        )
        if limit is not None:
            cursor = cursor.limit(limit)

        async for doc in cursor:
            item = doc['data']
            if skip_empty and not item:
                continue
            yield item

        await self._update_metadata(update_accessed_at=True)

    @override
    def _specific_update_metadata(
        self,
        *,
        new_item_count: int | None = None,
        delta_item_count: int | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if new_item_count is not None:
            result['item_count'] = new_item_count
        # delta_item_count is handled via $inc in push_data directly
        return result
