from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from crawlee._utils.crypto import crypto_random_object_id

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorDatabase
    from typing_extensions import NotRequired, Self

    from crawlee.storage_clients.models import DatasetMetadata, KeyValueStoreMetadata, RequestQueueMetadata


logger = getLogger(__name__)


class MetadataUpdateParams(TypedDict, total=False):
    """Parameters for updating metadata."""

    update_accessed_at: NotRequired[bool]
    update_modified_at: NotRequired[bool]


class MongoDBClientMixin:
    """Mixin class for MongoDB clients.

    This mixin provides common MongoDB operations and basic methods for MongoDB storage clients.
    """

    _DEFAULT_NAME = 'default'
    """Default storage name when none provided."""

    _STORAGE_TYPE: ClassVar[str]
    """Storage type identifier used in the metadata collection."""

    _CLIENT_TYPE: ClassVar[str]
    """Human-readable client type for error messages."""

    _METADATA_COLLECTION = '_metadata'
    """Name of the metadata collection in the database."""

    def __init__(self, storage_name: str, storage_id: str, database: AsyncIOMotorDatabase) -> None:
        self._storage_name = storage_name
        self._storage_id = storage_id
        self._database = database

    @property
    def database(self) -> AsyncIOMotorDatabase:
        """Return the MongoDB database instance."""
        return self._database

    @classmethod
    async def _open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        database: AsyncIOMotorDatabase,
        metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata],
        extra_metadata_fields: dict[str, Any],
        instance_kwargs: dict[str, Any],
    ) -> Self:
        """Open or create a new MongoDB storage client.

        Args:
            id: The ID of the storage. If not provided, a random ID will be generated.
            name: The name of the storage for named (global scope) storages.
            alias: The alias of the storage for unnamed (run scope) storages.
            database: MongoDB database instance.
            metadata_model: Pydantic model for metadata validation.
            extra_metadata_fields: Storage-specific metadata fields.
            instance_kwargs: Additional arguments for the client constructor.

        Returns:
            An instance for the opened or created storage client.
        """
        internal_name = name or alias or cls._DEFAULT_NAME
        metadata_col = database[cls._METADATA_COLLECTION]
        storage_id: str | None = None
        storage_name: str | None = None

        if id:
            # Look up by ID
            doc = await metadata_col.find_one({'storage_type': cls._STORAGE_TYPE, 'storage_id': id})
            if doc is None:
                raise ValueError(f'{cls._CLIENT_TYPE} with ID "{id}" does not exist.')
            storage_id = doc['storage_id']
            storage_name = doc['storage_name']
        else:
            # Look up by name
            doc = await metadata_col.find_one({'storage_type': cls._STORAGE_TYPE, 'storage_name': internal_name})
            if doc is not None:
                storage_id = doc['storage_id']
                storage_name = doc['storage_name']

        if storage_name and storage_id:
            # Open existing storage
            client = cls(storage_name=storage_name, storage_id=storage_id, database=database, **instance_kwargs)
            await client._update_metadata(update_accessed_at=True)
        else:
            # Create new storage
            now = datetime.now(timezone.utc)
            new_id = crypto_random_object_id()
            metadata = metadata_model(
                id=new_id,
                name=name,
                created_at=now,
                accessed_at=now,
                modified_at=now,
                **extra_metadata_fields,
            )
            metadata_dict = metadata.model_dump()
            metadata_dict['storage_type'] = cls._STORAGE_TYPE
            metadata_dict['storage_name'] = internal_name
            metadata_dict['storage_id'] = new_id

            try:
                await metadata_col.insert_one(metadata_dict)
            except Exception:
                # Race condition: another client may have created the storage concurrently.
                doc = await metadata_col.find_one({
                    'storage_type': cls._STORAGE_TYPE,
                    'storage_name': internal_name,
                })
                if doc is None:
                    raise
                new_id = doc['storage_id']

            client = cls(storage_name=internal_name, storage_id=new_id, database=database, **instance_kwargs)

        # Ensure indexes exist
        await client._ensure_indexes()
        return client

    async def _ensure_indexes(self) -> None:
        """Create indexes required by the storage client. Override in subclasses."""

    async def _get_metadata_doc(self) -> dict[str, Any]:
        """Retrieve the raw metadata document from MongoDB."""
        metadata_col = self._database[self._METADATA_COLLECTION]
        doc = await metadata_col.find_one({
            'storage_type': self._STORAGE_TYPE,
            'storage_name': self._storage_name,
        })
        if doc is None:
            raise ValueError(f'{self._CLIENT_TYPE} with name "{self._storage_name}" does not exist.')
        return doc

    async def _get_metadata(
        self,
        metadata_model: type[DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata],
    ) -> DatasetMetadata | KeyValueStoreMetadata | RequestQueueMetadata:
        """Retrieve client metadata as a validated Pydantic model."""
        doc = await self._get_metadata_doc()
        await self._update_metadata(update_accessed_at=True)
        return metadata_model.model_validate(doc)

    async def _update_metadata(
        self,
        *,
        update_accessed_at: bool = False,
        update_modified_at: bool = False,
        **kwargs: Any,
    ) -> None:
        """Update storage metadata combining common and specific fields.

        Args:
            update_accessed_at: Whether to update accessed_at timestamp.
            update_modified_at: Whether to update modified_at timestamp.
            **kwargs: Additional arguments for _specific_update_metadata.
        """
        update_doc: dict[str, Any] = {}
        now = datetime.now(timezone.utc)

        if update_accessed_at:
            update_doc['accessed_at'] = now
        if update_modified_at:
            update_doc['modified_at'] = now

        specific = self._specific_update_metadata(**kwargs)
        update_doc.update(specific)

        if update_doc:
            metadata_col = self._database[self._METADATA_COLLECTION]
            await metadata_col.update_one(
                {'storage_type': self._STORAGE_TYPE, 'storage_name': self._storage_name},
                {'$set': update_doc},
            )

    def _specific_update_metadata(self, **_kwargs: Any) -> dict[str, Any]:
        """Return storage-specific metadata updates as a dict. Override in subclasses."""
        return {}

    async def _drop(self, *, collection_name: str, extra_filter: dict[str, Any] | None = None) -> None:
        """Drop all data and metadata for this storage instance.

        Args:
            collection_name: The data collection to delete from.
            extra_filter: Optional additional filter for the data collection delete.
        """
        data_filter = {'storage_name': self._storage_name}
        if extra_filter:
            data_filter.update(extra_filter)

        await self._database[collection_name].delete_many(data_filter)
        await self._database[self._METADATA_COLLECTION].delete_one({
            'storage_type': self._STORAGE_TYPE,
            'storage_name': self._storage_name,
        })

    async def _purge(
        self,
        *,
        collection_name: str,
        metadata_kwargs: MetadataUpdateParams,
        extra_filter: dict[str, Any] | None = None,
    ) -> None:
        """Purge all data but keep the metadata for this storage instance.

        Args:
            collection_name: The data collection to delete from.
            metadata_kwargs: Metadata update parameters.
            extra_filter: Optional additional filter for the data collection delete.
        """
        data_filter = {'storage_name': self._storage_name}
        if extra_filter:
            data_filter.update(extra_filter)

        await self._database[collection_name].delete_many(data_filter)
        await self._update_metadata(**metadata_kwargs)
