from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import BulkWriteError
from typing_extensions import NotRequired, override

from crawlee import Request
from crawlee._utils.crypto import crypto_random_object_id
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

from ._client_mixin import MetadataUpdateParams, MongoDBClientMixin

if TYPE_CHECKING:
    from collections.abc import Sequence

    from motor.motor_asyncio import AsyncIOMotorDatabase

logger = getLogger(__name__)

_COLLECTION_NAME = 'rq_requests'


class _QueueMetadataUpdateParams(MetadataUpdateParams):
    """Parameters for updating queue metadata."""

    new_handled_request_count: NotRequired[int]
    new_pending_request_count: NotRequired[int]
    new_total_request_count: NotRequired[int]
    delta_handled_request_count: NotRequired[int]
    delta_pending_request_count: NotRequired[int]
    delta_total_request_count: NotRequired[int]


class MongoDBRequestQueueClient(RequestQueueClient, MongoDBClientMixin):
    """MongoDB implementation of the request queue client.

    This client persists requests to MongoDB using a single shared collection with FIFO ordering.
    Requests are stored as documents partitioned by ``storage_name`` with a unique compound index
    on ``(storage_name, unique_key)`` for deduplication.

    The request queue data is stored in the ``rq_requests`` collection with the following document structure::

        {
            "storage_name": str,
            "unique_key": str,
            "data": str,          # JSON-serialized Request
            "is_handled": bool,
            "sequence_number": int,
            "blocked_by": str | None,
            "blocked_until": datetime | None,
        }

    Deduplication is achieved through MongoDB's unique index. The ``find_one_and_update`` atomic operation
    is used for safe concurrent fetching of the next request.
    """

    _STORAGE_TYPE = 'request_queue'
    _CLIENT_TYPE = 'Request queue'

    _MAX_BATCH_FETCH_SIZE = 10
    """Maximum number of requests to fetch in a single batch operation."""

    _BLOCK_SECONDS = 300
    """Time in seconds to block a fetched request before it can be auto-reclaimed."""

    _RECLAIM_INTERVAL = timedelta(seconds=30)
    """Interval to check for stale requests to reclaim."""

    def __init__(
        self,
        storage_name: str,
        storage_id: str,
        database: AsyncIOMotorDatabase,
    ) -> None:
        """Initialize a new instance.

        Preferably use the ``MongoDBRequestQueueClient.open`` class method to create a new instance.
        """
        super().__init__(storage_name=storage_name, storage_id=storage_id, database=database)

        self._pending_fetch_cache: deque[Request] = deque()
        self.client_key = crypto_random_object_id(length=32)[:32]
        self._sequence_counter = 0
        self._forefront_counter = 0
        self._next_reclaim_stale: datetime | None = None

    @classmethod
    async def open(
        cls,
        *,
        id: str | None,
        name: str | None,
        alias: str | None,
        database: AsyncIOMotorDatabase,
    ) -> MongoDBRequestQueueClient:
        """Open or create a new MongoDB request queue client.

        Args:
            id: The ID of the request queue.
            name: The name of the request queue for named (global scope) storages.
            alias: The alias of the request queue for unnamed (run scope) storages.
            database: MongoDB database instance.

        Returns:
            An instance for the opened or created storage client.
        """
        return await cls._open(
            id=id,
            name=name,
            alias=alias,
            database=database,
            metadata_model=RequestQueueMetadata,
            extra_metadata_fields={
                'had_multiple_clients': False,
                'handled_request_count': 0,
                'pending_request_count': 0,
                'total_request_count': 0,
            },
            instance_kwargs={},
        )

    @override
    async def _ensure_indexes(self) -> None:
        col = self._database[_COLLECTION_NAME]
        await col.create_index(
            [('storage_name', ASCENDING), ('unique_key', ASCENDING)],
            unique=True,
        )
        await col.create_index(
            [('storage_name', ASCENDING), ('is_handled', ASCENDING), ('blocked_until', ASCENDING),
             ('sequence_number', ASCENDING)],
        )

    async def _get_next_sequence(self, *, forefront: bool) -> int:
        """Get the next sequence number for a new request."""
        if forefront:
            self._forefront_counter -= 1
            return self._forefront_counter
        self._sequence_counter += 1
        return self._sequence_counter

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        return await self._get_metadata(RequestQueueMetadata)

    @override
    async def drop(self) -> None:
        await self._drop(collection_name=_COLLECTION_NAME)

    @override
    async def purge(self) -> None:
        await self._purge(
            collection_name=_COLLECTION_NAME,
            metadata_kwargs=_QueueMetadataUpdateParams(
                update_accessed_at=True,
                update_modified_at=True,
                new_pending_request_count=0,
                new_handled_request_count=0,
                new_total_request_count=0,
            ),
        )
        # Also reset metadata counts
        metadata_col = self._database[self._METADATA_COLLECTION]
        await metadata_col.update_one(
            {'storage_type': self._STORAGE_TYPE, 'storage_name': self._storage_name},
            {'$set': {
                'pending_request_count': 0,
                'handled_request_count': 0,
                'total_request_count': 0,
            }},
        )
        self._pending_fetch_cache.clear()
        self._sequence_counter = 0
        self._forefront_counter = 0

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        col = self._database[_COLLECTION_NAME]
        processed_requests: list[ProcessedRequest] = []

        # Deduplicate input by unique_key (keep first occurrence)
        requests_by_key: dict[str, Request] = {}
        for req in requests:
            if req.unique_key not in requests_by_key:
                requests_by_key[req.unique_key] = req

        unique_keys = list(requests_by_key.keys())

        # Check which requests already exist
        existing_docs = col.find(
            {'storage_name': self._storage_name, 'unique_key': {'$in': unique_keys}},
            projection={'unique_key': 1, 'is_handled': 1, '_id': 0},
        )
        existing_map: dict[str, bool] = {}
        async for doc in existing_docs:
            existing_map[doc['unique_key']] = doc['is_handled']

        new_docs = []
        delta_pending = 0
        delta_total = 0

        for unique_key, request in requests_by_key.items():
            if unique_key in existing_map:
                processed_requests.append(
                    ProcessedRequest(
                        unique_key=unique_key,
                        was_already_present=True,
                        was_already_handled=existing_map[unique_key],
                    )
                )
            else:
                seq = await self._get_next_sequence(forefront=forefront)
                new_docs.append({
                    'storage_name': self._storage_name,
                    'unique_key': unique_key,
                    'data': request.model_dump_json(),
                    'is_handled': False,
                    'sequence_number': seq,
                    'blocked_by': None,
                    'blocked_until': None,
                })
                processed_requests.append(
                    ProcessedRequest(
                        unique_key=unique_key,
                        was_already_present=False,
                        was_already_handled=False,
                    )
                )
                delta_pending += 1
                delta_total += 1

        if new_docs:
            try:
                await col.insert_many(new_docs, ordered=False)
            except BulkWriteError as bwe:
                # Some inserts may fail due to duplicate key errors in concurrent scenarios.
                n_inserted = bwe.details.get('nInserted', 0)
                delta_pending = n_inserted
                delta_total = n_inserted

        # Update metadata counts
        if delta_pending > 0 or delta_total > 0:
            metadata_col = self._database[self._METADATA_COLLECTION]
            await metadata_col.update_one(
                {'storage_type': self._STORAGE_TYPE, 'storage_name': self._storage_name},
                {'$inc': {
                    'pending_request_count': delta_pending,
                    'total_request_count': delta_total,
                }},
            )
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

        return AddRequestsResponse(
            processed_requests=processed_requests,
            unprocessed_requests=[],
        )

    @override
    async def fetch_next_request(self) -> Request | None:
        if self._pending_fetch_cache:
            return self._pending_fetch_cache.popleft()

        now = datetime.now(timezone.utc)
        blocked_until = now + timedelta(seconds=self._BLOCK_SECONDS)

        col = self._database[_COLLECTION_NAME]

        # Atomically find a pending, non-blocked request and mark it as blocked
        doc = await col.find_one_and_update(
            {
                'storage_name': self._storage_name,
                'is_handled': False,
                '$or': [
                    {'blocked_until': None},
                    {'blocked_until': {'$lte': now}},
                ],
            },
            {
                '$set': {
                    'blocked_by': self.client_key,
                    'blocked_until': blocked_until,
                },
            },
            sort=[('sequence_number', ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

        await self._update_metadata(update_accessed_at=True)

        if doc is None:
            return None

        return Request.model_validate_json(doc['data'])

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        col = self._database[_COLLECTION_NAME]
        doc = await col.find_one(
            {'storage_name': self._storage_name, 'unique_key': unique_key},
            projection={'data': 1, '_id': 0},
        )
        if doc is None:
            return None
        return Request.model_validate_json(doc['data'])

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        col = self._database[_COLLECTION_NAME]

        # Check if the request is in progress (blocked by a client)
        doc = await col.find_one({
            'storage_name': self._storage_name,
            'unique_key': request.unique_key,
            'blocked_by': {'$ne': None},
        })
        if doc is None:
            logger.warning(f'Marking request {request.unique_key} as handled that is not in progress.')
            return None

        if request.handled_at is None:
            request.handled_at = datetime.now(timezone.utc)

        await col.update_one(
            {'storage_name': self._storage_name, 'unique_key': request.unique_key},
            {'$set': {
                'is_handled': True,
                'blocked_by': None,
                'blocked_until': None,
                'data': request.model_dump_json(),
            }},
        )

        # Update metadata counts
        metadata_col = self._database[self._METADATA_COLLECTION]
        await metadata_col.update_one(
            {'storage_type': self._STORAGE_TYPE, 'storage_name': self._storage_name},
            {'$inc': {
                'handled_request_count': 1,
                'pending_request_count': -1,
            }},
        )
        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

        return ProcessedRequest(
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=True,
        )

    @override
    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        col = self._database[_COLLECTION_NAME]

        doc = await col.find_one({
            'storage_name': self._storage_name,
            'unique_key': request.unique_key,
            'blocked_by': {'$ne': None},
        })
        if doc is None:
            logger.info(f'Reclaiming request {request.unique_key} that is not in progress.')
            return None

        if forefront:
            seq = await self._get_next_sequence(forefront=True)
            blocked_until = datetime.now(timezone.utc) + timedelta(seconds=self._BLOCK_SECONDS)
            await col.update_one(
                {'storage_name': self._storage_name, 'unique_key': request.unique_key},
                {'$set': {
                    'sequence_number': seq,
                    'blocked_by': self.client_key,
                    'blocked_until': blocked_until,
                    'data': request.model_dump_json(),
                }},
            )
            self._pending_fetch_cache.appendleft(request)
        else:
            await col.update_one(
                {'storage_name': self._storage_name, 'unique_key': request.unique_key},
                {'$set': {
                    'blocked_by': None,
                    'blocked_until': None,
                    'data': request.model_dump_json(),
                }},
            )

        await self._update_metadata(update_accessed_at=True, update_modified_at=True)

        return ProcessedRequest(
            unique_key=request.unique_key,
            was_already_present=True,
            was_already_handled=False,
        )

    @override
    async def is_empty(self) -> bool:
        if self._pending_fetch_cache:
            return False

        # Periodically reclaim stale requests
        now = datetime.now(timezone.utc)
        if self._next_reclaim_stale is None or now >= self._next_reclaim_stale:
            await self._reclaim_stale_requests()
            self._next_reclaim_stale = now + self._RECLAIM_INTERVAL

        metadata = await self.get_metadata()
        return metadata.pending_request_count == 0

    async def _reclaim_stale_requests(self) -> None:
        """Reclaim requests that have been blocked for too long."""
        col = self._database[_COLLECTION_NAME]
        now = datetime.now(timezone.utc)

        # Unblock all stale requests
        await col.update_many(
            {
                'storage_name': self._storage_name,
                'is_handled': False,
                'blocked_until': {'$lte': now},
                'blocked_by': {'$ne': None},
            },
            {'$set': {'blocked_by': None, 'blocked_until': None}},
        )

    @override
    def _specific_update_metadata(
        self,
        *,
        new_handled_request_count: int | None = None,
        new_pending_request_count: int | None = None,
        new_total_request_count: int | None = None,
        delta_handled_request_count: int | None = None,
        delta_pending_request_count: int | None = None,
        delta_total_request_count: int | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if new_handled_request_count is not None:
            result['handled_request_count'] = new_handled_request_count
        if new_pending_request_count is not None:
            result['pending_request_count'] = new_pending_request_count
        if new_total_request_count is not None:
            result['total_request_count'] = new_total_request_count
        # delta counts are handled via $inc directly in the calling methods
        return result
