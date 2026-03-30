from __future__ import annotations

import json
from http import HTTPStatus
from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee.storage_clients._base import KeyValueStoreClient
from crawlee.storage_clients.models import KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from httpx import AsyncClient

logger = getLogger(__name__)


class HttpKeyValueStoreClient(KeyValueStoreClient):
    """HTTP implementation of the key-value store client.

    This client communicates with a remote server via HTTP to store and retrieve key-value pairs.
    It delegates all storage operations to the remote server, making it suitable for distributed
    crawling setups and system integration scenarios.
    """

    def __init__(
        self,
        *,
        http_client: AsyncClient,
        api_url: str,
        metadata: KeyValueStoreMetadata,
    ) -> None:
        self._http_client = http_client
        self._api_url = api_url.rstrip('/')
        self._metadata = metadata

    @property
    def _store_url(self) -> str:
        return f'{self._api_url}/key-value-stores/{self._metadata.id}'

    @override
    async def get_metadata(self) -> KeyValueStoreMetadata:
        response = await self._http_client.get(self._store_url)
        response.raise_for_status()
        self._metadata = KeyValueStoreMetadata.model_validate(response.json())
        return self._metadata

    @override
    async def drop(self) -> None:
        response = await self._http_client.delete(self._store_url)
        response.raise_for_status()

    @override
    async def purge(self) -> None:
        response = await self._http_client.post(f'{self._store_url}/purge')
        response.raise_for_status()

    @override
    async def get_value(self, *, key: str) -> KeyValueStoreRecord | None:
        response = await self._http_client.get(
            f'{self._store_url}/records/{key}',
        )

        if response.status_code == HTTPStatus.NOT_FOUND:
            return None

        response.raise_for_status()

        content_type = response.headers.get('content-type', 'application/octet-stream')
        value = response.json() if 'application/json' in content_type else response.content

        return KeyValueStoreRecord(
            key=key,
            value=value,
            content_type=content_type,
            size=len(response.content),
        )

    @override
    async def set_value(self, *, key: str, value: Any, content_type: str | None = None) -> None:
        headers = {}
        if content_type:
            headers['content-type'] = content_type

        if isinstance(value, bytes):
            data = value
            headers.setdefault('content-type', 'application/octet-stream')
        elif isinstance(value, str):
            data = value.encode('utf-8')
            headers.setdefault('content-type', 'text/plain; charset=utf-8')
        else:
            data = json.dumps(value, ensure_ascii=False, default=str).encode('utf-8')
            headers.setdefault('content-type', 'application/json; charset=utf-8')

        response = await self._http_client.put(
            f'{self._store_url}/records/{key}',
            content=data,
            headers=headers,
        )
        response.raise_for_status()

    @override
    async def delete_value(self, *, key: str) -> None:
        response = await self._http_client.delete(
            f'{self._store_url}/records/{key}',
        )
        response.raise_for_status()

    @override
    async def iterate_keys(
        self,
        *,
        exclusive_start_key: str | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[KeyValueStoreRecordMetadata]:
        params: dict[str, Any] = {}
        if exclusive_start_key is not None:
            params['exclusiveStartKey'] = exclusive_start_key
        if limit is not None:
            params['limit'] = limit

        response = await self._http_client.get(
            f'{self._store_url}/keys',
            params=params,
        )
        response.raise_for_status()
        data = response.json()

        for item in data.get('items', []):
            yield KeyValueStoreRecordMetadata.model_validate(item)

    @override
    async def get_public_url(self, *, key: str) -> str:
        return f'{self._store_url}/records/{key}'

    @override
    async def record_exists(self, *, key: str) -> bool:
        response = await self._http_client.head(
            f'{self._store_url}/records/{key}',
        )
        return response.status_code == HTTPStatus.OK
