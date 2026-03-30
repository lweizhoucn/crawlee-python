from __future__ import annotations

from logging import getLogger
from typing import Any

import httpx
from typing_extensions import override

from crawlee._utils.docs import docs_group
from crawlee.configuration import Configuration
from crawlee.storage_clients._base import StorageClient
from crawlee.storage_clients.models import DatasetMetadata, KeyValueStoreMetadata, RequestQueueMetadata

from ._dataset_client import HttpDatasetClient
from ._key_value_store_client import HttpKeyValueStoreClient
from ._request_queue_client import HttpRequestQueueClient

logger = getLogger(__name__)


@docs_group('Storage clients')
class HttpStorageClient(StorageClient):
    """HTTP implementation of the storage client.

    This storage client communicates with a remote server via HTTP to manage datasets,
    key-value stores, and request queues. It delegates all storage operations to the
    remote server, making it suitable for distributed crawling setups, system integration,
    and centralized data management.

    The HTTP storage client supports:
    - Reporting crawled data to a remote system (push_data)
    - Fetching tasks/requests from a remote task queue (fetch_next_request)
    - Reporting task status back to the remote system (mark_request_as_handled)

    Example usage::

        from crawlee import service_locator
        from crawlee.storage_clients import HttpStorageClient

        storage_client = HttpStorageClient(api_url='http://localhost:8000/api/v1')
        service_locator.set_storage_client(storage_client)
    """

    def __init__(
        self,
        *,
        api_url: str,
        api_token: str | None = None,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            api_url: The base URL of the remote API server.
            api_token: Optional authentication token for the API.
            timeout: HTTP request timeout in seconds.
            headers: Optional extra HTTP headers to include in all requests.
        """
        self._api_url = api_url.rstrip('/')
        self._api_token = api_token

        default_headers: dict[str, str] = {
            'Accept': 'application/json',
        }
        if api_token:
            default_headers['Authorization'] = f'Bearer {api_token}'
        if headers:
            default_headers.update(headers)

        self._http_client = httpx.AsyncClient(
            timeout=timeout,
            headers=default_headers,
        )

    async def close(self) -> None:
        """Close the underlying HTTP client and release resources."""
        await self._http_client.aclose()

    @override
    async def create_dataset_client(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        alias: str | None = None,
        configuration: Configuration | None = None,
    ) -> HttpDatasetClient:
        configuration = configuration or Configuration.get_global_configuration()

        params: dict[str, Any] = {}
        if id is not None:
            params['id'] = id
        if name is not None:
            params['name'] = name
        if alias is not None:
            params['alias'] = alias

        response = await self._http_client.post(
            f'{self._api_url}/datasets',
            json=params,
        )
        response.raise_for_status()
        metadata = DatasetMetadata.model_validate(response.json())

        client = HttpDatasetClient(
            http_client=self._http_client,
            api_url=self._api_url,
            metadata=metadata,
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
    ) -> HttpKeyValueStoreClient:
        configuration = configuration or Configuration.get_global_configuration()

        params: dict[str, Any] = {}
        if id is not None:
            params['id'] = id
        if name is not None:
            params['name'] = name
        if alias is not None:
            params['alias'] = alias

        response = await self._http_client.post(
            f'{self._api_url}/key-value-stores',
            json=params,
        )
        response.raise_for_status()
        metadata = KeyValueStoreMetadata.model_validate(response.json())

        client = HttpKeyValueStoreClient(
            http_client=self._http_client,
            api_url=self._api_url,
            metadata=metadata,
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
    ) -> HttpRequestQueueClient:
        configuration = configuration or Configuration.get_global_configuration()

        params: dict[str, Any] = {}
        if id is not None:
            params['id'] = id
        if name is not None:
            params['name'] = name
        if alias is not None:
            params['alias'] = alias

        response = await self._http_client.post(
            f'{self._api_url}/request-queues',
            json=params,
        )
        response.raise_for_status()
        metadata = RequestQueueMetadata.model_validate(response.json())

        client = HttpRequestQueueClient(
            http_client=self._http_client,
            api_url=self._api_url,
            metadata=metadata,
        )
        await self._purge_if_needed(client, configuration)
        return client
