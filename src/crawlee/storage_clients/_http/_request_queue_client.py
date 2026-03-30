from __future__ import annotations

from http import HTTPStatus
from logging import getLogger
from typing import TYPE_CHECKING

from typing_extensions import override

from crawlee import Request
from crawlee.storage_clients._base import RequestQueueClient
from crawlee.storage_clients.models import AddRequestsResponse, ProcessedRequest, RequestQueueMetadata

if TYPE_CHECKING:
    from collections.abc import Sequence

    from httpx import AsyncClient

logger = getLogger(__name__)


class HttpRequestQueueClient(RequestQueueClient):
    """HTTP implementation of the request queue client.

    This client communicates with a remote server via HTTP to manage a request queue.
    It delegates all queue operations to the remote server, making it suitable for distributed
    crawling setups and system integration scenarios.
    """

    def __init__(
        self,
        *,
        http_client: AsyncClient,
        api_url: str,
        metadata: RequestQueueMetadata,
    ) -> None:
        self._http_client = http_client
        self._api_url = api_url.rstrip('/')
        self._metadata = metadata

    @property
    def _queue_url(self) -> str:
        return f'{self._api_url}/request-queues/{self._metadata.id}'

    @override
    async def get_metadata(self) -> RequestQueueMetadata:
        response = await self._http_client.get(self._queue_url)
        response.raise_for_status()
        self._metadata = RequestQueueMetadata.model_validate(response.json())
        return self._metadata

    @override
    async def drop(self) -> None:
        response = await self._http_client.delete(self._queue_url)
        response.raise_for_status()

    @override
    async def purge(self) -> None:
        response = await self._http_client.post(f'{self._queue_url}/purge')
        response.raise_for_status()

    @override
    async def add_batch_of_requests(
        self,
        requests: Sequence[Request],
        *,
        forefront: bool = False,
    ) -> AddRequestsResponse:
        payload = {
            'requests': [req.model_dump(by_alias=True) for req in requests],
            'forefront': forefront,
        }

        response = await self._http_client.post(
            f'{self._queue_url}/requests/batch',
            json=payload,
        )
        response.raise_for_status()
        return AddRequestsResponse.model_validate(response.json())

    @override
    async def get_request(self, unique_key: str) -> Request | None:
        response = await self._http_client.get(
            f'{self._queue_url}/requests',
            params={'uniqueKey': unique_key},
        )

        if response.status_code == HTTPStatus.NOT_FOUND:
            return None

        response.raise_for_status()
        return Request.model_validate(response.json())

    @override
    async def fetch_next_request(self) -> Request | None:
        response = await self._http_client.post(
            f'{self._queue_url}/requests/fetch',
        )

        if response.status_code == HTTPStatus.NO_CONTENT:
            return None

        response.raise_for_status()
        data = response.json()

        if data is None:
            return None

        return Request.model_validate(data)

    @override
    async def mark_request_as_handled(self, request: Request) -> ProcessedRequest | None:
        response = await self._http_client.post(
            f'{self._queue_url}/requests/handled',
            json=request.model_dump(by_alias=True),
        )

        if response.status_code == HTTPStatus.NO_CONTENT:
            return None

        response.raise_for_status()
        return ProcessedRequest.model_validate(response.json())

    @override
    async def reclaim_request(
        self,
        request: Request,
        *,
        forefront: bool = False,
    ) -> ProcessedRequest | None:
        response = await self._http_client.post(
            f'{self._queue_url}/requests/reclaim',
            json=request.model_dump(by_alias=True),
            params={'forefront': forefront},
        )

        if response.status_code == HTTPStatus.NO_CONTENT:
            return None

        response.raise_for_status()
        return ProcessedRequest.model_validate(response.json())

    @override
    async def is_empty(self) -> bool:
        response = await self._http_client.get(
            f'{self._queue_url}/head',
        )
        response.raise_for_status()
        data = response.json()
        return data.get('isEmpty', True)
