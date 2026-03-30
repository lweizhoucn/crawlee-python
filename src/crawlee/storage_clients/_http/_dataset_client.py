from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients.models import DatasetItemsListPage, DatasetMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from httpx import AsyncClient

logger = getLogger(__name__)


class HttpDatasetClient(DatasetClient):
    """HTTP implementation of the dataset client.

    This client communicates with a remote server via HTTP to store and retrieve dataset items.
    It delegates all storage operations to the remote server, making it suitable for distributed
    crawling setups and system integration scenarios.
    """

    def __init__(
        self,
        *,
        http_client: AsyncClient,
        api_url: str,
        metadata: DatasetMetadata,
    ) -> None:
        self._http_client = http_client
        self._api_url = api_url.rstrip('/')
        self._metadata = metadata

    @property
    def _dataset_url(self) -> str:
        return f'{self._api_url}/datasets/{self._metadata.id}'

    @override
    async def get_metadata(self) -> DatasetMetadata:
        response = await self._http_client.get(self._dataset_url)
        response.raise_for_status()
        self._metadata = DatasetMetadata.model_validate(response.json())
        return self._metadata

    @override
    async def drop(self) -> None:
        response = await self._http_client.delete(self._dataset_url)
        response.raise_for_status()

    @override
    async def purge(self) -> None:
        response = await self._http_client.post(f'{self._dataset_url}/purge')
        response.raise_for_status()

    @override
    async def push_data(self, data: list[dict[str, Any]] | dict[str, Any]) -> None:
        response = await self._http_client.post(
            f'{self._dataset_url}/items',
            json=data,
        )
        response.raise_for_status()

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
        params: dict[str, Any] = {
            'offset': offset,
            'desc': desc,
            'skipEmpty': skip_empty,
            'skipHidden': skip_hidden,
            'clean': clean,
        }
        if limit is not None:
            params['limit'] = limit
        if fields is not None:
            params['fields'] = ','.join(fields)
        if omit is not None:
            params['omit'] = ','.join(omit)
        if unwind is not None:
            params['unwind'] = ','.join(unwind)
        if flatten is not None:
            params['flatten'] = ','.join(flatten)
        if view is not None:
            params['view'] = view

        response = await self._http_client.get(
            f'{self._dataset_url}/items',
            params=params,
        )
        response.raise_for_status()
        return DatasetItemsListPage.model_validate(response.json())

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
        batch_size = 1000
        current_offset = offset
        items_yielded = 0

        while True:
            current_limit = batch_size
            if limit is not None:
                remaining = limit - items_yielded
                if remaining <= 0:
                    break
                current_limit = min(batch_size, remaining)

            page = await self.get_data(
                offset=current_offset,
                limit=current_limit,
                clean=clean,
                desc=desc,
                fields=fields,
                omit=omit,
                unwind=unwind,
                skip_empty=skip_empty,
                skip_hidden=skip_hidden,
            )

            if not page.items:
                break

            for item in page.items:
                yield item
                items_yielded += 1

            current_offset += len(page.items)

            if len(page.items) < current_limit:
                break
