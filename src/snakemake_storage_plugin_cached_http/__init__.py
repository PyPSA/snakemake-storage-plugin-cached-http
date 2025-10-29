# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import asyncio
import hashlib
import json
from logging import Logger
import shutil
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing_extensions import override
from urllib.parse import urlparse

import httpx
import platformdirs
from reretry import retry  # pyright: ignore[reportUnknownVariableType]
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_common.logging import get_logger
from snakemake_interface_common.plugin_registry.plugin import SettingsBase
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime
from snakemake_interface_storage_plugins.storage_object import StorageObjectRead
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)
from tqdm_loggable.auto import tqdm

from .cache import Cache
from .monkeypatch import is_zenodo_url  # noqa: F401 - applies monkeypatch on import

logger = get_logger()


class ReretryLoggerAdapter:
    """Adapter to make Snakemake's logger compatible with reretry's logging expectations."""

    _logger: Logger

    def __init__(self, snakemake_logger: Logger):
        self._logger = snakemake_logger

    def warning(self, msg: str, *args, **kwargs):  # pyright: ignore[reportUnknownParameterType, reportUnusedParameter, reportMissingParameterType]
        """
        Format message manually before passing to Snakemake logger.

        This is necessary because Snakemake's DefaultFormatter has a bug where it
        returns record["msg"] without calling interpolating the args. This causes
        literal "%s" to appear in log output instead of formatted values.
        """
        if args:
            # Pre-format the message with % operator
            msg = msg % args
        self._logger.warning(msg)


# Define settings for the Zenodo storage plugin
# NB: We derive from SettingsBase rather than StorageProviderSettingsBase to remove the
# unsupported max_requests_per_second option
@dataclass
class StorageProviderSettings(SettingsBase):
    cache: str = field(
        default_factory=lambda: platformdirs.user_cache_dir(
            "snakemake-pypsa-eur", ensure_exists=False
        ),
        metadata={
            "help": 'Cache directory for downloaded files (default: platform-dependent user cache dir). Set to "" to deactivate caching.',
            "env_var": True,
        },
    )
    skip_remote_checks: bool = field(
        default=False,
        metadata={
            "help": "Whether to skip metadata checking with remote server (default: False, ie. do check).",
            "env_var": True,
        },
    )
    max_concurrent_downloads: int | None = field(
        default=3,
        metadata={
            "help": "Maximum number of concurrent downloads.",
            "env_var": False,
        },
    )


@dataclass
class ZenodoFileMetadata:
    """Metadata for a file in a Zenodo record."""

    checksum: str | None
    size: int


class WrongChecksum(Exception):
    observed: str
    expected: str

    def __init__(self, observed: str, expected: str):
        self.observed = observed
        self.expected = expected
        super().__init__(f"Checksum mismatch: expected {expected}, got {observed}")


retry_decorator = retry(
    exceptions=(  # pyright: ignore[reportArgumentType]
        httpx.HTTPError,
        TimeoutError,
        OSError,
        WrongChecksum,
        WorkflowError,
    ),
    tries=5,
    delay=3,
    backoff=2,
    logger=ReretryLoggerAdapter(get_logger()),  # pyright: ignore[reportArgumentType]
)


# Implementation of storage provider
class StorageProvider(StorageProviderBase):
    settings: StorageProviderSettings
    cache: Cache | None

    def __post_init__(self):
        super().__post_init__()

        # Set up cache
        self.cache = (
            Cache(cache_dir=Path(self.settings.cache)) if self.settings.cache else None
        )

        # Initialize shared client for bounding connections and pipelining
        self._client: httpx.AsyncClient | None = None
        self._client_refcount: int = 0

        # Cache for record metadata to avoid repeated API calls
        self._record_cache: dict[str, dict[str, ZenodoFileMetadata]] = {}

    @override
    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    @override
    def rate_limiter_key(self, query: str, operation: Operation) -> str:
        raise NotImplementedError()

    @override
    def default_max_requests_per_second(self) -> float:
        raise NotImplementedError()

    @override
    @classmethod
    def example_queries(cls) -> list[ExampleQuery]:
        """Return an example query with description for this storage provider."""
        return [
            ExampleQuery(
                query="https://zenodo.org/records/17249457/files/ARDECO-SNPTD.2021.table.csv",
                description="A Zenodo file URL (currently the only supported HTTP source)",
                type=QueryType.INPUT,
            )
        ]

    @override
    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Only handle zenodo.org URLs"""
        if is_zenodo_url(query):
            return StorageQueryValidationResult(query=query, valid=True)

        return StorageQueryValidationResult(
            query=query,
            valid=False,
            reason="Not a Zenodo URL (only zenodo.org URLs are handled by this plugin)",
        )

    @override
    @classmethod
    def get_storage_object_cls(cls):
        return StorageObject

    @asynccontextmanager
    async def client(self):
        """
        Reentrant async context manager for httpx.AsyncClient.

        Creates a client on first entry and reuses it for nested calls.
        The client is closed only when all context managers have exited.

        Usage:
            async with provider.client() as client:
                response = await client.get(url)
                ...
        """
        self._client_refcount += 1

        # Create client on first entry
        if self._client is None:
            max_concurrent_downloads = self.settings.max_concurrent_downloads
            limits = httpx.Limits(
                max_keepalive_connections=max_concurrent_downloads,
                max_connections=max_concurrent_downloads,
            )
            timeout = httpx.Timeout(60, pool=None)

            self._client = httpx.AsyncClient(
                follow_redirects=True, limits=limits, timeout=timeout
            )

        try:
            yield self._client
        finally:
            self._client_refcount -= 1
            if self._client_refcount == 0:
                await self._client.aclose()
                self._client = None

    def _get_rate_limit_wait_time(self, headers: httpx.Headers) -> float | None:
        """
        Calculate wait time based on rate limit headers.

        Returns:
            float | None: Wait time in seconds if rate limited, None otherwise
        """
        remaining = int(headers.get("X-RateLimit-Remaining", 100))
        reset_time = int(headers.get("X-RateLimit-Reset", 0))

        if remaining >= 1:
            return None

        wait_seconds = max(0, reset_time - time.time() + 1)
        return wait_seconds

    @asynccontextmanager
    async def httpr(self, method: str, url: str):
        """
        HTTP request wrapper with rate limiting and exception logging.

        Args:
            method: HTTP method (e.g., "get", "post")
            url: URL to request

        Yields:
            httpx.Response object
        """
        try:
            async with self.client() as client, client.stream(method, url) as response:
                wait_time = self._get_rate_limit_wait_time(response.headers)
                if wait_time is not None:
                    logger.info(
                        f"Zenodo rate limit exceeded. Waiting {wait_time:.0f}s until reset..."
                    )
                    await asyncio.sleep(wait_time)
                    raise httpx.HTTPError("Rate limit exceeded, retrying after wait")

                yield response
        except Exception as e:
            logger.warning(f"{type(e).__name__} while {method}'ing {url}")
            raise

    @retry_decorator
    async def get_metadata(
        self, record_id: str, netloc: str
    ) -> dict[str, ZenodoFileMetadata]:
        """
        Retrieve and cache file metadata for a Zenodo record.

        Args:
            record_id: The Zenodo record ID
            netloc: Network location (e.g., "zenodo.org")

        Returns:
            Dictionary mapping filename to ZenodoFileMetadata
        """
        # Check cache first
        if record_id in self._record_cache:
            return self._record_cache[record_id]

        # Fetch from API
        api_url = f"https://{netloc}/api/records/{record_id}"

        async with self.httpr("get", api_url) as response:
            if response.status_code != 200:
                raise WorkflowError(
                    f"Failed to fetch Zenodo record metadata: HTTP {response.status_code} ({api_url})"
                )

            # Read the full response body
            content = await response.aread()
            data = json.loads(content)

        # Parse files array and build metadata dict
        metadata: dict[str, ZenodoFileMetadata] = {}
        files = data.get("files", [])
        for file_info in files:
            filename: str | None = file_info.get("key")
            checksum: str | None = file_info.get("checksum")
            size: int = file_info.get("size", 0)

            if not filename:
                continue

            metadata[filename] = ZenodoFileMetadata(checksum=checksum, size=size)

        # Store in cache
        self._record_cache[record_id] = metadata

        return metadata


# Implementation of storage object
class StorageObject(StorageObjectRead):
    provider: StorageProvider  # pyright: ignore[reportIncompatibleVariableOverride]
    record_id: str
    filename: str
    netloc: str

    def __post_init__(self):
        super().__post_init__()

        # Parse URL to extract record ID and filename
        # URL format: https://zenodo.org/records/{record_id}/files/{filename}
        parsed = urlparse(str(self.query))
        _records, record_id, _files, filename = parsed.path.strip("/").split(
            "/", maxsplit=3
        )

        if _records != "records" or _files != "files":
            raise WorkflowError(
                f"Invalid Zenodo URL format: {self.query}. "
                f"Expected format: https://zenodo.org/records/{{record_id}}/files/{{filename}}"
            )

        self.record_id = record_id
        self.filename = filename
        self.netloc = parsed.netloc

    @override
    def local_suffix(self) -> str:
        """Return the local suffix for this object (used by parent class)."""
        parsed = urlparse(str(self.query))
        return f"{parsed.netloc}{parsed.path}"

    @override
    def get_inventory_parent(self) -> str | None:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    @override
    async def managed_exists(self) -> bool:
        if self.provider.settings.skip_remote_checks:
            return True

        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                return True

        metadata = await self.provider.get_metadata(self.record_id, self.netloc)
        return self.filename in metadata

    @override
    async def managed_mtime(self) -> float:
        return 0

    @override
    async def managed_size(self) -> int:
        if self.provider.settings.skip_remote_checks:
            return 0

        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                return cached.stat().st_size

        metadata = await self.provider.get_metadata(self.record_id, self.netloc)
        return metadata[self.filename].size if self.filename in metadata else 0

    @override
    async def inventory(self, cache: IOCacheStorageInterface) -> None:
        """
        Gather file metadata (existence, size) from cache or remote.
        Checks local cache first, then queries remote if needed.
        """
        key = self.cache_key()
        if key in cache.exists_in_storage:
            # Already inventorized
            return

        if self.provider.settings.skip_remote_checks:
            cache.exists_in_storage[key] = True
            cache.mtime[key] = Mtime(storage=0)
            cache.size[key] = 0
            return

        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                cache.exists_in_storage[key] = True
                cache.mtime[key] = Mtime(storage=0)
                cache.size[key] = cached.stat().st_size
                return

        metadata = await self.provider.get_metadata(self.record_id, self.netloc)
        exists = self.filename in metadata
        cache.exists_in_storage[key] = exists
        cache.mtime[key] = Mtime(storage=0)
        cache.size[key] = metadata[self.filename].size if exists else 0

    @override
    def cleanup(self):
        """Nothing to cleanup"""
        pass

    @override
    def exists(self) -> bool:
        raise NotImplementedError()

    @override
    def size(self) -> int:
        raise NotImplementedError()

    @override
    def mtime(self) -> float:
        raise NotImplementedError()

    @override
    def retrieve_object(self) -> None:
        raise NotImplementedError()

    async def verify_checksum(self, path: Path) -> None:
        """
        Verify `path` against checksum provided by zenodo metadata.

        Raises:
            WrongChecksum
        """
        # Get cached or fetch record metadata
        metadata = await self.provider.get_metadata(self.record_id, self.netloc)
        if self.filename not in metadata:
            raise WorkflowError(
                f"File {self.filename} not found in Zenodo record {self.record_id}"
            )

        checksum = metadata[self.filename].checksum
        if checksum is None:
            return

        digest, checksum_expected = checksum.split(":", maxsplit=1)

        def compute_hash(path: Path = path, digest: str = digest):
            with open(path, "rb") as f:
                return hashlib.file_digest(f, digest).hexdigest().lower()

        # Compute checksum asynchronously (hashlib releases GIL)
        # checksum_observed = await asyncio.to_thread(compute_hash)
        checksum_observed = compute_hash(path, digest)

        if checksum_expected != checksum_observed:
            raise WrongChecksum(observed=checksum_observed, expected=checksum_expected)

    @retry_decorator
    async def managed_retrieve(self):
        """Async download with concurrency control and progress bar"""
        local_path = self.local_path()
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # If already in cache, just copy
        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                logger.info(
                    f"Retrieved {self.filename} of zenodo record {self.record_id} from cache"
                )
                shutil.copy2(cached, local_path)
                return

        try:
            # Download from Zenodo using a get request, rate limit errors are detected and
            # raise WorkflowError to trigger a retry
            async with self.provider.httpr("get", str(self.query)) as response:
                if response.status_code != 200:
                    raise WorkflowError(
                        f"Failed to download from Zenodo: HTTP {response.status_code} ({self.query})"
                    )

                total_size = int(response.headers.get("content-length", 0))

                # Download to local path with progress bar
                with local_path.open(mode="wb") as f:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc=self.filename,
                        position=None,
                        leave=True,
                    ) as pbar:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))

            await self.verify_checksum(local_path)

            # Copy to cache after successful verification
            if self.provider.cache:
                self.provider.cache.put(str(self.query), local_path)

        except:
            if local_path.exists():
                local_path.unlink()
            raise
