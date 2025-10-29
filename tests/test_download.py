# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Functional tests for downloading and checksumming from Zenodo."""

import json
import logging

import pytest

from snakemake_storage_plugin_cached_http import (
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
    WrongChecksum,
)

TEST_URL = "https://zenodo.org/records/16810901/files/attributed_ports.json"


@pytest.fixture
def storage_provider(tmp_path):
    """Create a StorageProvider instance for testing."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        cache=str(cache_dir),
        skip_remote_checks=False,
        max_concurrent_downloads=3,
    )

    logger = logging.getLogger("test")

    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=logger,
        settings=settings,
    )

    return provider


@pytest.fixture
def storage_object(storage_provider):
    """Create a StorageObject for the test file."""
    # Create storage object
    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    yield obj


@pytest.mark.asyncio
async def test_zenodo_metadata_fetch(storage_provider):
    """Test that we can fetch metadata from Zenodo API."""
    record_id = "16810901"
    netloc = "zenodo.org"

    metadata = await storage_provider.get_metadata(record_id, netloc)

    assert "attributed_ports.json" in metadata
    file_meta = metadata["attributed_ports.json"]
    assert file_meta.checksum is not None
    assert file_meta.size > 0
    assert file_meta.checksum.startswith("md5:")


@pytest.mark.asyncio
async def test_storage_object_exists(storage_object):
    """Test that the storage object reports existence correctly."""
    exists = await storage_object.managed_exists()
    assert exists is True


@pytest.mark.asyncio
async def test_storage_object_size(storage_object):
    """Test that the storage object reports size correctly."""
    size = await storage_object.managed_size()
    assert size > 0
    # The file is a small JSON file, should be less than 1MB
    assert size < 1_000_000


@pytest.mark.asyncio
async def test_storage_object_mtime(storage_object):
    """Test that mtime is 0 for immutable Zenodo URLs."""
    mtime = await storage_object.managed_mtime()
    assert mtime == 0


@pytest.mark.asyncio
async def test_download_and_checksum(storage_object, tmp_path):
    """Test downloading a file and verifying its checksum."""
    local_path = tmp_path / "test_download" / "attributed_ports.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Mock the local_path method to return our test path
    storage_object.local_path = lambda: local_path

    # Download the file
    await storage_object.managed_retrieve()

    # Verify file was downloaded
    assert local_path.exists()
    assert local_path.stat().st_size > 0

    # Verify it's valid JSON (use utf-8 with error handling for any encoding issues)
    with open(local_path, encoding="utf-8", errors="replace") as f:
        data = json.load(f)
        assert isinstance(data, dict)

    # Verify checksum (should not raise WrongChecksum exception)
    await storage_object.verify_checksum(local_path)


@pytest.mark.asyncio
async def test_cache_functionality(storage_provider, tmp_path):
    """Test that files are cached after download."""
    # First download
    obj1 = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    local_path1 = tmp_path / "download1" / "attributed_ports.json"
    local_path1.parent.mkdir(parents=True, exist_ok=True)
    obj1.local_path = lambda: local_path1

    await obj1.managed_retrieve()

    # Verify cache was populated
    assert obj1.query_path is not None
    assert obj1.query_path.exists()

    # Second download should use cache
    obj2 = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    local_path2 = tmp_path / "download2" / "attributed_ports.json"
    local_path2.parent.mkdir(parents=True, exist_ok=True)
    obj2.local_path = lambda: local_path2

    await obj2.managed_retrieve()

    # Both files should be identical
    assert local_path1.read_bytes() == local_path2.read_bytes()


@pytest.mark.asyncio
async def test_skip_remote_checks(tmp_path):
    """Test that skip_remote_checks works correctly."""
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    # Create provider with skip_remote_checks enabled
    settings = StorageProviderSettings(
        cache="",  # No cache
        skip_remote_checks=True,
        max_concurrent_downloads=3,
    )

    logger = logging.getLogger("test")
    provider_skip = StorageProvider(
        local_prefix=local_prefix,
        logger=logger,
        settings=settings,
    )

    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider_skip,
    )

    # With skip_remote_checks, these should return default values without API calls
    assert await obj.managed_exists() is True
    assert await obj.managed_mtime() == 0
    assert await obj.managed_size() == 0


@pytest.mark.asyncio
async def test_wrong_checksum_detection(storage_object, tmp_path):
    """Test that corrupted files are detected via checksum."""
    # Create a corrupted file
    corrupted_path = tmp_path / "corrupted.json"
    corrupted_path.write_text('{"corrupted": "data"}')

    # Verify checksum should raise WrongChecksum
    with pytest.raises(WrongChecksum):
        await storage_object.verify_checksum(corrupted_path)
