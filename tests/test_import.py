# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Basic import tests for the snakemake-storage-plugin-cached-http package."""

import pytest


def test_import_module():
    """Test that the main module can be imported."""
    import snakemake_storage_plugin_cached_http

    assert snakemake_storage_plugin_cached_http is not None


def test_import_storage_provider():
    """Test that StorageProvider class can be imported."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    assert StorageProvider is not None


def test_import_storage_object():
    """Test that StorageObject class can be imported."""
    from snakemake_storage_plugin_cached_http import StorageObject

    assert StorageObject is not None


def test_storage_provider_has_required_methods():
    """Test that StorageProvider has required interface methods."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    # Check for key methods required by the storage plugin interface
    assert hasattr(StorageProvider, "is_valid_query")
    assert hasattr(StorageProvider, "example_queries")
    assert hasattr(StorageProvider, "get_storage_object_cls")


def test_is_valid_query_zenodo():
    """Test that is_valid_query accepts zenodo.org URLs."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    # Valid Zenodo URL
    result = StorageProvider.is_valid_query(
        "https://zenodo.org/records/3520874/files/natura.tiff"
    )
    assert result.valid is True

    # Valid sandbox Zenodo URL
    result = StorageProvider.is_valid_query(
        "https://sandbox.zenodo.org/records/123/files/test.csv"
    )
    assert result.valid is True


def test_is_valid_query_non_zenodo():
    """Test that is_valid_query rejects non-zenodo URLs."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    # Non-Zenodo URL should be rejected
    result = StorageProvider.is_valid_query("https://example.com/file.txt")
    assert result.valid is False
    assert "zenodo" in result.reason.lower()


def test_example_queries():
    """Test that example queries are provided and valid."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    examples = StorageProvider.example_queries()
    assert len(examples) > 0
    assert all(hasattr(ex, "query") for ex in examples)
    assert all(hasattr(ex, "description") for ex in examples)
