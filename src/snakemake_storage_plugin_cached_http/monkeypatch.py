# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""
Warn if snakemake-storage-plugin-http is installed and patch it to refuse all URLs.

Since cached-http now accepts all HTTP(S) URLs, having snakemake-storage-plugin-http
installed at the same time causes Snakemake to raise "Multiple suitable storage
providers found". This module detects that situation, warns the user, and patches the
HTTP plugin to refuse every URL so that cached-http wins unconditionally.
"""

import warnings

from snakemake_interface_storage_plugins.storage_provider import (
    StorageQueryValidationResult,
)

try:
    import snakemake_storage_plugin_http

    warnings.warn(
        "snakemake-storage-plugin-http is installed alongside "
        "snakemake-storage-plugin-cached-http. Because cached-http from v0.5 handles "
        "all HTTP(S) URLs, the http plugin has been disabled for this session. "
        "Please uninstall snakemake-storage-plugin-http. "
        "This compatibility shim will be removed in v0.6.",
        FutureWarning,
    )

    snakemake_storage_plugin_http.StorageProvider.is_valid_query = classmethod(
        lambda c, q: StorageQueryValidationResult(
            query=q,
            valid=False,
            reason="Disabled: snakemake-storage-plugin-cached-http handles all HTTP(S) URLs",
        )
    )
except ImportError:
    pass
