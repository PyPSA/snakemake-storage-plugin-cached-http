<!--
SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

## Unreleased — **Breaking change**

This release accepts all HTTP(S) URLs and therefore conflicts with `snakemake-storage-plugin-http`.
**You must uninstall `snakemake-storage-plugin-http`** before upgrading, otherwise Snakemake will
raise *"Multiple suitable storage providers found"* for any HTTP(S) URL.

### Added

- Generic HTTP(S) fallback: any `http://` or `https://` URL is now accepted, with size and
  mtime read from `Content-Length` and `Last-Modified` response headers. Servers that do not
  support `HEAD` requests are handled gracefully (size and mtime default to 0). No checksum
  is available for generic URLs.

### Removed

- Dependency on `snakemake-storage-plugin-http` — this plugin now handles all HTTP(S) URLs
  directly, with no monkey-patching required.

## v0.4.0 — Google Cloud Storage support

### Added

- Support for `storage.googleapis.com` URLs with checksum verification via the GCS JSON API
  (`md5Hash` field) and mtime from GCS object metadata.

## v0.3.0 — data.pypsa.org support

### Added

- Support for `data.pypsa.org` URLs with checksum verification via `manifest.yaml` files
  discovered by searching up the directory tree.
- Redirect support: manifest entries can specify a `redirect` field to point to another path.

## v0.2.0 — Dynamic versioning and zstd support

### Added

- Dynamic versioning via `setuptools-scm`.
- `zstandard` dependency for decompressing Cloudflare-compressed responses.

## v0.1.0 — Initial release

### Added

- Snakemake storage plugin for Zenodo URLs (`zenodo.org`, `sandbox.zenodo.org`) with:
  - Local filesystem caching via `Cache` class
  - Checksum verification from Zenodo API
  - Adaptive rate limiting using `X-RateLimit-*` headers with exponential backoff retry
  - Concurrent download limiting via semaphore
  - Progress bars with `tqdm-loggable`
