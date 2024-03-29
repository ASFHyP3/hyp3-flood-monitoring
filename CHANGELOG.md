# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.1]
### Changed
- `transfer-products` now runs every 30 minutes instead of every six hours.
- `transfer-products` no longer transfers the `dem.tif` product.

## [0.4.0]
### Changed
- S3 objects are now copied directly to the target bucket, rather than being downloaded and uploaded.

## [0.3.1]
### Fixed
- Missing `ListBucket` permissions for transfer-products lambda function

## [0.3.0]
### Added
- `ProcessingState` cloudformation parameter to support enabling/disabling processing on a per-deployment basis.
### Changed
- `transfer_products.py` no longer includes the job name or job ID in the target S3 key

## [0.2.0]
### Changed
- The `generate-stats` action can now be triggered manually for either test or prod. Automatic workflow
  runs have been disabled.

## [0.1.0]
### Added
- Initial release of the HyP3 Flood Monitoring system.
