# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- s3.download_with_metadata() downloads a file and returns both the file path and a selection of metadata about the object (e.g. ETag, Version, etc)
- s3.get_object() takes a new argument, `extra_args={}` that is passed to the underlying client
- s3.download() takes a new argument, `extra_args={}` that is passed to the underlying client
- s3.download() parses query parameters from the provided url, adding them  to the `extra_args` passed to the underlying client

### Modified
- s3.urlparse() now supports query parameters, returning them in the `'query_params'` section of the returned `dict`
- added `coverage.xml` to `.gitignore`

## [v0.3.3] - 2022-09-14

### Added
- s3.latest_inventory() takes manifest_age_days argument for how far back to look for manifest
- s3.latest_inventory() takes is_latest argument for filtering on versioned files
- s3.latest_inventory() takes key_contains array argument for filtering on key containing strings of the array

### Changed
- No longer testing against python 3.6
- Now testing against python 3.9 and 3.10

### Fixed
- s3.delete works
- Handle bucket locations in US Standard region

## [v0.3.2] - 2021-07-15

### Added
- tests for secrets
- formatting and linting added to CI tests

### Removed
- Logging of error when fetching secrets (leave to caller)

### Changed
- Moved from CircleCI to GitHub Actions
- test directory renamed to tests
- Update moto version
- Linting and formatting updates

## [v0.3.1] - 2020-07-29

### Changed
- s3.download() now uses boto3 download_file for multipart stream downloads to avoid memory errors downloading large files

## [v0.3.0] - 2020-06-14

### Changed
- `requester_pays` parameter now argument to s3 session rather than download

## [v0.2.3] - 2020-05-06

### Fixed
- Syntax error with s3().upload_json

## [v0.2.2] - 2020-05-05

### Added
- Split up s3.latest_inventory() into multiple more modular functions, there is now
  - s3.latest_inventory_manifest() - get manifest file for latest inventory
  - s3.latest_inventory_files() - iterator to inventory files (each is listing of files in inventory)

## [v0.2.1] - 2020-02-28

### Added
- secrets module with get_secrets function for getting AWS secret as a dictionary {key:value}

### Fixed
- s3.urlpase when URL is an http URL

## [v0.2.0] - 2020-02-12

### Added
- Updated logging when reading from AWS inventory
- s3.read_inventory_file added, breaking up functionality from s3.latest_inventory
- s3.read and s3.read_json now take requester_pays keyword

### Changed
- s3.latest_inventory() returns URLs instead of an object
- s3.latest_inventory faster filtering

## [v0.1.3] - 2020-01-31

### Added
- s3().https_to_s3 function
- urlparse, and all s3() functions that take in an URL, now take in either an https or s3 url to an s3 object

## [v0.1.2] - 2020-01-12

### Added
- s3().delete function
- s3().upload_json() function for uploading JSON as a file to s3

## [v0.1.1] - 2019-12-06

### Added
- requester_pays option to s3.download, defaults to False

## [v0.1.0] - 2019-12-05

### Changed
- The s3 and stepfunction modules are now a class, and the init function accepts a boto3 Session. If not provided a default session is created
- s3.upload now accepts an `http_url` keyword. If set to True it will return the https URL instead of the S3 URL
- s3.find now returns complete s3 URL for each found object, not just the key

## [v0.0.3] - 2019-11-22

### Added
- s3.latest_inventory iterator function added for looping through matching files in an s3 inventory
- s3.get_presigned_url for generating a presigned URL for s3....does not use boto3

## [v0.0.2] - 2019-10-08

Initial Release

[Unreleased]: https://github.com/matthewhanson/boto3-utils/compare/0.3.2...main
[v0.3.2]: https://github.com/matthewhanson/boto3-utils/compare/0.3.1...0.3.2
[v0.3.1]: https://github.com/matthewhanson/boto3-utils/compare/0.3.0...0.3.1
[v0.3.0]: https://github.com/matthewhanson/boto3-utils/compare/0.2.3...0.3.0
[v0.2.3]: https://github.com/matthewhanson/boto3-utils/compare/0.2.2...0.2.3
[v0.2.2]: https://github.com/matthewhanson/boto3-utils/compare/0.2.1...0.2.2
[v0.2.1]: https://github.com/matthewhanson/boto3-utils/compare/0.2.0...0.2.1
[v0.2.0]: https://github.com/matthewhanson/boto3-utils/compare/0.1.3...0.2.0
[v0.1.3]: https://github.com/matthewhanson/boto3-utils/compare/0.1.2...0.1.3
[v0.1.2]: https://github.com/matthewhanson/boto3-utils/compare/0.1.1...0.1.2
[v0.1.1]: https://github.com/matthewhanson/boto3-utils/compare/0.1.0...0.1.1
[v0.1.0]: https://github.com/matthewhanson/boto3-utils/compare/0.0.3...0.1.0
[v0.0.3]: https://github.com/matthewhanson/boto3-utils/compare/0.0.2...0.0.3
[v0.0.2]: https://github.com/matthewhanson/boto3-utils/tree/0.0.2
