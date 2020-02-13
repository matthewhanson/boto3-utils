# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/matthewhanson/boto3-utils/compare/master...develop
[v0.2.0]: https://github.com/matthewhanson/boto3-utils/compare/0.1.3...0.2.0
[v0.1.3]: https://github.com/matthewhanson/boto3-utils/compare/0.1.2...0.1.3
[v0.1.2]: https://github.com/matthewhanson/boto3-utils/compare/0.1.1...0.1.2
[v0.1.1]: https://github.com/matthewhanson/boto3-utils/compare/0.1.0...0.1.1
[v0.1.0]: https://github.com/matthewhanson/boto3-utils/compare/0.0.3...0.1.0
[v0.0.3]: https://github.com/matthewhanson/boto3-utils/compare/0.0.2...0.0.3
[v0.0.2]: https://github.com/matthewhanson/boto3-utils/tree/0.0.2
