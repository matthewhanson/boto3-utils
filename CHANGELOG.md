# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- The s3 module is now a class. The class init function accepts a boto3 Session
- s3.s3_to_https upload now accepts a `http_url` keyword. If set to True it will return the https URL instead of the S3 URL

### Changed
- s3.find now returns complete s3 URL for each found object, not just the key

## [v0.0.3] = 2019-11-22

### Added
- s3.latest_inventory iterator function added for looping through matching files in an s3 inventory
- s3.get_presigned_url for generating a presigned URL for s3....does not use boto3

## [v0.0.2] - 2019-10-08

Initial Release

[Unreleased]: https://github.com/matthewhanson/boto3-utils/compare/master...develop
[v0.0.3]: https://github.com/matthewhanson/boto3-utils/compare/0.0.2...0.0.3
[v0.0.2]: https://github.com/matthewhanson/boto3-utils/tree/0.0.2
