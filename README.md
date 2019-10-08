# AWS Utils

This is repository of convenience functions for AWS utilizing [boto3](https://github.com/boto/boto3).

## Modules

### s3

The s3 module contains functions for easily working with S3, such as uploading, downloading, checking for the existence of files, and crawling buckets for matching files.

All functions in the s3 module use S3 URLs rather than separate `bucket` and `key` fields like boto3 uses. Instead, URLs look like:

```
s3://bucketname/prefix/file
```

The `s3.urlparse` function takes in an S3 URL and returns a dictionary containing the components: `bucket`, `key`, and `filename`.

## About
boto3-utils was created by [Matthew Hanson](http://github.com/matthewhanson)
