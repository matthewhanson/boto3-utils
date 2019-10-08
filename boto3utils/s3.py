import boto3
import json
import logging

import os.path as op

from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dateutil.parser import parse
from gzip import GzipFile
from io import BytesIO
from os import makedirs, getenv

logger = logging.getLogger(__name__)

# s3 client
s3 = boto3.client('s3')


def urlparse(url):
    """ Split S3 URL into bucket, key, filename """
    if url[0:5] != 's3://':
        raise Exception('Invalid S3 url %s' % url)

    url_obj = url.replace('s3://', '').split('/')

    # remove empty items
    url_obj = list(filter(lambda x: x, url_obj))

    return {
        'bucket': url_obj[0],
        'key': '/'.join(url_obj[1:]),
        'filename': url_obj[-1] if len(url_obj) > 1 else ''
    }


def s3_to_https(url, region=getenv('AWS_REGION', getenv('AWS_DEFAULT_REGION', 'us-east-1'))):
    """ Convert an s3 URL to an s3 https URL """    
    parts = urlparse(url)
    return 'https://%s.s3.%s.amazonaws.com/%s' % (parts['bucket'], region, parts['key'])


def exists(url):
    """ Check if this URL exists on S3 """
    parts = urlparse(url)
    try:
        s3.head_object(Bucket=parts['bucket'], Key=parts['key'])
        return True
    except ClientError as exc:
        if exc.response['Error']['Code'] != '404':
            raise
        return False


def upload(filename, uri, public=False, extra={}):
    """ Upload object to S3 uri (bucket + prefix), keeping same base filename """
    logger.debug("Uploading %s to %s" % (filename, uri))
    s3_uri = urlparse(uri)
    uri_out = 's3://%s' % op.join(s3_uri['bucket'], s3_uri['key'])
    if public:
        extra['ACL'] = 'public-read'
    with open(filename, 'rb') as data:
        s3.upload_fileobj(data, s3_uri['bucket'], s3_uri['key'], ExtraArgs=extra)
    return uri_out


def download(uri, path=''):
    """
    Download object from S3
    :param uri: URI of object to download
    :param path: Output path
    """
    s3_uri = urlparse(uri)
    fout = op.join(path, s3_uri['filename'])
    logger.debug("Downloading %s as %s" % (uri, fout))
    if path != '':
        makedirs(path, exist_ok=True)

    with open(fout, 'wb') as f:
        s3.download_fileobj(
            Bucket=s3_uri['bucket'],
            Key=s3_uri['key'],
            Fileobj=f
        )
    return fout


def read_json(url):
    """
    Download object from S3 as JSON
    """
    parts = urlparse(url)
    response = s3.get_object(Bucket=parts['bucket'], Key=parts['key'])
    body = response['Body'].read()
    if op.splitext(parts['key'])[1] == '.gz':
        body = GzipFile(None, 'rb', fileobj=BytesIO(body)).read()
    return json.loads(body.decode('utf-8'))


def find(url, suffix=''):
    """
    Generate objects in an S3 bucket.
    :param url: The beginning part of the URL to match (bucket + optional prefix)
    :param suffix: Only fetch objects whose keys end with this suffix.
    """
    parts = urlparse(url)
    kwargs = {'Bucket': parts['bucket']}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(parts['key'], str):
        kwargs['Prefix'] = parts['key']

    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(parts['key']) and key.endswith(suffix):
                yield obj['Key']

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
