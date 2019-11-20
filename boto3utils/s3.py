import boto3
import json
import logging

import os.path as op

from botocore.exceptions import ClientError
from datetime import datetime, timedelta
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


def read(url):
    """ Read object from s3 """
    parts = urlparse(url)
    response = s3.get_object(Bucket=parts['bucket'], Key=parts['key'])
    body = response['Body'].read()
    if op.splitext(parts['key'])[1] == '.gz':
        body = GzipFile(None, 'rb', fileobj=BytesIO(body)).read()
    return body.decode('utf-8')


def read_json(url):
    """ Download object from S3 as JSON """
    return json.loads(read(url))


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


def latest_inventory(url, suffix=None):
    """ Return generator function for for objects in Bucket with suffix (all files if suffix=None) """
    parts = urlparse(url)
    # get latest manifest file
    today = datetime.now()
    manifest_key = None
    for dt in [today, today - timedelta(1)]:
        prefix = op.join(parts['key'], dt.strftime('%Y-%m-%d'))
        _url = 's3://%s/%s' % (parts['bucket'], prefix)
        keys = [k for k in find(_url, suffix='manifest.json')]
        if len(keys) == 1:
            manifest_key = keys[0]
            break
    if manifest_key:
        _url = 's3://%s/%s' % (parts['bucket'], manifest_key)
        manifest = read_json(_url)
        for f in manifest.get('files', []):
            _url = 's3://%s/%s' % (parts['bucket'], f['key'])
            inv = read(_url).split('\n')
            for line in inv:
                info = line.replace('"', '').split(',')
                dt = datetime.strptime(info[3], "%Y-%m-%dT%H:%M:%S.%fZ")
                url = 's3://%s/%s' % (parts['bucket'], info[1])    
                if suffix is None or info[1].endswith(suffix):   
                    yield {
                        'datetime': dt,
                        'url': url             
                    }
