import boto3
import json
import logging

import os.path as op

from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dateutil.parser import parse
from gzip import GzipFile
from io import BytesIO
from os import makedirs

logger = logging.getLogger(__name__)

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


def s3url_to_https(url):
    """ Convert an s3 URL to an s3 https URL """
    parts = urlparse(url)
    return 'https://'


def https_to_s3url(url):
    """ Convert an s3 https URL to an s3 URL """
    parts = url.split('/')
    return 's3://'



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
        extra['acl'] = 'public-read'
    with open(filename, 'rb') as data:
        s3.upload_fileobj(data, s3_uri['bucket'], s3_uri['key'], ExtraArgs=extra)
    return uri_out


def download(uri, path=''):
    """ Download object from S3 """
    s3_uri = urlparse(uri)
    fout = op.join(path, s3_uri['filename'])
    logger.debug("Downloading %s as %s" % (uri, fout))
    if path != '':
        os.makedirs(path, exist_ok=True)

    s3 = boto3.client('s3')

    with open(fout, 'wb') as f:
        s3.download_fileobj(
            Bucket=s3_uri['bucket'],
            Key=s3_uri['key'],
            Fileobj=f
        )
    return fout


def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

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
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj['Key']


def read_from_s3(bucket, key):
    """ Download object from S3 as JSON """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read()
    if op.splitext(key)[1] == '.gz':
        body = GzipFile(None, 'rb', fileobj=BytesIO(body)).read()
    return body.decode('utf-8')


def read_inventory(filename):
    """ Create generator from s3 inventory file """
    with open(filename) as f:
        line = f.readline()
        if 'datetime' not in line:
            parts = line.split(',')
            yield {
                'datetime': parse(parts[0]),
                'path': parts[1].strip('\n')
            }
        for line in f.readlines():
            parts = line.split(',')
            yield {
                'datetime': parse(parts[0]),
                'path': parts[1].strip('\n')
            }


def latest_inventory():
    """ Return generator function for list of scenes """
    s3 = boto3.client('s3')
    # get latest file
    today = datetime.now()
    key = None
    for dt in [today, today - timedelta(1)]:
        prefix = op.join(SETTINGS['inv_key'], dt.strftime('%Y-%m-%d'))
        keys = [k for k in get_matching_s3_keys(SETTINGS['inv_bucket'], prefix=prefix, suffix='manifest.json')]
        if len(keys) == 1:
            key = keys[0]
            break
    if key:
        manifest = json.loads(read_from_s3(SETTINGS['inv_bucket'], key))
        for f in manifest.get('files', []):
            inv = read_from_s3(SETTINGS['inv_bucket'], f['key']).split('\n')
            inv = [i.replace('"', '').split(',') for i in inv if 'tileInfo.json' in i]
            for info in inv:
                yield {
                    'datetime': parse(info[3]),
                    'path': info[1]
                }

