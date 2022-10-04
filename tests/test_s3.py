import boto3
import os
import pytest

from boto3utils import s3
from shutil import rmtree

BUCKET = 'testbucket'
BUCKET_WEST = 'testbucket_west'
KEY = 'testkey'


def create_test_bucket(s3, bucket):
    params = {
        'Bucket': bucket,
    }

    if s3.meta.region_name != 'us-east-1':
        params['CreateBucketConfiguration'] = {
            'LocationConstraint': s3.meta.region_name,
        }

    s3.create_bucket(**params)
    s3.put_object(Body='helloworld', Bucket=bucket, Key=KEY)
    s3.upload_file(Filename=os.path.join(testpath, 'test.json'),
                   Bucket=bucket,
                   Key='test.json')


@pytest.fixture
def s3mock(s3):
    create_test_bucket(s3, BUCKET)
    yield s3


@pytest.fixture
def s3mock_west(s3_west):
    create_test_bucket(s3_west, BUCKET_WEST)
    yield s3_west


testpath = os.path.dirname(__file__)


def test_urlparse_params():
    parts = s3.urlparse('s3://bucket/path?VersionId=yellowbeardthepirate')
    assert (parts['bucket'] == 'bucket')
    assert (parts['key'] == 'path')
    assert (parts['key'] == parts['filename'])
    assert (parts['parameters'] == {'VersionId': 'yellowbeardthepirate'})


def test_urlparse():
    parts = s3.urlparse('s3://bucket/path')
    assert (parts['bucket'] == 'bucket')
    assert (parts['key'] == 'path')
    assert (parts['key'] == parts['filename'])
    assert (parts['parameters'] == {})


def test_urlparse_nokey():
    parts = s3.urlparse('s3://bucket')
    assert (parts['bucket'] == 'bucket')
    assert (parts['key'] == '')
    assert (parts['filename'] == '')
    assert (parts['parameters'] == {})


def test_urlparse_invalid():
    with pytest.raises(Exception):
        s3.urlparse('invalid')


def test_s3_to_https():
    s3url = 's3://bucket/prefix/filename'
    url = s3.s3_to_https(s3url, region='us-west-2')
    assert (url == 'https://bucket.s3.us-west-2.amazonaws.com/prefix/filename')


def test_get_bucket_region_null(s3mock):
    region = s3().get_bucket_region(BUCKET)
    assert region == 'us-east-1'


def test_get_bucket_region(s3mock_west):
    region = s3().get_bucket_region(BUCKET_WEST)
    assert region == 'us-west-2'


def test_exists(s3mock):
    exists = s3().exists('s3://%s/%s' % (BUCKET, 'keymaster'))
    assert (exists is False)
    exists = s3().exists('s3://%s/%s' % (BUCKET, KEY))
    assert (exists)


def test_exists_invalid():
    with pytest.raises(Exception):
        s3().exists('invalid')


def test_upload_download(s3mock):
    url = 's3://%s/mytestfile' % BUCKET
    s3().upload(__file__, url, public=True)
    exists = s3().exists(url)
    assert (exists)
    path = os.path.join(testpath, 'test_s3/test_upload_download')
    fname = s3().download(url, path)
    assert (os.path.exists(fname))
    assert (os.path.join(path, os.path.basename(url)) == fname)
    rmtree(path)


def test_upload_getobject(s3mock):
    # upload the object
    url = 's3://%s/mytestfile' % BUCKET
    s3().upload(__file__, url, public=True)
    exists = s3().exists(url)
    assert (exists)

    obj1 = s3().get_object(BUCKET, "mytestfile")

    obj2 = s3().get_object(BUCKET,
                           "mytestfile",
                           extra_args={'IfMatch': obj1['ETag']})

    assert obj1['ETag'] == obj2['ETag']
    assert obj1['ContentLength'] == obj2['ContentLength']


def test_upload_download_parameters(s3mock):
    # upload the file
    url = 's3://%s/mytestfile' % BUCKET
    s3().upload(__file__, url, public=True)
    exists = s3().exists(url)
    assert (exists)

    # download the with metadata
    path = os.path.join(testpath, 'test_s3/test_upload_download')
    fname, meta = s3().download_with_metadata(
        url, path, extra_args={"ChecksumMode": "Enabled"})
    assert (os.path.exists(fname))
    assert (os.path.join(path, os.path.basename(url)) == fname)
    assert 'ETag' in meta
    assert 'binary/octet-stream' == meta['ContentType']
    assert 'LastModified' in meta
    rmtree(path)


def test_read_json(s3mock):
    url = 's3://%s/test.json' % BUCKET
    out = s3().read_json(url)
    assert (out['field'] == 'value')


def test_delete(s3mock):
    url = 's3://%s/test.json' % BUCKET
    out = s3().delete(url)
    assert (out['ResponseMetadata']['HTTPStatusCode'] == 204)


def test_find(s3mock):
    url = 's3://%s/test' % BUCKET
    urls = list(s3().find(url))
    assert (len(urls) > 0)
    assert (url + '.json' in urls)


def test_latest_inventory():
    from botocore.handlers import disable_signing

    url = 's3://sentinel-inventory/sentinel-s1-l1c/sentinel-s1-l1c-inventory'
    suffix = 'productInfo.json'
    session = boto3.Session()
    _s3 = s3(session)

    # as we are actually hitting S3 (which is not great), we need
    # to prevent signing our request with the dummy test credentials
    _s3.s3.meta.events.register('choose-signer.s3.*', disable_signing)

    for url in _s3.latest_inventory(url, suffix=suffix):
        # dt = datetime.strptime(f['LastModifiedDate'], "%Y-%m-%dT%H:%M:%S.%fZ")
        # hours = (datetime.today() - dt).seconds // 3600
        # assert(hours < 24)
        assert (url.endswith(suffix))
        break
    # for f in _s3.latest_inventory(url):
    #    dt = datetime.strptime(f['LastModifiedDate'], "%Y-%m-%dT%H:%M:%S.%fZ")
    #    hours = (datetime.today() - dt).seconds // 3600
    #    assert(hours < 24)
    #    break
