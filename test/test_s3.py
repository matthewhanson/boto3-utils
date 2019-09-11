import boto3
import os
import pytest

from moto import mock_s3

# setup test fixtures
@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-west-2')

from boto3utils import s3 as s3utils


testpath = os.path.dirname(__file__)


def test_urlparse():
    parts = s3utils.urlparse('s3://bucket/path')
    assert(parts['bucket'] == 'bucket')
    assert(parts['key'] == 'path')
    assert(parts['key'] == parts['filename'])

def test_urlparse_nokey():
    parts = s3utils.urlparse('s3://bucket')
    assert(parts['bucket'] == 'bucket')
    assert(parts['key'] == '')
    assert(parts['filename'] == '')

def test_urlparse_invalid():
    with pytest.raises(Exception):
        s3utils.urlparse('invalid')
    
def test_exists(s3):
    bucket = 'HoleInTheBucket'
    key = 'keymaster'
    s3.create_bucket(Bucket=bucket)
    exists = s3utils.exists('s3://%s/%s' % (bucket, key))
    assert(exists is False)
    s3.put_object(Body='helloworld', Bucket=bucket, Key=key)
    exists = s3utils.exists('s3://%s/%s' % (bucket, key))
    assert(exists)

def test_exists_invalid():
    with pytest.raises(Exception):
        s3utils.exists('invalid')

def test_upload_download(s3):
    bucket = 'BucketHead'
    s3.create_bucket(Bucket=bucket)
    url = 's3://%s/mytestfile' % bucket
    s3utils.upload(__file__, url)
    exists = s3utils.exists(url)
    assert(exists)
    filename = os.path.join(testpath, 'test_s3/test_upload_download.py')
    fname = s3utils.download(url, filename)
    assert(os.path.exists(fname))
    assert(filename == fname)
