import boto3
import os
import pytest

from moto import mock_s3
from boto3utils import s3 as s3utils


BUCKET = 'testbucket'
KEY = 'testkey'


@pytest.fixture(scope='function')
def s3():
    with mock_s3():
        client = boto3.client('s3', region_name='us-west-2', 
                              aws_access_key_id='noid', aws_secret_access_key='nokey')
        client.create_bucket(Bucket=BUCKET)
        client.put_object(Body='helloworld', Bucket=BUCKET, Key=KEY)
        client.upload_file(Filename=os.path.join(testpath, 'test.json'), Bucket=BUCKET, Key='test.json')
        yield client


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

def test_s3_to_https():
    s3url = 's3://bucket/prefix/filename'
    url = s3utils.s3_to_https(s3url, region='us-west-2')
    assert(url == 'https://bucket.s3.us-west-2.amazonaws.com/prefix/filename')
    
def test_exists(s3):
    exists = s3utils.exists('s3://%s/%s' % (BUCKET, 'keymaster'))
    assert(exists is False)
    exists = s3utils.exists('s3://%s/%s' % (BUCKET, KEY))
    assert(exists)

def test_exists_invalid():
    with pytest.raises(Exception):
        s3utils.exists('invalid')

def test_upload_download(s3):
    url = 's3://%s/mytestfile' % BUCKET
    s3utils.upload(__file__, url)
    exists = s3utils.exists(url)
    assert(exists)
    path = os.path.join(testpath, 'test_s3/test_upload_download')
    fname = s3utils.download(url, path)
    assert(os.path.exists(fname))
    assert(os.path.join(path, os.path.basename(url)) == fname)

def test_read_json(s3):
    url = 's3://%s/test.json' % BUCKET
    out = s3utils.read_json(url)
    assert(out['field'] == 'value')