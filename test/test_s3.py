import boto3
import os
import pytest

from datetime import datetime
# this must be imported before any boto3 module
from moto import mock_s3

from boto3utils import s3
from shutil import rmtree


BUCKET = 'testbucket'
KEY = 'testkey'


@pytest.fixture(scope='function')
def s3mock():
    with mock_s3():
        client = boto3.client('s3', region_name='us-west-2', 
                              aws_access_key_id='noid', aws_secret_access_key='nokey')
        client.create_bucket(Bucket=BUCKET)
        client.put_object(Body='helloworld', Bucket=BUCKET, Key=KEY)
        client.upload_file(Filename=os.path.join(testpath, 'test.json'), Bucket=BUCKET, Key='test.json')
        yield client


testpath = os.path.dirname(__file__)


def test_urlparse():
    parts = s3.urlparse('s3://bucket/path')
    assert(parts['bucket'] == 'bucket')
    assert(parts['key'] == 'path')
    assert(parts['key'] == parts['filename'])

def test_urlparse_nokey():
    parts = s3.urlparse('s3://bucket')
    assert(parts['bucket'] == 'bucket')
    assert(parts['key'] == '')
    assert(parts['filename'] == '')

def test_urlparse_invalid():
    with pytest.raises(Exception):
        s3.urlparse('invalid')

def test_s3_to_https():
    s3url = 's3://bucket/prefix/filename'
    url = s3.s3_to_https(s3url, region='us-west-2')
    assert(url == 'https://bucket.s3.us-west-2.amazonaws.com/prefix/filename')
    
def test_exists(s3mock):
    exists = s3().exists('s3://%s/%s' % (BUCKET, 'keymaster'))
    assert(exists is False)
    exists = s3().exists('s3://%s/%s' % (BUCKET, KEY))
    assert(exists)

def test_exists_invalid():
    with pytest.raises(Exception):
        s3().exists('invalid')

def test_upload_download(s3mock):
    url = 's3://%s/mytestfile' % BUCKET
    s3().upload(__file__, url, public=True)
    exists = s3().exists(url)
    assert(exists)
    path = os.path.join(testpath, 'test_s3/test_upload_download')
    fname = s3().download(url, path)
    assert(os.path.exists(fname))
    assert(os.path.join(path, os.path.basename(url)) == fname)
    rmtree(path)

def test_read_json(s3mock):
    url = 's3://%s/test.json' % BUCKET
    out = s3().read_json(url)
    assert(out['field'] == 'value')

def test_find(s3mock):
    url = 's3://%s/test' % BUCKET
    urls = list(s3().find(url))
    assert(len(urls) > 0)
    assert(url + '.json' in urls)

def test_latest_inventory():
    url = 's3://sentinel-inventory/sentinel-s1-l1c/sentinel-s1-l1c-inventory'
    suffix = 'productInfo.json'
    session = boto3.Session()
    _s3 = s3(session)
    for url in _s3.latest_inventory(url, suffix=suffix):
        #dt = datetime.strptime(f['LastModifiedDate'], "%Y-%m-%dT%H:%M:%S.%fZ")
        #hours = (datetime.today() - dt).seconds // 3600
        #assert(hours < 24)
        assert(url.endswith(suffix))
        break
    #for f in _s3.latest_inventory(url):
    #    dt = datetime.strptime(f['LastModifiedDate'], "%Y-%m-%dT%H:%M:%S.%fZ")
    #    hours = (datetime.today() - dt).seconds // 3600
    #    assert(hours < 24)
    #    break