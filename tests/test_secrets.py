# this must be imported before any boto3 module
from moto import mock_secretsmanager

import os
import boto3
import pytest
import json
import base64

from boto3utils import secrets
from botocore.exceptions import ClientError

testpath = os.path.dirname(__file__)

SECRET_NAME = 'secret'
SECRET = {'mock_key': 'mock_val'}
SECRET_STRING = json.dumps(SECRET)
SECRET_BINARY = base64.b64encode(SECRET_STRING.encode())


@mock_secretsmanager
def test_get_secret_string():
    boto3.session.Session().client('secretsmanager',
                                   region_name='us-east-1').create_secret(
                                       Name=SECRET_NAME,
                                       SecretString=SECRET_STRING)
    secret = secrets.get_secret(SECRET_NAME)
    assert (secret == SECRET)


@mock_secretsmanager
def test_get_secret_undef():
    with pytest.raises(ClientError):
        secrets.get_secret(SECRET_NAME)


@mock_secretsmanager
def test_get_secret_binary():
    boto3.session.Session().client('secretsmanager',
                                   region_name='us-west-2').create_secret(
                                       Name=SECRET_NAME,
                                       SecretBinary=SECRET_BINARY)
    secret = secrets.get_secret(SECRET_NAME)
    assert (secret == SECRET)
    # client.create_secret(Name=SECRET_NAME, SecretBinary=SECRET_BINARY)
