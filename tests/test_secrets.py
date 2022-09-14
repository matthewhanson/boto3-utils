import pytest
import json
import base64

from boto3utils import secrets
from botocore.exceptions import ClientError


SECRET_NAME = 'secret'
SECRET = {'mock_key': 'mock_val'}
SECRET_STRING = json.dumps(SECRET)
SECRET_BINARY = base64.b64encode(SECRET_STRING.encode())


@pytest.fixture
def secret(secretsmanager):
    secretsmanager.create_secret(
        Name=SECRET_NAME,
        SecretString=SECRET_STRING,
    )
    return secretsmanager


@pytest.fixture
def binary_secret(secretsmanager):
    secretsmanager.create_secret(
        Name=SECRET_NAME,
        SecretBinary=SECRET_BINARY,
    )
    return secretsmanager


def test_get_secret_string(secret):
    secret = secrets.get_secret(SECRET_NAME)
    assert (secret == SECRET)


def test_get_secret_undef(secretsmanager):
    with pytest.raises(ClientError):
        secrets.get_secret(SECRET_NAME)


def test_get_secret_binary(binary_secret):
    secret = secrets.get_secret(SECRET_NAME)
    assert (secret == SECRET)
