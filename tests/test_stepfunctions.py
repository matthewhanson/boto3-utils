#import boto3
#import os
#import pytest

# this must be imported before any boto3 module
#from moto import mock_sfn

#from boto3utils import stepfunctions as sfn
#from shutil import rmtree


#@pytest.fixture(scope='function')
#def sfn_mock():
#    with mock_sfn():
#        client = boto3.client('stepfunctions')
#        yield client


#def test_run_activity(sfn):
#    sfn.run_activity(sfn, arn)