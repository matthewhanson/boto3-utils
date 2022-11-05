import pytest

from boto3utils.s3inventory import S3Inventory

DATE = '2022-10-31'


@pytest.fixture
def test_inventory():
    inv = S3Inventory(
        "s3://sentinel-inventory/sentinel-s2-l2a/sentinel-s2-l2a-inventory",
        date=DATE)
    yield inv


def test_init_s3inventory(test_inventory):
    assert (test_inventory.manifest['datetime'].startswith(DATE))
    assert (test_inventory.manifest['creationTimestamp'] == '1667178000000')
    assert (test_inventory.manifest['sourceBucket'] == 'sentinel-s2-l2a')
    assert (len(test_inventory.manifest['files']) == 1258)


def test_inventory_files(test_inventory):
    filenames = test_inventory.inventory_file_hrefs()
    assert (len(filenames) == 1258)
