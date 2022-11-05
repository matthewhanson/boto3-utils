from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from typing import Optional

from boto3utils import s3
from dateutil.parser import parse

logger = logging.getLogger(__name__)


class S3Inventory(object):
    def __init__(self,
                 href,
                 date: str = datetime.now(),
                 max_age: int = 5,
                 **kwargs):

        self.href = href
        self.s3client = s3(**kwargs)

        self.datetime = parse(date) if isinstance(date, str) else date

        self.manifest = self.read_manifest(href,
                                           self.datetime,
                                           max_age=max_age,
                                           s3client=self.s3client)

        # get file schema
        self.schema = [
            str(key).strip() for key in self.manifest['fileSchema'].split(',')
        ]

    @classmethod
    def read_manifest(cls,
                      href: str,
                      date: datetime = datetime.now(),
                      max_age: int = 1,
                      s3client: "s3" = None):
        """ Get latest inventory manifest file """
        if s3client is None:
            s3client = s3()

        parts = s3client.urlparse(href)

        logger.info(f"Reading manifest file {href}")

        # get manifest file for date, default to latest
        for dt in [date - timedelta(x) for x in range(max_age)]:
            _key = Path(parts['key']) / dt.strftime('%Y-%m-%d')
            _href = 's3://%s/%s' % (parts['bucket'], _key)
            manifests = [
                k for k in s3client.find(_href, suffix='manifest.json')
            ]
            if len(manifests) == 1:
                url = manifests[0]
                manifest = s3client.read_json(url)
                manifest['datetime'] = Path(url).parent.stem
                return manifest

    def inventory_file_hrefs(self, outpath: str = None):
        bucket = self.s3client.urlparse(self.href)['bucket']

        files = self.manifest.get('files', [])
        numfiles = len(files)
        logger.info(f"{numfiles} inventory files")

        return ['s3://%s/%s' % (bucket, f['key']) for f in files]

    @classmethod
    def read_inventory_file(cls,
                            fname,
                            schema,
                            s3client: Optional["s3"] = None):
        logger.debug('Reading inventory file %s' % (fname))

        inv = [{
            schema[i]: v
            for i, v in enumerate(line.replace('"', '').split(','))
        } for line in s3client.read(fname).split('\n')]

        return inv

    @classmethod
    def save_inventory_file(cls,
                            fname,
                            schema,
                            s3client,
                            outpath: str = '',
                            overwrite=False):
        fout = Path(outpath, Path(fname).stem)
        if not fout.exists() or overwrite:
            inv = cls.read_inventory_file(fname, schema, s3client)
            with open(fout, 'w') as f:
                f.write(json.dumps(inv))
        return fout

    @classmethod
    def filter_inventory_file(cls,
                              fname,
                              schema,
                              prefix=None,
                              suffix=None,
                              start_date=None,
                              end_date=None,
                              is_latest=None,
                              key_contains=None,
                              datetime_key='LastModifiedDate',
                              s3client: "s3" = s3()):
        inv = cls.read_inventory_file(fname, schema, s3client=s3client)

        def fvalid(info):
            return True if 'Key' in info and 'Bucket' in info else False

        def fprefix(info):
            return True if info['Key'][:len(prefix)] == prefix else False

        def fsuffix(info):
            return True if info['Key'].endswith(suffix) else False

        def fstartdate(info):
            dt = datetime.strptime(info[datetime_key],
                                   "%Y-%m-%dT%H:%M:%S.%fZ").date()
            return True if dt > start_date else False

        def fenddate(info):
            dt = datetime.strptime(info[datetime_key],
                                   "%Y-%m-%dT%H:%M:%S.%fZ").date()
            return True if dt < end_date else False

        def islatest(info):
            latest = info.get("IsLatest")
            if latest:
                return latest not in ("false", False)
            return True

        def fcontains(info):
            for part in key_contains:
                if part not in info["Key"]:
                    return False
            return True

        inv = filter(fvalid, inv)

        if is_latest is not None:
            inv = filter(islatest, inv)
        if key_contains:
            inv = filter(fcontains, inv)
        if prefix:
            inv = filter(fprefix, inv)
        if suffix:
            inv = filter(fsuffix, inv)
        if start_date:
            inv = filter(fstartdate, inv)
        if end_date:
            inv = filter(fenddate, inv)

        _i = -1
        for _i, i in enumerate(inv):
            yield 's3://%s/%s' % (i['Bucket'], i['Key'])
        logger.info(f"Matched {_i+1} files")

    def latest_inventory_files(self, url, manifest=None):
        if not manifest:
            manifest = self.latest_inventory_manifest(url)

        bucket = self.urlparse(url)['bucket']
        if manifest:
            files = manifest.get('files', [])
            numfiles = len(files)
            logger.info('Getting latest inventory from %s (%s files)' %
                        (url, numfiles))

            for f in files:
                _url = 's3://%s/%s' % (bucket, f['key'])
                yield _url

    def latest_inventory(self, url, **kwargs):
        """ Return generator function for objects in Bucket with suffix (all files if suffix=None) """
        manifest_age_days = kwargs.pop("manifest_age_days", 1)
        manifest = self.latest_inventory_manifest(url, manifest_age_days)

        # read through latest manifest looking for matches
        if manifest:
            # get file schema
            keys = [
                str(key).strip() for key in manifest['fileSchema'].split(',')
            ]

            for i, url in enumerate(self.latest_inventory_files(url,
                                                                manifest)):
                logger.info('Reading inventory file %s' % (i + 1))
                results = self.read_inventory_file(url, keys, **kwargs)
                yield from results


if __name__ == "__main__":
    logging.basicConfig(level='INFO',
                        format="%(asctime)s [%(levelname)8s] %(message)s")
    inv = S3Inventory(
        "s3://sentinel-inventory/sentinel-s2-l2a/sentinel-s2-l2a-inventory",
        date='2022-10-31')
    hrefs = inv.inventory_file_hrefs()
    num_files = len(hrefs)
    filters = {"suffix": "tileInfo.json"}
    total = 0
    for i, fname in enumerate(hrefs):
        logger.info(f"Reading inventory file {i+1}/{num_files}: {fname}")
        filenames = []
        finv = inv.filter_inventory_file(fname,
                                         inv.schema,
                                         s3client=inv.s3client,
                                         **filters)
        for f in finv:
            filenames.append(f)
        total += len(filenames)
    logger.info(f"Matched {total} total files in {num_files} inventory files")
