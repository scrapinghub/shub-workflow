#!/usr/bin/env python
"""
Generate deliverables

Usage:

    deliver.py <spider> [<spider> ... ][options]

For listing valid values for spider argument, just run deliver.py without arguments.

This script does the following:

* searches for untagged finished jobs that corresponds to the given spiders and same flow id as inherited or passed
  to the delivery job.
* gets all their items generated and uploads to customer s3 bucket in files of given max size (either in items or bytes)
* tags processed jobs with 'delivered' tag, so they are not processed again

Deliver class is meant to be subclasses for overriding default behaviors, either via overriding of configuration class
attributes or instance methods.

configuration class attributes
------------------------------

DeliverScript.s3_bucket_name - Target s3 bucket (required)
DeliverScript.success_file - A boolean. Whether or not to generate a finall _SUCCESS file (optional, False by default)

Following environment variables are also required:

AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SH_APIKEY

The default file format are gzipped json list. The default file name format will be:

s3://<bucket_name>/<spider name>/<formatted timestamp>/<file number>.jsonl.gz

The formatted timestamp corresponds to the time where the delivery script starts to run.

These defaults are only there for providing a fast deployable delivery script. But mosts project has its own
requirements, so the DeliverScript is designed for easy overriding of methods for very flexible customization.


"""
import json
import re
import sys
import os
import gzip
import datetime
import logging
import tempfile
from tempfile import mktemp

from s3fs import S3FileSystem
from sqlitedict import SqliteDict

from .script import BaseScript


_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


_FIELD_RE = re.compile(r'\{field:(.+?)\}')
_ARGUMENT_RE = re.compile(r'\{argument:(.+?)\}')


class SqliteDictDupesFilter(object):
    def __init__(self):
        """
        SqlteDict based dupes filter
        """
        self.dupes_db_file = tempfile.mktemp()
        self.__filter = None

    def __create_db(self):
        self.__filter = SqliteDict(self.dupes_db_file, flag='n', autocommit=True)

    def __contains__(self, element):
        if self.__filter is None:
            self.__create_db()
        return element in self.__filter

    def add(self, element):
        if self.__filter is None:
            self.__create_db()
        self.__filter[element] = '-'

    def close(self):
        if self.__filter is not None:
            try:
                self.__filter.close()
                os.remove(self.dupes_db_file)
            except:
                pass


def _upload_file_to_s3(bucketname, keyname, filename=None):
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    with s3.open(f's3://{bucketname}/{keyname}', 'wb') as f:
        if filename is None:
            f.write(b'')
        else:
            with open(filename, 'rb') as r:
                f.write(r.read())


class OutputFile(object):

    max_filesize_items = 1000000
    max_filesize_bytes = 1000000000

    def __init__(self, s3_bucket_name, keyprefix, testmode=False):
        self.__s3_bucket_name = s3_bucket_name
        self.__key_prefix = keyprefix

        self.__filename = mktemp()
        self.__file = self.create_new_file()
        self.__filecount = 0
        self.__items_count = 0
        self.__size_bytes = 0
        self.__testmode = testmode

        spath = self.__key_prefix.split('/')[:-1]
        spath.append('_SUCCESS')
        self.success_file = os.path.join(*spath)

    @property
    def items_count(self):
        return self.__items_count

    @property
    def filename(self):
        return self.__filename

    @property
    def key_prefix(self):
        return self.__key_prefix

    @property
    def filecount(self):
        return self.__filecount

    def create_new_file(self):
        return gzip.open(self.filename, "wb")

    def write(self, item):
        line = json.dumps(item) + '\n'
        self.__file.write(line.encode('utf8'))
        self.__items_count += 1
        self.__size_bytes += sys.getsizeof(line)
        if self.max_filesize_items > 0 and self.__items_count == self.max_filesize_items or \
                self.max_filesize_bytes > 0 and self.__size_bytes >= self.max_filesize_bytes:
            self.flush(new_file=True)

    def flush(self, new_file=False):
        if self.__items_count > 0:
            self.__file.close()
            self._upload_file()
            self.__filecount += 1
            self.__items_count = 0
            self.__size_bytes = 0
            if new_file:
                self.__file = self.create_new_file()
        elif not new_file:
            self.__file.close()

    def gen_keyname(self):
        return self.key_prefix + "%05d.jsonl.gz" % self.filecount

    def _upload_file(self):
        keyname = self.gen_keyname()
        if not self.__testmode:
            _upload_file_to_s3(self.__s3_bucket_name, keyname, self.filename)
        _LOG.info("Saved %d items in s3://%s/%s", self.__items_count, self.__s3_bucket_name, keyname)


class OutputFileDict(object):

    outputfile_class = OutputFile

    def __init__(self, s3_bucket_name, testmode=False):
        self.__s3_bucket_name = s3_bucket_name
        self.__testmode = testmode
        self.__outputfiles = {}

    def __getitem__(self, key):
        if key not in self.__outputfiles:
            self.__outputfiles[key] = self.outputfile_class(s3_bucket_name=self.__s3_bucket_name, keyprefix=key,
                                                            testmode=self.__testmode)
        return self.__outputfiles[key]

    def values(self):
        return self.__outputfiles.values()

    def keys(self):
        return self.__outputfiles.keys()

    def pop(self, key):
        return self.__outputfiles.pop(key)


class S3DeliverScript(BaseScript):

    s3_success_file = False
    s3_bucket_name = None

    default_sh_chunk_size = 1_000

    def __init__(self):
        super().__init__()

        self.output_files = self.set_output_files()
        self.__start_datetime = datetime.datetime.now()

        self.itemcount = 0
        self.filecount = 0
        self.dupes_filter = {i: SqliteDictDupesFilter() for i in self.args.filter_dupes_by_field}

    @property
    def description(self):
        return __doc__

    @property
    def start_datetime(self):
        return self.__start_datetime

    def set_output_files(self):
        return OutputFileDict(self.s3_bucket_name, self.args.test_mode)

    def add_argparser_options(self):
        super().add_argparser_options()

        self.argparser.add_argument('scrapername', help='Indicate target scraper names', nargs='*')
        self.argparser.add_argument('--filter-dupes-by-field', default=[],
                                    help='Dedupe by any of the given item field. Can be given multiple times')
        self.argparser.add_argument('--one-file-per-job', action='store_true', help='Generate one file per job.')
        self.argparser.add_argument('--test-mode', action='store_true',
                                    help='Run in test mode (performs all processes, but doesn\'t\
                                          upload files nor tag jobs)')
        self.argparser.add_argument('--sh-chunk-size', type=int, default=self.default_sh_chunk_size, help=(
            'Chunk/page size for downloading items from Scrapy Cloud. For tweaking memory consumption and speed.'
            ' Note that the performance will depend on the sizes of individual items in the cloud.'
        ))

    def gen_keyprefix(self, scrapername, job, item):
        formatted_datetime = self.start_datetime.strftime('%Y-%m-%dT%H:%M:%S')
        return os.path.join(scrapername, formatted_datetime)

    def _process_job_items(self, scrapername, spider_job):
        first_keyprefix = None
        job_item_count = 0
        chunks = spider_job.items.list_iter(chunksize=self.args.sh_chunk_size)
        items_iter = (item for chunk in chunks for item in chunk)
        for item in items_iter:
            seen = False
            for field in self.dupes_filter.keys():
                if field in item:
                    if item[field] in self.dupes_filter[field]:
                        seen = True
                    else:
                        self.dupes_filter[field].add(item[field])
            if seen:
                continue

            try:
                keyprefix = self.gen_keyprefix(scrapername, spider_job, item)
                first_keyprefix = first_keyprefix or keyprefix
            except KeyError as e:
                if first_keyprefix is None:
                    _LOG.info("Skipped job: %s. %s", spider_job.key, str(e))
                    return
                keyprefix = first_keyprefix
            self.output_files[keyprefix].write(item)
            self.itemcount += 1
            job_item_count += 1
            if self.itemcount % 100000 == 0:
                _LOG.info("Processed %d items.", self.itemcount)
        _LOG.info("Processed all %d items of spider job %s", job_item_count, spider_job.key)

    def run(self):
        success_files = set()
        all_jobs_to_tag = set()
        for scrapername in self.args.scrapername:
            _LOG.info(f"Processing spider {scrapername}")
            jobs_to_tag = self.process_spider_jobs(scrapername)
            all_jobs_to_tag.update(jobs_to_tag)

            for ofile in self.output_files.values():
                ofile.flush()
                if self.s3_success_file:
                    success_files.add(ofile.success_file)

            _LOG.info("Total Processed items for spider %s: %d", scrapername, self.itemcount)

        jcount = 0
        if self.args.test_mode:
            all_jobs_to_tag = set()
        for jkey in all_jobs_to_tag:
            self.add_job_tags(jkey, tags=['delivered'])
            jcount += 1
            if jcount % 100 == 0:
                _LOG.info("Marked %d jobs as delivered", jcount)

        for success_file in success_files:
            if not self.args.test_mode:
                _upload_file_to_s3(self.s3_bucket_name, success_file)
            _LOG.info("Created s3://%s/%s", self.s3_bucket_name, success_file)

        self.close(success_files)

    def close(self, success_files):
        pass

    def process_spider_jobs(self, scrapername):
        jobs_to_tag = []
        start = 0
        has_tag = [f"FLOW_ID={self.flow_id}"]
        jobs_count = 0

        while True:
            for spider_job in self.get_project().jobs.iter(spider=scrapername, state="finished", lacks_tag="delivered",
                                                           has_tag=has_tag, start=start):
                jobs_count += 1
                sj = self.get_project().jobs.get(spider_job['key'])
                self._process_job_items(scrapername, sj)
                jobs_to_tag.append(spider_job['key'])
            if jobs_count == 0 or jobs_count % 1000 != 0:
                break
            start += 1000

        return jobs_to_tag


if __name__ == '__main__':
    deliver = S3DeliverScript()
    deliver.run()
