#!/usr/bin/env python
"""
Generate deliverables

Usage:

    deliver.py <spider> [<spider> ... ][options]

For listing valid values for spider argument, just run deliver.py without arguments.

This script does the following:

* searches for untagged finished jobs that corresponds to the given spiders and same flow id as inherited or passed
  to the delivery job.
* gets all their items generated and uploads to customer s3 bucket, gs buckets or local folder, in files of given max
  size (either in items or bytes)
* tags processed jobs with 'delivered' tag, so they are not processed again (tag can be customized)

Deliver class is meant to be subclasses for overriding default behaviors, either via overriding of configuration class
attributes or instance methods.

configuration class attributes
------------------------------

DeliverScript.output_prefix - Target output prefix. Supports s3://..., gs:// and local absolute or relative folders
                              (required)
DeliverScript.success_file - A boolean. Whether or not to generate a finall _SUCCESS file
                             (optional, False by default)

Following environment variables are also required:

In case of using s3: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SH_APIKEY
In case of using gs: GOOGLE_APPLICATION_CREDENTIALS (you can use helper method
shub_workflow.deliver.gcstorage.set_credential_file_environ)

The default file format are gzipped json list. The default file name format will be:

<output prefix>/<spider name>/<formatted timestamp>/<file number>.jsonl.gz

The formatted timestamp corresponds to the time where the delivery script starts to run.

These defaults are only there for providing a fast deployable delivery script. But mosts project has its own
requirements, so the DeliverScript is designed for easy overriding of methods for very flexible customization.


"""
import json
import sys
import os
import gzip
import datetime
import logging
from tempfile import mktemp

from shub_workflow.deliver.futils import mv_file, touch
from shub_workflow.deliver.dupefilter import SqliteDictDupesFilter
from shub_workflow.deliver.base import BaseDeliverScript


_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


class OutputFile:

    max_filesize_items = 1000000
    max_filesize_bytes = 1000000000
    mv_file_kwargs = None
    gzipped_output = True

    def __init__(self, output_prefix, keyprefix, testmode=False):
        self.__output_prefix = output_prefix
        self.__key_prefix = keyprefix

        self.__filename = mktemp()
        self.__file = self.create_new_file()
        self.__filecount = 0
        self.__items_count = 0
        self.__size_bytes = 0
        self.__testmode = testmode

        spath = self.__key_prefix.split("/")[:-1]
        spath.append("_SUCCESS")
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
        if self.gzipped_output:
            return gzip.open(self.filename, "wb")
        return open(self.filename, "w")

    def write(self, item):
        line = json.dumps(item) + "\n"
        if self.gzipped_output:
            self.__file.write(line.encode("utf8"))
        else:
            self.__file.write(line)
        self.__items_count += 1
        self.__size_bytes += sys.getsizeof(line)
        if (
            self.max_filesize_items > 0
            and self.__items_count == self.max_filesize_items
            or self.max_filesize_bytes > 0
            and self.__size_bytes >= self.max_filesize_bytes
        ):
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
        destination = os.path.join(self.__output_prefix, keyname)
        if not self.__testmode:
            kwargs = self.mv_file_kwargs or {}
            mv_file(self.filename, destination, **kwargs)
        _LOG.info(f"Saved {self.__items_count} items in {destination}")


class OutputFileDict:

    outputfile_class = OutputFile

    def __init__(self, output_prefix, testmode=False):
        self.__output_prefix = output_prefix
        self.__testmode = testmode
        self.__outputfiles = {}

    def __getitem__(self, key):
        if key not in self.__outputfiles:
            self.__outputfiles[key] = self.outputfile_class(
                output_prefix=self.__output_prefix, keyprefix=key, testmode=self.__testmode
            )
        return self.__outputfiles[key]

    def values(self):
        return self.__outputfiles.values()

    def keys(self):
        return self.__outputfiles.keys()

    def pop(self, key):
        return self.__outputfiles.pop(key)


class DeliverScript(BaseDeliverScript):

    write_success_file = False
    output_prefix = ""
    outputfiledict_class = OutputFileDict

    default_sh_chunk_size = 1_000

    def __init__(self):
        super().__init__()

        self.output_files = self.set_output_files()
        self.__start_datetime = datetime.datetime.now()

        self.itemcount = 0
        self.filecount = 0
        self.dupes_filter = {i: SqliteDictDupesFilter() for i in self.args.filter_dupes_by_field}
        self.success_files = []

    @property
    def description(self):
        return __doc__

    @property
    def start_datetime(self):
        return self.__start_datetime

    def set_output_files(self):
        return self.outputfiledict_class(self.args.output_prefix, self.args.test_mode)

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("--output-prefix", help="Delivery prefix.", default=self.output_prefix)
        self.argparser.add_argument(
            "--filter-dupes-by-field",
            default=[],
            help="Dedupe by any of the given item field. Can be given multiple times",
        )
        self.argparser.add_argument("--one-file-per-job", action="store_true", help="Generate one file per job.")
        self.argparser.add_argument(
            "--sh-chunk-size",
            type=int,
            default=self.default_sh_chunk_size,
            help=(
                "Chunk/page size for downloading items from Scrapy Cloud. For tweaking memory consumption and speed."
                " Note that the performance will depend on the sizes of individual items in the cloud."
            ),
        )

    def gen_keyprefix(self, scrapername, job, item):
        formatted_datetime = self.start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        return os.path.join(scrapername, formatted_datetime)

    def process_job_items(self, scrapername, spider_job):
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

    def process_spider_jobs(self, scrapername):
        super().process_spider_jobs(scrapername)
        _LOG.info("Total Processed items for spider %s: %d", scrapername, self.itemcount)
        for ofile in self.output_files.values():
            ofile.flush()
            if self.write_success_file:
                self.success_files.add(ofile.success_file)

    def on_close(self):
        for success_file in self.success_files:
            remote_success_file = os.path.join(self.args.output_prefix, success_file)
            if not self.args.test_mode:
                touch(remote_success_file)
            _LOG.info(f"Created {remote_success_file}")
        super().on_close()


# for compatibility with older versions. New DeliverScript handles indistictly local files, s3 and gs.
class S3DeliverScript(DeliverScript):

    s3_bucket_name = None

    def __init__(self):
        self.output_prefix = self.s3_bucket_name
        super().__init__()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=("%(asctime)s [%(levelname)s] %(pathname)s:%(lineno)d %(message)s"))
    deliver = DeliverScript()
    deliver.run()
