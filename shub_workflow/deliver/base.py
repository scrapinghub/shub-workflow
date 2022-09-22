import json
import logging
from uuid import uuid1
from typing import Set

from collection_scanner import CollectionScanner
from scrapinghub.client.exceptions import NotFound

from shub_workflow.script import BaseScript
from shub_workflow.deliver.dupefilter import SqliteDictDupesFilter

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


class BaseDeliverScript(BaseScript):

    DELIVERED_TAG = "delivered"
    SCRAPERNAME_NARGS = "+"

    # print log every given items processed
    LOG_EVERY = 1000

    # define here the fields used to deduplicate items. All them compose the dedupe key.
    # target item values must be strings.
    # for changing behavior, override is_seen_item()
    DEDUPE_KEY_BY_FIELDS = ()

    MAX_PROCESSED_ITEMS = float("inf")

    SEEN_ITEMS_CLASS = SqliteDictDupesFilter

    def __init__(self):
        super().__init__()
        self._all_jobs_to_tag = []
        self.total_items_count = 0
        self.total_dupe_filtered_items_count = 0
        self.seen_items = self.SEEN_ITEMS_CLASS()

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("scrapername", help="Target scraper names", nargs=self.SCRAPERNAME_NARGS)
        self.argparser.add_argument(
            "--test-mode",
            action="store_true",
            help="Run in test mode (performs all processes, but doesn't upload files nor consumes jobs)",
        )

    def get_target_tags(self):
        return []

    def get_delivery_spider_jobs(self, scrapername, target_tags):
        if self.flow_id:
            flow_id_tag = [f"FLOW_ID={self.flow_id}"]
            target_tags = flow_id_tag + target_tags
        yield from self.get_jobs_with_tags(scrapername, target_tags, state=["finished"], lacks_tag=[self.DELIVERED_TAG])

    def process_spider_jobs(self, scrapername):
        target_tags = self.get_target_tags()
        for sj in self.get_delivery_spider_jobs(scrapername, target_tags):
            self.process_job_items(scrapername, sj)
            if not self.args.test_mode:
                self._all_jobs_to_tag.append(sj.key)
            if self.total_items_count >= self.MAX_PROCESSED_ITEMS:
                break

    def get_item_unique_key(self, item):
        key = tuple(item[f] for f in self.DEDUPE_KEY_BY_FIELDS)
        return ",".join(key)

    def is_seen_item(self, item):
        key = self.get_item_unique_key(item)
        return key and key in self.seen_items

    def add_seen_item(self, item):
        key = self.get_item_unique_key(item)
        if key:
            self.seen_items.add(key)

    def process_job_items(self, scrapername, spider_job):
        for item in spider_job.items.iter():
            if self.is_seen_item(item):
                self.total_dupe_filtered_items_count += 1
            else:
                self.on_item(item, scrapername)
                self.add_seen_item(item)
            self.total_items_count += 1
            if self.total_items_count % self.LOG_EVERY == 0:
                _LOG.info(f"Processed {self.total_items_count} items.")

    def on_item(self, item, scrapername):
        print(json.dumps(item))

    def run(self):
        for scrapername in self.args.scrapername:
            _LOG.info(f"Processing spider {scrapername}")
            self.process_spider_jobs(scrapername)
        self.on_close()

    def on_close(self):
        _LOG.info(f"Processed a total of {self.total_items_count} items.")
        if self.DEDUPE_KEY_BY_FIELDS:
            _LOG.info(f"A total of {self.total_dupe_filtered_items_count} items were duplicated.")
        jcount = 0
        for jkey in self._all_jobs_to_tag:
            self.add_job_tags(jkey, tags=[self.DELIVERED_TAG])
            jcount += 1
            if jcount % 100 == 0:
                _LOG.info("Marked %d jobs as delivered", jcount)
        if hasattr(self.seen_items, "close"):
            self.seen_items.close()


class CachedDeliveredTagsMixin:

    """
    Usage:

    class MyDeliveryScript(CachedDeliveredTagsMixin, BaseDeliverScript):
        pass

    Under situations where you need speed and you have lots of jobs to deliver from,
    this mixin caches in a collection the delivered jobs instead of tagging them
    one by one.

    An external job scheduled from time to time may take care of tagging jobs while
    cleaning the collection. This is the recommended approach. Use the class
    SyncDeliveredScript for creating a script for that purpose.
    """

    TAGS_COLLECTION = "delivered"

    def __init__(self):
        super().__init__()
        self._cache = None
        self._to_store: Set = set()

    def get_delivered_cached(self):
        self._cache = set()
        try:
            for batch in CollectionScanner(
                collection_name=self.TAGS_COLLECTION,
                project_id=super().get_project().key,
            ).scan_collection_batches():
                for record in batch:
                    self._cache.update(record["data"])
        except NotFound:
            pass
        _LOG.info(f"Loaded {len(self._cache)} keys from delivered cache collection.")

    def add_job_tags(self, jobkey=None, tags=None):
        if jobkey is None or tags != [self.DELIVERED_TAG]:
            super().add_job_tags(jobkey, tags)
        else:
            if self._cache is None:
                self.get_delivered_cached()
            self._cache.add(jobkey)
            self._to_store.add(jobkey)

    def sync(self):
        if self._to_store:
            col = super().get_project().collections.get_store(self.TAGS_COLLECTION)
            to_store_all = list(self._to_store)
            self._to_store = set()
            to_store_count = len(to_store_all)
            while to_store_all:
                to_store, to_store_all = to_store_all[:100], to_store_all[100:]
                key = str(uuid1())
                record = {"_key": key, "data": list(to_store)}
                col.set(record)
            _LOG.info(f"Synced delivered cache ({to_store_count} jobs).")

    def get_delivery_spider_jobs(self, scrapername, target_tags):
        if self._cache is None:
            self.get_delivered_cached()
        for sj in super().get_delivery_spider_jobs(scrapername, target_tags):
            if sj.key not in self._cache:
                yield sj

    def on_close(self):
        super().on_close()
        self.sync()


class SyncDeliveredScript(BaseScript):

    TAGS_COLLECTION = "delivered"
    DELIVERED_TAG = "delivered"

    def run(self):
        store = self.get_project().collections.get_store(self.TAGS_COLLECTION)
        try:
            for batch in CollectionScanner(
                collection_name=self.TAGS_COLLECTION,
                project_id=super().get_project().key,
                meta=["_key"],
            ).scan_collection_batches():
                for record in batch:
                    for key in record["data"]:
                        self.add_job_tags(key, [self.DELIVERED_TAG])
                    store.delete([record["_key"]])
                    _LOG.info(f"Synced {len(record['data'])} jobs from {record['_key']}.")
        except NotFound:
            pass


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=logging.INFO)
    script = SyncDeliveredScript()
    script.run()
