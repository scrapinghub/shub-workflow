import json
import logging

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
