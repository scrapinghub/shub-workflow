import abc
import time
import json
import asyncio
import logging
from collections import defaultdict
from typing import Generator, List, Tuple, Optional, Protocol, Union, Type, Dict

from scrapinghub.client.jobs import Job
from scrapy import Item

from shub_workflow.script import BaseLoopScript, JobKey
from shub_workflow.deliver.dupefilter import SqliteDictDupesFilter, DupesFilterProtocol

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


class DeliverScriptProtocol(Protocol):

    DELIVERED_TAG: str
    SCRAPERNAME_NARGS: Union[str, int]

    @abc.abstractmethod
    def get_delivery_spider_jobs(
        self,
        scrapername: str,
        target_tags: List[str],
        only_finished: bool = True,
    ) -> Generator[Job, None, None]:
        ...

    @abc.abstractmethod
    def add_job_tags(self, jobkey: Optional[JobKey] = None, tags: Optional[List[str]] = None):
        ...

    @abc.abstractmethod
    def has_delivery_running_spider_jobs(self, scrapername: str, target_tags: List[str]) -> bool:
        ...


class BaseDeliverScript(BaseLoopScript, DeliverScriptProtocol):

    DELIVERED_TAG = "delivered"
    SCRAPERNAME_NARGS: Union[str, int] = "+"

    # print log every given items processed
    LOG_EVERY = 1000

    # minimal run time in order to ensure target jobs started to be scheduled
    MIN_RUN_TIME = 30

    # define here the fields used to deduplicate items. All them compose the dedupe key.
    # target item values must be strings.
    # for changing behavior, override is_seen_item()
    DEDUPE_KEY_BY_FIELDS: Tuple[str, ...] = ()

    MAX_PROCESSED_ITEMS = float("inf")

    SEEN_ITEMS_CLASS: Type[DupesFilterProtocol] = SqliteDictDupesFilter
    strict_max_time = False

    def __init__(self):
        super().__init__()
        self._all_jobs_to_tag = []
        self.total_items_count = 0
        self.total_dupe_filtered_items_count = 0
        self.seen_items: DupesFilterProtocol = self.SEEN_ITEMS_CLASS()
        self.seen_fields: Dict[str, int] = defaultdict(int)
        self.start_time = time.time()

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("scrapername", help="Target scraper names", nargs=self.SCRAPERNAME_NARGS)
        self.argparser.add_argument(
            "--test-mode",
            action="store_true",
            help="Run in test mode (performs all processes, but doesn't upload files nor consumes jobs)",
        )

    def get_target_tags(self) -> List[str]:
        """
        Return here additional tags (aside from FLOW_ID, which is automatically handled)
        that jobs must have in order to be included in delivery.
        """
        return []

    def get_delivery_spider_jobs(
        self, scrapername: str, target_tags: List[str], only_finished: bool = True
    ) -> Generator[Job, None, None]:
        if self.flow_id:
            flow_id_tag = [f"FLOW_ID={self.flow_id}"]
            target_tags = flow_id_tag + target_tags
        state = ["finished"]
        if not only_finished:
            state.append("running")
        yield from self.get_jobs_with_tags(scrapername, target_tags, state=state, lacks_tag=[self.DELIVERED_TAG])

    def has_delivery_running_spider_jobs(self, scrapername: str, target_tags: List[str]) -> bool:
        if self.flow_id:
            flow_id_tag = [f"FLOW_ID={self.flow_id}"]
            target_tags = flow_id_tag + target_tags
        for sj in self.get_jobs_with_tags(scrapername, target_tags, state=["running", "pending"]):
            if sj.key not in self._all_jobs_to_tag:
                return True
        return False

    def process_spider_jobs(self, scrapername: str, only_finished: bool = True) -> bool:
        if self.total_items_count >= self.MAX_PROCESSED_ITEMS:
            return False
        if self.strict_max_time and self.is_max_time_ran_out():
            return False
        target_tags = self.get_target_tags()
        for sj in self.get_delivery_spider_jobs(scrapername, target_tags, only_finished):
            if sj.key in self._all_jobs_to_tag:
                continue
            self.process_job_items(scrapername, sj)
            if not self.args.test_mode:
                self._all_jobs_to_tag.append(sj.key)
            if self.total_items_count >= self.MAX_PROCESSED_ITEMS:
                return False
            if self.strict_max_time and self.is_max_time_ran_out():
                return False
        return True

    def get_item_unique_key(self, item: Item) -> str:
        assert all(isinstance(item[f], str) for f in self.DEDUPE_KEY_BY_FIELDS)
        key = tuple(item[f] for f in self.DEDUPE_KEY_BY_FIELDS)
        return ",".join(key)

    def is_seen_item(self, item: Item) -> bool:
        key = self.get_item_unique_key(item)
        return key != "" and key in self.seen_items

    def add_seen_item(self, item: Item):
        key = self.get_item_unique_key(item)
        if key:
            self.seen_items.add(key)

    def process_job_items(self, scrapername: str, spider_job: Job):
        for item in spider_job.items.iter():
            if self.is_seen_item(item):
                self.total_dupe_filtered_items_count += 1
            else:
                self.on_item(item, scrapername)
                self.add_seen_item(item)
                for key, value in item.items():
                    if value:
                        self.seen_fields[key] += 1
            self.total_items_count += 1
            if self.total_items_count % self.LOG_EVERY == 0:
                _LOG.info(f"Processed {self.total_items_count} items.")

    def on_item(self, item: Item, scrapername: str):
        print(json.dumps(item))

    def workflow_loop(self) -> bool:
        for scrapername in self.args.scrapername:
            _LOG.info(f"Processing spider {scrapername}")
            if not self.process_spider_jobs(scrapername):
                return False
        if self.loop_mode:
            target_tags = self.get_target_tags()
            if self.has_delivery_running_spider_jobs(scrapername, target_tags):
                return True
            if time.time() - self.start_time < self.MIN_RUN_TIME:
                return True
        return False

    def close_files(self):
        pass

    def on_close(self):
        if self.loop_mode:
            for scrapername in self.args.scrapername:
                _LOG.info(f"Processing remaining spider {scrapername}")
                self.process_spider_jobs(scrapername, only_finished=False)
        self.close_files()
        jobs_count = len(self._all_jobs_to_tag)
        _LOG.info(f"Processed a total of {jobs_count} jobs.")
        _LOG.info(f"Processed a total of {self.total_items_count} items.")
        if self.DEDUPE_KEY_BY_FIELDS:
            _LOG.info(f"A total of {self.total_dupe_filtered_items_count} items were duplicated.")
        asyncio.run(self._tag_all())
        if hasattr(self.seen_items, "close"):
            self.seen_items.close()
        for key, value in self.seen_fields.items():
            self.stats.set_value(f"delivery/fields/count/{key}", value)
        self.stats.set_value("delivery/items/count", self.total_items_count)
        self.stats.set_value("delivery/items/duplicated", self.total_dupe_filtered_items_count)
        self.stats.set_value("delivery/jobs/count", jobs_count)

    async def _tag_all(self):
        while self._all_jobs_to_tag:
            to_tag, self._all_jobs_to_tag = self._all_jobs_to_tag[:1000], self._all_jobs_to_tag[1000:]
            cors = [self.async_add_job_tags(jkey, tags=[self.DELIVERED_TAG]) for jkey in to_tag]
            await asyncio.gather(*cors)
            _LOG.info("Marked %d jobs as delivered", len(to_tag))
