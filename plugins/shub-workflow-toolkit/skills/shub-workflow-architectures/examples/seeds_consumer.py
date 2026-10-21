"""
Consumer: the first issuer of the data chain, and the SINGLE WRITER of the frontier slots of the sources it reads.
It reads the finished spider jobs, writes the new seeds of each job into the frontier (before the job is tagged as
consumed: at-least-once), and partitions the items by id hash into NUM_SLOTS output slots for the deduplicators.

    py:consumer.py class:SeededSpider          # one consumer for all the sources
    py:consumer.py canonical:mysite.com        # or one per source, each writing only its own slots
"""
import logging
from collections import defaultdict
from typing import Dict, Set

from hcf_backend.manager import HCFManager
from shub_workflow.issuer import IssuerItem, IssuerScriptWithSCJobInput, ItemId

from myproject.common import CONSUMER_OUTPUT, NUM_SLOTS, get_slotname

LOGGER = logging.getLogger(__name__)

# every field that any spider may declare in its seeds_fields
SEEDS_FIELDS = ("tags", "authors")


class Article(IssuerItem):
    url: str
    tags: list
    authors: list


class Consumer(IssuerScriptWithSCJobInput[Article]):

    loop_mode = 120
    output_folder = CONSUMER_OUTPUT
    parallel_outputs = NUM_SLOTS
    # the scripts that schedule the consumed spiders (for the flush of the partial batches of stopped sources)
    source_crawlmanagers = ("py:crawlmanager.py",)

    def __init__(self):
        super().__init__()
        self.hcf = HCFManager(self.project_settings["FRONTIER_NAME"], project_id=self.project_id)
        self.job_seeds: Dict[str, Set[str]] = defaultdict(set)

    def build_item_id(self, item: Article) -> ItemId:
        return ItemId(f"{item['source']}:{item['url']}")

    def process_item(self, item: Article, input_source):
        for field in SEEDS_FIELDS:
            self.job_seeds[field].update(item.get(field) or [])
        # TODO: light post-processing that may change often belongs here (or in a filter), not in the deduplicators
        super().process_item(item, input_source)

    def post_process_input_items(self, spider_job, args):
        _, _, canonical_name, spidercls = args
        for field in getattr(spidercls, "seeds_fields", ()):
            if seeds := self.job_seeds.get(field):
                slot = get_slotname(canonical_name, field)
                for seed in seeds:
                    self.hcf.add_request(slot, {"fp": seed, "qdata": {}})
                self.hcf.flush(slot)
                LOGGER.info(f"Wrote {len(seeds)} seeds into slot {slot}.")
        self.job_seeds.clear()


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Consumer().run()
