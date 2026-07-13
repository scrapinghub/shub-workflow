"""
A Scrapy-Cloud-job-input issuer (the typical first stage: a "consumer" that reads raw crawl output).

Reads items from finished SC spider jobs (selected by the `target` CLI arg: spider:/canonical:/class:),
processes them, and writes batch files. Consumed jobs are tagged CONSUMED=True so they aren't re-read.
The base get_new_inputs() visits the matched spiders round-robin, rotating the starting spider across
loops, so when the target matches many spiders (e.g. class:MyBaseSpider, to consume every source in one
job) a source with a big backlog can't starve the others.

This is a DISCOVERY consumer, and it needs NO process_input() override:
- explode_input_items (a jmespath) explodes each scraped record into its bundled objects, so process_item()
  is called once per object — omit it for flat records;
- process_item() does the per-item work (here: collect discovery seeds) then calls super() to issue;
- post_process_input_items() runs once per job for side effects (here: push the job's seeds to a frontier).
"""
import logging
from typing import List, Set

from typing_extensions import NotRequired
from shub_workflow.issuer import IssuerScriptWithSCJobInput, IssuerItem, ItemId, InputSource


class MyItem(IssuerItem):
    url: str
    outlinks: NotRequired[List[str]]


class MyConsumer(IssuerScriptWithSCJobInput[MyItem]):

    loop_mode = 120
    output_folder = "gs://bucket/consumer-out"
    parallel_outputs = 10
    max_inputs_per_loop = 1_000
    # Each scraped record bundles a list of items under "items"; explode them so process_item() runs once per
    # item — no process_input() override needed. Omit this for flat records (one issued item per record).
    explode_input_items = "items"

    # Other knobs (see the skill): set_item_source (default True stamps item["source"] = scanned spider
    # canonical name; set False for a secondary spider whose items already carry their source), and
    # flush_on_each_input + a big default_filesize (one output file per job).

    def __init__(self):
        super().__init__()
        self._seeds: Set[str] = set()   # seeds discovered while reading the current job

    def build_item_id(self, item: MyItem) -> ItemId:
        return ItemId(item["url"])

    def process_item(self, item: MyItem, input_source: InputSource):
        # per-item work; item["source"] is already stamped by the base (set_item_source). Collect seeds, then
        # call super() so the item goes through preliminary dedup + issue to an output slot.
        self._seeds.update(item.get("outlinks", []))
        super().process_item(item, input_source)

    def post_process_input_items(self, spider_job, args):
        # per-job side effect (issues nothing, so flush_on_each_input stays False): push the job's discovered
        # seeds to the crawl frontier, then reset for the next job.
        self.write_seeds_to_frontier(self._seeds)
        self._seeds = set()

    def write_seeds_to_frontier(self, seeds: Set[str]):
        # push seeds to your crawl frontier (e.g. hcf_backend's HCFManager). Real consumers often do this on a
        # background thread so it doesn't block the read loop.
        ...


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    # invoke as:  python myconsumer.py class:MyBaseSpider --project-id=<id>
    MyConsumer().run()
