"""
A Scrapy-Cloud-job-input issuer (the typical first stage: a "consumer" that reads raw crawl output).

Reads items from finished SC spider jobs (selected by the `target` CLI arg: spider:/canonical:/class:),
processes them, and writes batch files. Consumed jobs are tagged CONSUMED=True so they aren't re-read.
You implement build_item_id(); IssuerScriptWithSCJobInput provides input discovery/consumption.
"""
import logging

from shub_workflow.issuer import IssuerScriptWithSCJobInput, IssuerItem, ItemId


class MyItem(IssuerItem):
    url: str


class MyConsumer(IssuerScriptWithSCJobInput[MyItem]):

    loop_mode = 120
    output_folder = "gs://bucket/consumer-out"
    parallel_outputs = 10
    max_inputs_per_loop = 1_000           # cap jobs read per cycle

    def build_item_id(self, item: MyItem) -> ItemId:
        return ItemId(item["url"])

    # The base process_input() already reads a job's items and calls process_item() on each, stamping
    # item["source"] with the canonical spider name. Override only to add side effects, e.g. extracting
    # seeds and writing them to the HCF frontier (see the discovery consumer pattern).


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    # invoke as:  python myconsumer.py canonical:example_source --project-id=<id>
    MyConsumer().run()
