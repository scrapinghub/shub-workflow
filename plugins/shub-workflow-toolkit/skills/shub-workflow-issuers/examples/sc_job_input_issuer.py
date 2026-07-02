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

    # --- IssuerScriptWithSCJobInput knobs (defaults shown; uncomment to change) ---
    # set_item_source = False        # default True: stamp item["source"] = scanned spider canonical name.
    #                                # Set False for a SECONDARY / post-processing spider whose items already
    #                                # carry their originating source (conserve it instead of overwriting).
    # flush_on_each_input = True     # default False: flush output at the end of EACH scanned job.
    # default_filesize = 10_000_000  # pair a big filesize with flush_on_each_input to guarantee ALL items
    #                                # of one scanned job land in the SAME output file.
    # To aggregate a scanned job's stats, mix in shub_workflow.utils.monitor.SpiderStatsAggregatorMixin and
    # call self.aggregate_spider_stats(...) from an overridden post_process_input_items() hook.

    def build_item_id(self, item: MyItem) -> ItemId:
        return ItemId(item["url"])

    # The base process_input() already reads a job's items and calls process_item() on each (stamping
    # item["source"] with the canonical spider name unless set_item_source is False). Override only to add
    # side effects, e.g. extracting seeds and writing them to the HCF frontier (discovery consumer pattern).


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    # invoke as:  python myconsumer.py canonical:example_source --project-id=<id>
    MyConsumer().run()
