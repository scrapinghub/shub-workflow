"""
A file-input issuer (the common mid-pipeline stage: consumer / filter / deduplicator / balancer).

Reads gzipped JSON-lines batch files from an input folder, processes/dedups items, and writes batch
files to an output folder. You usually only implement build_item_id() and set a few attributes;
IssuerScriptWithFileSystemInput provides get_new_inputs / process_input / remove_inputs.
"""
import logging
from typing import Tuple

from shub_workflow.issuer import IssuerScriptWithFileSystemInput, IssuerItem, ItemId


class MyItem(IssuerItem):       # extend the item model with your fields
    url: str
    duration: int


class MyDeduplicator(IssuerScriptWithFileSystemInput[MyItem]):

    loop_mode = 120                       # continuous stage
    input_folder = "gs://bucket/stage-in"
    output_folder = "gs://bucket/stage-out"
    processed_folder = "gs://bucket/stage-in-processed"   # None => delete processed inputs instead
    parallel_outputs = 10                 # 10 hash-routed output slots (same id -> same slot)
    default_filesize = 100_000            # items per output batch file
    MAX_ITEMS = 1_000_000_000
    LOAD_DELIVERED_IDS_DAYS = 30          # dedup against the last 30 days of output

    def __init__(self):
        super().__init__()
        # REQUIRED when LOAD_DELIVERED_IDS_DAYS is set: refill the bloom filter from prior output(s).
        self.load_last_outputs((self.output_folder,))

    def build_item_id(self, item: MyItem) -> ItemId:
        return ItemId(item["url"])

    # Optional: filter/transform before the base dedup+enqueue.
    # def process_item(self, item: MyItem, input_source):
    #     if item["duration"] <= 240:
    #         super().process_item(item, input_source)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MyDeduplicator().run()
