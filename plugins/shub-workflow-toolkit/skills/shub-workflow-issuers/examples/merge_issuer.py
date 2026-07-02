"""
The accumulate-then-merge pattern for an IssuerScriptWithSCJobInput (GENERIC — not delivery-specific).

Some issuers must combine several related records read from a single input into fewer output records
(join, roll-up, reconcile, ...). Instead of issuing each raw record inline, process_item() ACCUMULATES
them; once the whole input has been read, the post_process_input_items() hook merges the accumulated
records and issue_item()s the combined ones, then resets the accumulator. Any issuer can use this —
a delivery issuer, a filter, a roll-up stage, etc. — so it is illustrated on its own here.

Example: a spider emits, per product, one "info" record plus several "image" records sharing a
product id; we want a single output record per product carrying its images.
"""
import logging
from collections import defaultdict
from typing import Dict, List

from shub_workflow.issuer import IssuerScriptWithSCJobInput, IssuerItem, ItemId, InputSource


class ProductItem(IssuerItem):
    product_id: str
    images: List[str]


class MergeIssuer(IssuerScriptWithSCJobInput[ProductItem]):

    loop_mode = 120
    output_folder = "gs://bucket/merged-out"
    flush_on_each_input = True            # optional: emit one output file per scanned job

    def __init__(self):
        super().__init__()
        # per-input accumulators; reset at the end of each input in post_process_input_items()
        self._info: Dict[str, ProductItem] = {}
        self._images: Dict[str, List[str]] = defaultdict(list)

    def build_item_id(self, item: ProductItem) -> ItemId:
        return ItemId(item["product_id"])

    def process_item(self, item: dict, input_source: InputSource):
        # accumulate instead of issuing inline (do NOT call super())
        pid = item["product_id"]
        if item.get("kind") == "image":
            self._images[pid].append(item["image_url"])
        else:
            self._info[pid] = ProductItem(product_id=pid, source=item["source"], images=[])

    def post_process_input_items(self, spider_job, args):
        # the whole input has been read: merge and issue one combined record per product. issue_item()
        # needs id / source / input_source set on the record; then reset the accumulators.
        # spider_job is the just-read job (its key is the input source; also args[0]["key"]).
        for pid, product in self._info.items():
            product["images"] = self._images.get(pid, [])
            product["id"] = self.build_item_id(product)
            product["input_source"] = InputSource(spider_job.key)
            self.issue_item(product)
        self._info.clear()
        self._images.clear()


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MergeIssuer().run()
