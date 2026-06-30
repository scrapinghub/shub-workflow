"""
Delivery built as an issuer (the modern replacement for the deprecated BaseDeliverScript).

A final-stage issuer: read a job's items, accumulate/merge them in memory, then issue the merged
records once per job and flush them to the customer's delivery location. Distinctive choices:
- dedupe = False (delivery doesn't dedup; that happened upstream),
- close_on_no_inputs = True (run to completion, not continuously),
- a very large default_filesize (keep a job's delivery in one file),
- process_item() overridden to accumulate (NOT call issue_item inline); merge + issue in process_input(),
- the output path/prefix usually comes from the spider job's args.
"""
import logging
from typing import Dict, List

from shub_workflow.issuer import IssuerScriptWithSCJobInput, IssuerItem, ItemId, Source


class DeliverItem(IssuerItem):
    payload: dict


class DeliveryIssuer(IssuerScriptWithSCJobInput[DeliverItem]):

    name = "deliver"
    dedupe = False
    close_on_no_inputs = True
    default_filesize = 10_000_000         # effectively "one file per source per job"

    def __init__(self):
        super().__init__()
        self._acc: Dict[Source, List[DeliverItem]] = {}

    def build_item_id(self, item: DeliverItem) -> ItemId:
        return ItemId("null")             # unused: process_item is overridden, no inline issuing

    def process_item(self, item: DeliverItem, input_source):
        # accumulate instead of issuing immediately
        self._acc.setdefault(item["source"], []).append(item)

    def process_input(self, jkey, args) -> bool:
        ok = super().process_input(jkey, args)     # reads the job's items -> process_item()
        for source, items in self._acc.items():
            merged = self._merge(items)            # your merge/validate/clean logic
            for rec in merged:
                rec["source"] = source
                rec["input_source"] = jkey
                self.issue_item(rec)               # now issue the finished records
        self.flush_files()                         # one delivery file per source for this job
        self._acc.clear()
        return ok

    def _merge(self, items: List[DeliverItem]) -> List[DeliverItem]:
        return items


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    DeliveryIssuer().run()
