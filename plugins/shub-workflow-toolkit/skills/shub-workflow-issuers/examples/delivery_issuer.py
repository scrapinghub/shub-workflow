"""
Delivery built as an issuer (the modern replacement for the deprecated BaseDeliverScript).

A final-stage issuer that writes the customer's delivery files. Its DEFINING traits are about being
the terminal stage of the pipeline, NOT about any particular item processing:
- dedupe = False (dedup already happened upstream),
- close_on_no_inputs = True (run to completion, not continuously),
- set_item_source = False (delivery reads a SECONDARY / post-processing spider, so each item already
  carries its originating source — conserve it),
- flush_on_each_input = True + a big default_filesize (so all of a job's items land in one delivery
  file, never pre-empted by a size split),
- output goes to the customer's delivery location (output_folder / compute_destination_filename).

By default it issues each read item as-is. If a particular delivery must COMBINE records before
writing them, it uses the generic accumulate-then-merge pattern (see merge_issuer.py) — that pattern
is NOT specific to delivery, so it is illustrated separately.
"""
import logging

from shub_workflow.issuer import IssuerScriptWithSCJobInput, IssuerItem, ItemId


class DeliverItem(IssuerItem):
    payload: dict


class DeliveryIssuer(IssuerScriptWithSCJobInput[DeliverItem]):

    name = "deliver"
    dedupe = False
    close_on_no_inputs = True
    set_item_source = False               # reads a secondary spider — keep each item's own "source"
    flush_on_each_input = True            # one delivery file per scanned job
    default_filesize = 10_000_000         # so the per-job flush is never split by size first
    output_folder = "gs://customer-bucket/delivery"

    def build_item_id(self, item: DeliverItem) -> ItemId:
        return ItemId(item["id"])

    # Each read item is issued as-is (default process_item) and written under output_folder with a
    # timestamped, source-prefixed name. Override compute_destination_filename() to control the delivery
    # path/name (e.g. derive it from the scanned job's spider args, available via post_process_input_items).


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    DeliveryIssuer().run()
