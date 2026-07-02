"""
Delivery built as an issuer (the modern replacement for the deprecated BaseDeliverScript).

There is NO dedicated delivery class — a delivery is just an IssuerScriptWithSCJobInput configured for
the terminal stage. Which knobs you set is USE-CASE DEPENDENT (see references/migrating-from-
basedeliverscript.md); this example delivers an already-post-processed pipeline:
- close_on_no_inputs = True — deliver the finished jobs, then stop (typical for delivery),
- flush_on_each_input = True + a big default_filesize — one delivery file per scanned job,
- dedupe = False — the data was ALREADY deduplicated upstream (a delivery reading a primary spider's
  crawl directly would instead keep dedupe=True),
- set_item_source = False — this reads a SECONDARY spider whose items already carry their originating
  source (a delivery of a PRIMARY spider would keep the default True),
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
