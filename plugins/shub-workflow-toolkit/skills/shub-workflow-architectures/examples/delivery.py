"""
Delivery: an issuer configured for the terminal stage. This one reads the finished jobs of a spider stage (e.g. the
downloader) and writes one delivery file per job, then stops. Run it as a periodic job:

    py:deliver.py spider:downloader

To deliver the output of a deduplicator instead, use IssuerScriptWithFileSystemInput with dedupe = False. For the
full delivery configuration and the migration of BaseDeliverScript, see the shub-workflow-issuers skill.
"""
import logging
import os

from shub_workflow.issuer import IssuerItem, IssuerScriptWithSCJobInput, ItemId

from myproject.common import DELIVERY

LOGGER = logging.getLogger(__name__)


class Record(IssuerItem):
    url: str


class Deliver(IssuerScriptWithSCJobInput[Record]):

    output_folder = DELIVERY
    close_on_no_inputs = True  # deliver what is available, then stop
    flush_on_each_input = True  # one file per input job, consumed right after it is written
    default_filesize = 10_000_000  # ... not split by size
    set_item_source = False  # the downloader items already carry the source of the crawl
    dedupe = False  # deduplicated upstream
    # TODO: CONSUMED_TAG = "delivered" when replacing a delivery that tagged its jobs so, or everything is re-delivered

    def build_item_id(self, item: Record) -> ItemId:
        return ItemId(item["url"])

    def process_input(self, input_source, args):
        self.current_job = input_source.replace("/", "_")
        return super().process_input(input_source, args)

    def compute_destination_filename(self, output_slot, source) -> str:
        # keep the batch naming standard: a source designation before the rest
        return os.path.join(self.output_folder, f"{source}_{self.current_job}.jl.gz")

    # TODO (optional, only if the spider stage fails often): in post_process_input_items(), write the records with
    # recoverable errors back into the input queue of the spider stage as retry batches


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Deliver().run()
