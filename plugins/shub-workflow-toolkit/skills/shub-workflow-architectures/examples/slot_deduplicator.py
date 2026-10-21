"""
Persistent deduplicator of one slot of the consumer output. One instance per slot, running continuously:

    py:deduplicator.py <slot>

On start it refills its bloom filter with the ids written in the last LOAD_DELIVERED_IDS_DAYS days to its own output
and to the downstream folders (where items go after it), so an id already passed along is never issued again. When
it reaches MAX_ITEMS it stops by itself and the keep-alive scheduler starts a fresh one.

The same shape serves for filters (override process_item(), call super() only for the items that pass), balancers
(no input_slot; get_filesize_from_item() per source) and reducers (close_on_no_inputs).
"""
import logging

from shub_workflow.issuer import IssuerItem, IssuerScriptWithFileSystemInput, ItemId, Slot

from myproject.common import CONSUMER_OUTPUT, DEDUP_OUTPUT, DOWNLOADING

LOGGER = logging.getLogger(__name__)


class Article(IssuerItem):
    url: str


class Deduplicator(IssuerScriptWithFileSystemInput[Article]):

    loop_mode = 300
    input_folder = CONSUMER_OUTPUT  # TODO: the filters output, if there are filters (filters go BEFORE dedup)
    output_folder = DEDUP_OUTPUT
    default_filesize = 10_000
    MAX_ITEMS = 1_000_000_000  # TODO: size it from the expected volume per slot
    ERRORS_RATE = 1e-9
    LOAD_DELIVERED_IDS_DAYS = 365  # TODO: how far back an item must stay deduplicated

    DOWNSTREAM_FOLDERS = (DOWNLOADING,)  # TODO: every folder items go to after this stage

    def __init__(self):
        super().__init__()
        self.input_slot = self.output_slot = Slot(self.args.slot)
        self.load_last_outputs((self.output_folder,) + self.DOWNSTREAM_FOLDERS, prefix=f"{self.output_slot}_")

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("slot", help="Slot to process.")

    def build_item_id(self, item: Article) -> ItemId:
        return ItemId(item["id"])  # already computed by the consumer


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Deduplicator().run()
