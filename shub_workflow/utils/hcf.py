"""
Tools for handling a ScrapyCloud Hubstorage Crawl Frontier (HCF).

Requires the optional hcf-backend package:

    pip install shub-workflow[with-hcf-backend]
"""
import logging
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from shub_workflow.script import BaseScript

LOGGER = logging.getLogger(__name__)


def _import_hcf_backend():
    try:
        from hcf_backend.manager import HCFManager
        from hcf_backend.utils.hcfpal import HCFPal
    except ImportError:
        raise ImportError(
            "hcf-backend is required for using shub_workflow.utils.hcf. "
            "Install it with: pip install shub-workflow[with-hcf-backend]"
        )
    return HCFManager, HCFPal


class FrontierTool(BaseScript):
    """
    Command line tool for inspecting and handling the slots of a crawl frontier: list the slots, count, dump, add
    seeds, delete batches or slots, and move the seeds of a slot into another one.

    It can run as is, passing the frontier name by command line:

        python -m shub_workflow.utils.hcf --frontier myfrontier --list

    In a ScrapyCloud project, add a script (i.e. scripts/frontier_tool.py) that subclasses it in order to set the
    frontier name (and your project base script mixin, if any):

        class MyFrontierTool(FrontierTool):
            frontier_name = "myfrontier"

    A subclass can add its own commands by adding options in add_argparser_options() and overriding run(), calling
    super().run() for the rest, and reusing the handling methods (add_seeds(), move_slot(), etc.)

    HCF accepts a single writer per slot at a time. Don't add or move seeds into a slot while another process (i.e.
    a consumer) is writing to it.
    """

    # The name of the frontier. If not set, the command line option --frontier is required.
    frontier_name: Optional[str] = None
    # When adding seeds, flush them to the frontier each this number of seeds.
    add_seeds_flush_size: int = 1000
    # When moving seeds, read the source slot in chunks of at least this number of requests.
    move_read_size: int = 1000

    @property
    def description(self) -> str:
        return "Inspect and handle the slots of a crawl frontier."

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("slot", nargs="?", help="Target slot. If no command option is given, dump it.")
        self.argparser.add_argument(
            "--frontier",
            default=self.frontier_name,
            required=self.frontier_name is None,
            help="Frontier name." + (" Default: %(default)s" if self.frontier_name else ""),
        )
        self.argparser.add_argument("--list", action="store_true", help="List the frontier slots.")
        self.argparser.add_argument("--count", action="store_true", help="Count the requests in the slot.")
        self.argparser.add_argument(
            "--add-seed", "-a", action="append", default=[], help="Add a seed to the slot. Can be given multiple times."
        )
        self.argparser.add_argument(
            "--delete-batch",
            action="append",
            default=[],
            help="Delete a batch id from the slot. Can be given multiple times.",
        )
        self.argparser.add_argument("--delete-slot", action="store_true", help="Delete the slot.")
        self.argparser.add_argument(
            "--move", metavar="TARGET_SLOT", help="Move all seeds of the slot into the given target slot."
        )
        self.argparser.add_argument(
            "--max-seeds", type=int, default=100, help="Max number of seeds to dump. Default: %(default)s"
        )

    @property
    def hcf_manager(self):
        if getattr(self, "_hcf_manager", None) is None:
            HCFManager, _ = _import_hcf_backend()
            self._hcf_manager = HCFManager(self.args.frontier, project_id=self.project_id)
        return self._hcf_manager

    @property
    def hcfpal(self):
        if getattr(self, "_hcfpal", None) is None:
            _, HCFPal = _import_hcf_backend()
            self._hcfpal = HCFPal(project_id=self.project_id)
        return self._hcfpal

    def list_slots(self) -> List[str]:
        return self.hcfpal.get_slots(self.args.frontier)

    def count_slot(self, slot: str) -> int:
        return self.hcfpal.get_slot_count(self.args.frontier, slot)

    def add_seeds(self, slot: str, seeds: Iterable[str], qdata: Optional[Dict[str, Any]] = None) -> int:
        """
        Add the given seeds to the slot. Returns the number of seeds sent. Seeds whose fingerprint was already
        seen by the slot are discarded by the frontier.
        """
        count = 0
        for seed in seeds:
            self.hcf_manager.add_request(slot, {"fp": seed, "qdata": qdata or {}})
            count += 1
            if count % self.add_seeds_flush_size == 0:
                self.hcf_manager.flush(slot)
                LOGGER.info(f"Added {count} seeds to slot {slot!r}.")
        self.hcf_manager.flush(slot)
        LOGGER.info(f"Added {count} seeds to slot {slot!r}.")
        return count

    def delete_batches(self, slot: str, batch_ids: List[str]):
        self.hcf_manager.delete(slot, batch_ids)

    def delete_slot(self, slot: str):
        self.hcfpal.delete_slots(self.args.frontier, [slot])

    def move_slot(self, source_slot: str, target_slot: str) -> int:
        """
        Move all the seeds of the source slot, with their qdata, into the target slot. Each source batch is deleted
        only after its seeds were flushed into the target, so an interrupted move can be safely restarted. Seeds whose
        fingerprint was already seen by the target slot are discarded by the frontier. Returns the number of seeds read.
        """
        count = 0
        while batches := list(self.hcf_manager.read(source_slot, self.move_read_size)):
            for batch in batches:
                for fp, qdata in batch["requests"]:
                    self.hcf_manager.add_request(target_slot, {"fp": fp, "qdata": qdata or {}})
                    count += 1
                self.hcf_manager.flush(target_slot)
                self.hcf_manager.delete(source_slot, [batch["id"]])
            LOGGER.info(f"Moved {count} seeds from slot {source_slot!r} to slot {target_slot!r}.")
        return count

    def dump_slot(self, slot: str, max_seeds: int) -> Iterator[Tuple[str, str, Dict[str, Any]]]:
        """
        Yields (batch id, seed, qdata) tuples for the first max_seeds seeds of the slot, without removing them.
        """
        for batch_id, (fp, qdata) in self.hcfpal.dump_slot(self.args.frontier, slot, max_seeds):
            yield batch_id, fp, qdata

    def run(self):
        if self.args.list:
            for slot in self.list_slots():
                print(slot)
            return
        if not self.args.slot:
            self.argparser.error("Missing slot argument.")
        if self.args.count:
            print(self.count_slot(self.args.slot))
        elif self.args.add_seed:
            self.add_seeds(self.args.slot, self.args.add_seed)
        elif self.args.delete_batch:
            self.delete_batches(self.args.slot, self.args.delete_batch)
        elif self.args.delete_slot:
            self.delete_slot(self.args.slot)
        elif self.args.move:
            self.move_slot(self.args.slot, self.args.move)
        else:
            print(f"Dumping seeds on slot {self.args.slot!r}")
            for batch_id, seed, qdata in self.dump_slot(self.args.slot, self.args.max_seeds):
                if qdata:
                    print(batch_id, seed, qdata)
                else:
                    print(batch_id, seed)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    FrontierTool().run()
