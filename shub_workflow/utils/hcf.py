"""
Tools for handling a ScrapyCloud Hubstorage Crawl Frontier (HCF).

Requires the optional hcf-backend package:

    pip install shub-workflow[with-hcf-backend]
"""
import json
import logging
from itertools import cycle
from typing import Any, Dict, Generator, Iterable, Iterator, List, Mapping, Optional, Tuple, TypedDict

from shub_workflow.crawl import GeneratorCrawlManager, JobParams, ScheduleArgs
from shub_workflow.script import BaseScript, JobDict, JobKey, Outcome, SpiderName

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


class SeedsSpiderConfig(TypedDict, total=False):
    """
    The configuration of a spider in HCFSeedsCrawlManager.ENABLED_SPIDERS. A project can subclass it in order to
    add its own keys.
    """

    # Max jobs in parallel. None or absent: read the ScrapyCloud project setting <SPIDER>_RUNNING_JOBS (default 1)
    # on every loop, so it can be changed without a deploy. 0 pauses the spider.
    RUNNING_JOBS: Optional[int]
    # Seeds passed to each job. Default 1.
    SEEDS_PER_JOB: int
    # ScrapyCloud units of each job. Default 1.
    SC_UNITS: int


class HCFSeedsCrawlManager(GeneratorCrawlManager):
    """
    Crawl manager that feeds a spider with seeds read from its slot of a crawl frontier: it reads seed batches from
    the slot, deletes them as it reads them, and schedules spider jobs with the seeds, in chunks of SEEDS_PER_JOB,
    passed as a json list in the spider argument seeds_argument. When the slot is empty the generator ends: the
    manager waits for its running jobs and closes, and a scheduler is expected to start it again later. One
    instance runs per spider, with the spider name as command line argument:

        py:crawlmanager.py <spider>

    Subclass it in your project in order to set ENABLED_SPIDERS (and your project base script mixin, if any):

        class MyCrawlManager(HCFSeedsCrawlManager):
            name = "crawlmanager"
            ENABLED_SPIDERS = {
                SpiderName("mysite.com_tags"): {"SEEDS_PER_JOB": 10, "RUNNING_JOBS": 4},
                SpiderName("mysite.com_authors"): {"SEEDS_PER_JOB": 100, "SC_UNITS": 2},
            }

    The slot of a spider is named after its canonical name and its seeds_source attribute (see get_base_slotname()).
    A source too big for a single writer can have its slot split in several ones (see SLOT_SPLITS): they are read in
    rotation. The other behaviors are customized by overriding the hooks: filter_out_seed(), process_seeds(),
    build_schedule_args(), requested_seeds(), on_empty_frontier().

    It is meant to be scheduled automatically, so it acquires the jobs left running by a previous instance of the
    same spider (acquire_all_jobs, with get_owned_jobs() narrowed to the target spider), and doesn't acquire finished
    jobs. Requires the optional hcf-backend package.
    """

    # The spiders that run with this manager, each one with its configuration. Starting the manager for a spider
    # that is not here is an error.
    ENABLED_SPIDERS: Mapping[SpiderName, SeedsSpiderConfig] = {}
    # Number of slots of a source (by canonical name), when its slot is split. Default 1 (not split).
    SLOT_SPLITS: Dict[str, int] = {}
    # The name of the frontier. If not set, it is read from the project setting FRONTIER_NAME.
    frontier_name: Optional[str] = None
    # The spider argument that receives the json list of seeds.
    seeds_argument: str = "seeds"
    # Close reasons of the spider jobs that stop the crawl (i.e. a suspended account): the manager stops scheduling
    # and closes once its running jobs finish.
    fatal_outcomes: Tuple[str, ...] = ()

    acquire_all_jobs = True
    dont_acquire_finished_jobs = True

    def __init__(self):
        super().__init__()
        assert self.spider is not None
        if self.spider not in self.ENABLED_SPIDERS:
            self.argparser.error(f"Spider {self.spider} is not enabled.")
        self.spider_config: SeedsSpiderConfig = self.ENABLED_SPIDERS[self.spider]
        for outcome in self.fatal_outcomes:
            if outcome not in self.failed_outcomes:
                self.failed_outcomes.append(outcome)
        self.__split_gen = cycle(range(self.get_slot_splits()))
        self.__hcf = None

    @property
    def hcf(self):
        if self.__hcf is None:
            HCFManager, _ = _import_hcf_backend()
            frontier = self.frontier_name or self.project_settings.get("FRONTIER_NAME")
            assert frontier, "Set the frontier_name attribute or the FRONTIER_NAME project setting."
            self.__hcf = HCFManager(frontier, project_id=self.project_id)
        return self.__hcf

    @property
    def seeds_per_job(self) -> int:
        return self.spider_config.get("SEEDS_PER_JOB", 1)

    @property
    def max_running_jobs(self) -> int:
        running_jobs = self.spider_config.get("RUNNING_JOBS")
        if running_jobs is None:
            assert self.spider is not None
            settings = self.get_sc_project_settings()
            running_jobs = int(settings.get(f"{self.spider.upper()}_RUNNING_JOBS", 1))
        return running_jobs

    def get_owned_jobs(self, project_id: Optional[int] = None, **kwargs) -> Generator[JobDict, None, None]:
        """
        Owns every job of the target spider, whoever scheduled it, and only of that spider: all the instances share
        the same name, so the name-based acquisition alone would adopt the jobs of the other instances.
        """
        yield from self.get_jobs(project_id, spider=self.spider, **kwargs)

    def get_base_slotname(self) -> str:
        """
        The slot of the target spider: "<canonical name>_<seeds_source>" if the spider class has a seeds_source
        attribute (the field the seeds that feed it come from), else the canonical name alone.
        """
        assert self.spider is not None
        canonical_name = self.get_canonical_spidername(self.spider)
        seeds_source = getattr(self.spider_loader.load(self.spider), "seeds_source", None)
        return f"{canonical_name}_{seeds_source}" if seeds_source else canonical_name

    def get_slot_splits(self) -> int:
        assert self.spider is not None
        return self.SLOT_SPLITS.get(self.get_canonical_spidername(self.spider), 1)

    def get_slotname(self, split: int = 0) -> str:
        """
        The name of the given split of the slot: the base slot name for split 0, with a "_<split>" suffix for the
        other ones. The writers of a split slot (typically, one consumer per split) must follow the same naming.
        """
        base = self.get_base_slotname()
        return base if split == 0 else f"{base}_{split}"

    def filter_out_seed(self, seed: str, qdata: Dict[str, Any]) -> bool:
        """
        Whether to discard a seed read from the frontier. By default, discards the ones whose qdata is marked as
        processed.
        """
        return bool(qdata.get("processed"))

    def requested_seeds(self) -> int:
        """
        The minimal number of seeds to read from the frontier on each read.
        """
        return self.seeds_per_job * max(self.max_running_jobs, 1)

    def read_seeds_from_slot(self, slot: str) -> List[str]:
        """
        Reads batches from the slot, deleting each one as it is read, until at least requested_seeds() seeds pass
        filter_out_seed(), or the slot is empty. The seeds of a job that fails are therefore lost, unless MAX_RETRIES
        is set.
        """
        seeds: List[str] = []
        requested = self.requested_seeds()
        while len(seeds) < requested:
            batches = list(self.hcf.read(slot, requested - len(seeds)))
            if not batches:
                break
            for batch in batches:
                for seed, qdata in batch["requests"]:
                    if self.filter_out_seed(seed, qdata or {}):
                        self.stats.inc_value(f"hcf/seeds/{slot}/filtered")
                        continue
                    seeds.append(seed)
                self.hcf.delete(slot, [batch["id"]])
                self.stats.inc_value(f"hcf/batches/{slot}/read")
        return seeds

    def read_seeds(self) -> List[str]:
        """
        Reads seeds from the slots of the spider in rotation, starting with the slot that follows the last one read:
        an empty slot is skipped, until one gives seeds or all of them were found empty.
        """
        for _ in range(self.get_slot_splits()):
            slot = self.get_slotname(next(self.__split_gen))
            if seeds := self.read_seeds_from_slot(slot):
                return seeds
            LOGGER.info(f"Slot {slot} is empty.")
        return []

    def process_seeds(self, seeds: List[str]) -> List[str]:
        """
        Transforms the seeds read from the frontier before they are chunked into jobs (i.e. combine each one with
        a list of keywords). By default, returns them unchanged.
        """
        return seeds

    def build_schedule_args(self, seeds: List[str]) -> ScheduleArgs:
        """
        The scheduling arguments of a job for the given seeds. Override it in order to add spider arguments, job
        settings or tags.
        """
        return ScheduleArgs({self.seeds_argument: json.dumps(seeds), "units": self.spider_config.get("SC_UNITS", 1)})

    def on_empty_frontier(self):
        """
        Called when all the slots of the spider were found empty, right before the generator ends (i.e. log an alert,
        or reset the slot and schedule a seed job).
        """
        LOGGER.info(f"No more seeds for spider {self.spider}.")

    def set_parameters_gen(self) -> Generator[ScheduleArgs, None, None]:
        while True:
            seeds = self.read_seeds()
            if not seeds:
                self.on_empty_frontier()
                break
            seeds = self.process_seeds(seeds)
            seeds_per_job = self.seeds_per_job
            for idx in range(0, len(seeds), seeds_per_job):
                yield self.build_schedule_args(seeds[idx: idx + seeds_per_job])

    def bad_outcome_hook(self, spider: SpiderName, outcome: Outcome, job_args_override: JobParams, jobkey: JobKey):
        if outcome in self.fatal_outcomes:
            LOGGER.error(f"Job {jobkey} finished with {outcome!r}. Stopping the crawl.")
            self.workflow_loop_enabled = False
        else:
            super().bad_outcome_hook(spider, outcome, job_args_override, jobkey)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    FrontierTool().run()
