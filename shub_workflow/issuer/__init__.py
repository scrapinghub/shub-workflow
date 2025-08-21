import os
import re
import abc
import json
import time
import gzip
import hashlib
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import List, Optional, Dict, Set, NewType, Tuple, Iterable, Any, Generic, TypeVar, TypedDict, Union

import dateparser
from typing_extensions import NotRequired
from bloom_filter2 import BloomFilter
from shub_workflow.script import BaseLoopScript, SpiderName, JobKey
from shub_workflow.utils.dupefilter import DupesFilterProtocol


def hash_mod(text, divisor):
    """
    returns the module of dividing text md5 hash over given divisor
    """
    if isinstance(text, str):
        text = text.encode('utf8')
    md5 = hashlib.md5()
    md5.update(text)
    digest = md5.hexdigest()
    return int(digest, 16) % divisor


logging.getLogger("hcf_backend.manager").setLevel(logging.ERROR)
LOGGER = logging.getLogger(__name__)


Slot = NewType("Slot", str)

# sources in this list need to wait for crawler to be stopped, instead
# of reaching the given filesize
REAL_TIME_FILESIZE: int = 10000

TSTAMP_RE = re.compile(r"_\d{8}T\d{6}")

ItemId = NewType("ItemId", str)
Source = NewType("Source", str)
InputSource = NewType("InputSource", str)


class IssuerItem(TypedDict):
    # unique id
    id: ItemId
    # source crawler site
    source: Source
    # from where we are reading the item (i.e. job key or input file name)
    input_source: InputSource
    # search keywords from where the item cames from
    search_keywords: NotRequired[Set[str]]


ITEMTYPE = TypeVar("ITEMTYPE", bound=IssuerItem)


class MyJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)


class IssuerScript(BaseLoopScript, Generic[ITEMTYPE]):
    MAX_ITEMS: int = 200_000_000
    ERRORS_RATE: float = 1e-8
    default_filesize: int = 10_000
    output_folder: str
    parallel_outputs: int = 1
    input_slot: Optional[Slot] = None
    output_slot: Optional[Slot] = None
    dedupe = True
    min_wait_time_secs_to_flush_stopped_spiders: int = 0
    max_inputs_per_loop = -1

    def __init__(self):
        super().__init__()
        self.seen: DupesFilterProtocol = BloomFilter(
            max_elements=self.MAX_ITEMS, error_rate=self.ERRORS_RATE, filename=("livedup.bloom", -1)
        )
        if self.dedupe:
            LOGGER.info(f"Max capacity: {self.MAX_ITEMS}")
        self.items_queue: Dict[Union[Slot, None], Dict[Source, Dict[ItemId, ITEMTYPE]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self.pending_inputs_to_remove: Dict[InputSource, Set[Tuple[Union[Slot, None], Source]]] = defaultdict(set)
        self.processed_count = 0
        self.dupescounters: Dict[str, int] = Counter()
        self.totalcounters: Dict[str, int] = Counter()
        self.running_spiders_check_time: Dict[Source, int] = {}

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "--filesize",
            type=int,
            default=self.default_filesize,
            help=f"Output file size in urls. Default: {self.default_filesize}",
        )

    def process_item(self, item: ITEMTYPE, input_source: InputSource):
        """
        It must be called from process_input() (which you need to implement) for each item retrieved.
        """
        item["input_source"] = input_source
        canonical_name = item["id"].split(":", 1)[0]

        self.totalcounters[canonical_name] += 1

        if item["id"] in self.seen:
            self.stats.inc_value("urls/dupes")
            self.dupescounters[canonical_name] += 1

        if not self.dedupe or item["id"] not in self.seen:
            self.issue_item(item)
            self.seen.add(item["id"])
            self.stats.inc_value("urls/issued")
            self.stats.inc_value("urls/seen")
            self.stats.inc_value(f"urls/seen/{canonical_name}")
            self.stats.inc_value(f"urls/issued/{canonical_name}")

        self.processed_count += 1

    def get_output_slot_for_item(self, item: ITEMTYPE) -> Union[Slot, None]:
        return (
            Slot(str(hash_mod(item["id"], self.parallel_outputs)))
            if self.output_slot is None and self.parallel_outputs > 1
            else self.output_slot
        )

    def get_filesize_from_item(self, item: ITEMTYPE) -> int:
        if item.get("filters_file_hash"):
            return REAL_TIME_FILESIZE
        return self.args.filesize

    def issue_item(self, item: ITEMTYPE):
        slot = self.get_output_slot_for_item(item)
        self.enqueue_item(item, slot)
        filesize = self.get_filesize_from_item(item)
        if len(self.items_queue[slot][item["source"]]) >= filesize:
            self.send_file(slot, item["source"])

    def enqueue_item(self, item: ITEMTYPE, slot: Union[Slot, None]):
        input_source: InputSource = item["input_source"]
        self.add_item_to_queue(item, slot)
        self.pending_inputs_to_remove[input_source].add((slot, item["source"]))

    def add_item_to_queue(self, item: ITEMTYPE, slot: Union[Slot, None]):
        if item["id"] not in self.items_queue[slot][item["source"]] or "search_keywords" not in item:
            self.items_queue[slot][item["source"]][item["id"]] = item
        else:
            self.items_queue[slot][item["source"]][item["id"]]["search_keywords"].update(item["search_keywords"])

    def write_items_file(self, items: List[ITEMTYPE], destfile: str) -> int:
        count = 0
        with gzip.open("itemsout.jl.gz", "wt") as fz:
            for im in items:
                fz.write(json.dumps(im, cls=MyJsonEncoder) + "\n")
                count += 1
        self.fshelper.mv_file("itemsout.jl.gz", destfile)
        LOGGER.info(f"Wrote {count} records to {destfile}")
        return count

    def send_file(self, output_slot: Union[Slot, None], source: Source):
        destname = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%f")
        if self.input_slot is not None:
            destname = f"{destname}_{self.input_slot}"

        if output_slot is not None:
            destfile = os.path.join(self.output_folder, f"{output_slot}_{source}_{destname}.jl.gz")
        else:
            destfile = os.path.join(self.output_folder, f"{source}_{destname}.jl.gz")

        count = self.write_items_file(list(self.items_queue[output_slot][source].values()), destfile)
        self.stats.inc_value("urls/wrote", count)
        self.stats.inc_value(f"urls/{source}/wrote", count)
        if output_slot is not None:
            self.stats.inc_value(f"urls/{output_slot}/{source}/wrote", count)
        self.items_queue[output_slot][source] = {}

        for slots_sources in self.pending_inputs_to_remove.values():
            slots_sources.discard((output_slot, source))

    def on_close(self):
        super().on_close()
        self.seen.close()
        for slot, sources in self.items_queue.items():
            for source in list(sources.keys()):
                if len(sources[source]) > 0:
                    self.send_file(slot, source)
        to_remove = [k for k, v in self.pending_inputs_to_remove.items() if not v]
        self.remove_inputs(to_remove)

    @abc.abstractmethod
    def get_new_inputs(self) -> Iterable[Tuple[InputSource, Tuple[Any, ...]]]:
        """
        - Implement here the logic to generate all inputs.
        - Returns an iterable of 2-tuples, containing the source itself and a tuple of positional arguments
          in order to be passed to process_input (can be empty).
        """

    @abc.abstractmethod
    def process_input(self, inputsrc: InputSource, *args) -> bool:
        """
        iterate over items in the input and call self.process_item() for each one.
        Returns True if the input has been succesfully processed
        """

    def _issuer_workflow_loop(self) -> int:
        """
        Iterates over the items for each one, and call self.process_input()
        for each of them.
        Returns number of new inputs processed.
        """
        new_inputs_count = 0
        for inputsrc, args in self.get_new_inputs():
            if self.process_input(inputsrc, *args):
                new_inputs_count += 1
            if inputsrc not in self.pending_inputs_to_remove:  # input with no new items
                self.remove_inputs([inputsrc])
            if new_inputs_count == self.max_inputs_per_loop:
                break
        return new_inputs_count

    def workflow_loop(self) -> bool:
        new_inputs = self._issuer_workflow_loop()
        if new_inputs:
            self.stats.inc_value("inputs/processed", new_inputs)
            for name, total in self.totalcounters.items():
                self.stats.set_value(
                    f"urls/dupesrate/{name}",
                    round(self.dupescounters[name] / total, 2),
                )
            total_dupes = 0
            for spidername, count in self.dupescounters.items():
                canonical_name = self.get_canonical_spidername(SpiderName(spidername))
                if spidername == canonical_name:
                    total_dupes += count
            if self.processed_count:
                self.stats.set_value("urls/dupesrate", round(total_dupes / self.processed_count, 2))
        else:
            LOGGER.info(f"No new input. Total urls read: {self.processed_count}")
            total_pending = 0
            for slot, sources_items_dict in self.items_queue.items():
                for source, items_dict in sources_items_dict.items():
                    pending_for_source = len(items_dict.keys())
                    LOGGER.info(f"'{slot}/{source}' urls pending to be wrote: {pending_for_source}")
                    total_pending += pending_for_source
            LOGGER.info(f"Total urls pending to be wrote: {total_pending}")

        to_remove = [k for k, v in self.pending_inputs_to_remove.items() if not v]
        self.remove_inputs(to_remove)
        for iname in to_remove:
            self.pending_inputs_to_remove.pop(iname)

        now = int(time.time())
        for slot, sources_items_dict in self.items_queue.items():
            for source in list(sources_items_dict.keys()):
                if len(sources_items_dict[source]) > 0:
                    running_spiders = self.get_project_running_spiders(
                        canonical=True, crawlmanagers=("py:crawlmanager.py",)
                    )
                    if source not in running_spiders:
                        delay = now - self.running_spiders_check_time.setdefault(source, now)
                        if delay > self.min_wait_time_secs_to_flush_stopped_spiders:
                            self.send_file(slot, source)
                    else:
                        self.running_spiders_check_time.pop(source, None)

        if self.processed_count < self.MAX_ITEMS:
            return True
        return False

    @abc.abstractmethod
    def remove_inputs(self, inputs: List[InputSource]):
        ...


class IssuerScriptWithFileSystemInput(IssuerScript[ITEMTYPE]):

    # The folder where to find the input files.
    input_folder: str

    # The folder where to move processed input files.
    # If None (default), input files will be removed instead.
    processed_folder: Union[None, str] = None
    LOAD_DELIVERED_IDS_DAYS: int

    def get_new_inputs(self) -> Iterable[Tuple[InputSource, Tuple[Any, ...]]]:
        if self.input_slot is not None:
            fprefix = os.path.join(self.input_folder, f"{self.input_slot}_")
        else:
            fprefix = os.path.join(self.input_folder, "")
        LOGGER.info(f"Reading {fprefix}")
        for fname in self.fshelper.list_path(fprefix):
            if fname in self.pending_inputs_to_remove:
                continue
            yield fname, ()

    def process_input(self, fname: InputSource, *args) -> bool:
        self.fshelper.download_file(fname, "itemsin.jl.gz")
        with gzip.open("itemsin.jl.gz", "rt") as fz:
            count = 0
            for line in fz:
                item = json.loads(line)
                self.process_item(item, fname)
                count += 1
            LOGGER.info(f"Read {count} records from {fname}.")
        return True

    def remove_inputs(self, inputs: List[InputSource]):
        for iname in inputs:
            if self.processed_folder is None:
                self.fshelper.rm_file(iname)
                self.stats.inc_value("inputs/removed")
            else:
                self.fshelper.mv_file(iname, os.path.join(self.processed_folder, os.path.basename(iname)))
                self.stats.inc_value("inputs/moved")

    def load_last_outputs(self, output_folders: Tuple[str, ...], prefix: str = "", basename_re: Optional[str] = None):
        """
        Load ids from the last LOAD_DELIVERED_IDS_DAYS days
        """
        dtnow = datetime.utcnow()
        count = 0
        for output_folder in output_folders:
            output_folder = os.path.join(output_folder, prefix)
            LOGGER.info(f"Reading output folder {output_folder!r}...")
            for fname in self.fshelper.list_path(output_folder):
                basename = os.path.basename(fname)
                if basename_re is None or re.match(basename_re, basename) is not None:
                    if m := TSTAMP_RE.search(basename):
                        tstamp = m.group()
                        dt = dateparser.parse(tstamp, date_formats=("_%Y%m%dT%H%M%S",))
                        if dt is None or dtnow - dt > timedelta(days=self.LOAD_DELIVERED_IDS_DAYS):
                            continue
                        try:
                            self.fshelper.download_file(fname)
                        except Exception:
                            LOGGER.info(f"{fname} is not anymore there.")
                            continue
                        with gzip.open(basename) as r:
                            for line in r:
                                rec = json.loads(line)
                                uid = rec["id"]
                                self.stats.inc_value(f"urls/seen/{rec['source']}")
                                self.seen.add(uid)
                                self.stats.inc_value("urls/seen")
                                count += 1
                        self.fshelper.rm_file(basename)
        LOGGER.info(f"Loaded {count} seen ids.")


class IssuerScriptWithSCJobInput(IssuerScript[ITEMTYPE]):

    CONSUMED_TAG = "CONSUMED=True"

    def __init__(self):
        self._target_type: Union[str, None] = None
        self._target_name: Union[str, None] = None
        super().__init__()

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "target",
            help="Which spiders to target. In format <type>:<name>, where type can be spider, canonical or class."
        )

    def parse_args(self):
        args = super().parse_args()
        try:
            self._target_type, self._target_name = args.target.split(":")
        except ValueError:
            self.argparser.error("Wrong target format.")
        assert self._target_type in ("spider", "canonical", "class"), f"Invalid target type: {self._target_type}"
        return args

    def get_new_inputs(self) -> Iterable[Tuple[InputSource, Tuple[Any, ...]]]:
        for spidername in self.spider_loader.list():
            canonical_name = self.get_canonical_spidername(SpiderName(spidername))
            spidercls = self.spider_loader.load(spidername)
            if self._target_type == "spider" and spidername == self._target_name or \
                    self._target_type == "canonical" and canonical_name == self._target_name or \
                    self._target_type == "class" and self._target_name in [c.__name__ for c in spidercls.mro()]:
                for jdict in self.get_jobs(
                    spider=spidername, state=["finished"], meta=["spider_args"], lacks_tag=self.CONSUMED_TAG
                ):
                    yield InputSource(jdict["key"]), (jdict, spidername, canonical_name, spidercls)

    def remove_inputs(self, jobkeys: List[InputSource]):
        count = 0
        for count, jk in enumerate(jobkeys, start=1):
            self.add_job_tags(JobKey(jk), [self.CONSUMED_TAG])
            self.stats.inc_value("inputs/tagged")
            if count % 100 == 0:
                LOGGER.info(f"Tagged {count} input jobs.")
        LOGGER.info(f"Tagged a total of {count} input jobs.")
