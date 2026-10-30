import os
from io import StringIO
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.script import JobKey, Outcome, SpiderName
from shub_workflow.crawl import JobParams
from shub_workflow.utils.contexts import script_args
from shub_workflow.utils.hcf import (
    FrontierTool,
    HCFSeedsCrawlManager,
    SeedsSpiderConfig,
    _import_hcf_backend as real_import_hcf_backend,
)


class FakeFrontierStore:
    """In-memory frontier: slot -> list of batches ({"id", "requests": [[fp, qdata], ...]}), deduplicating by fp."""

    def __init__(self):
        self.batches: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.seen: Dict[str, set] = defaultdict(set)
        self.pending: Dict[str, List[Tuple[str, Dict]]] = defaultdict(list)
        self.flushes: List[str] = []
        self.next_id = 0

    def add(self, slot, fps):
        for fp, qdata in fps:
            if fp not in self.seen[slot]:
                self.seen[slot].add(fp)
                self.pending[slot].append((fp, qdata))

    def flush(self, slot):
        self.flushes.append(slot)
        if self.pending[slot]:
            self.next_id += 1
            self.batches[slot].append(
                {"id": f"b{self.next_id}", "requests": [[fp, qdata] for fp, qdata in self.pending.pop(slot)]}
            )

    def fps(self, slot):
        return [fp for batch in self.batches[slot] for fp, _ in batch["requests"]]


STORE = FakeFrontierStore()


class FakeHCFManager:
    def __init__(self, frontier, project_id=None):
        self.frontier = frontier
        self.project_id = project_id

    def add_request(self, slot, request):
        STORE.add(slot, [(request["fp"], request["qdata"])])

    def flush(self, slot=None):
        STORE.flush(slot)

    def read(self, slot, mincount=None):
        # like HCF, return whole batches until at least mincount requests, without removing them
        result, count = [], 0
        for batch in STORE.batches[slot]:
            if mincount is not None and count >= mincount:
                break
            result.append(batch)
            count += len(batch["requests"])
        return iter(result)

    def delete(self, slot, ids):
        STORE.batches[slot] = [b for b in STORE.batches[slot] if b["id"] not in ids]


class FakeHCFPal:
    def __init__(self, project_id=None):
        self.project_id = project_id

    def get_slots(self, frontier):
        return sorted(slot for slot, batches in STORE.batches.items() if batches)

    def get_slot_count(self, frontier, slot):
        return len(STORE.fps(slot))

    def delete_slots(self, frontier, slots):
        for slot in slots:
            STORE.batches.pop(slot, None)
            STORE.seen.pop(slot, None)

    def dump_slot(self, frontier, slot, max_requests):
        count = 0
        for batch in STORE.batches[slot]:
            for request in batch["requests"]:
                yield batch["id"], request
                count += 1
                if count == max_requests:
                    return


class MyFrontierTool(FrontierTool):
    frontier_name = "myfrontier"


class FrontierToolTest(TestCase):
    def setUp(self):
        global STORE
        STORE = FakeFrontierStore()
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"
        for p in (
            patch("shub_workflow.script.BaseScript.get_sc_project_settings", new=lambda _: {}),
            patch("shub_workflow.utils.hcf._import_hcf_backend", new=lambda: (FakeHCFManager, FakeHCFPal)),
        ):
            p.start()
            self.addCleanup(p.stop)

    def run_tool(self, argv, cls=MyFrontierTool):
        with script_args(argv):
            tool = cls()
        with patch("builtins.print") as mocked_print:
            tool.run()
        return tool, [" ".join(str(a) for a in c.args) for c in mocked_print.call_args_list]

    def test_frontier_from_attribute_and_option(self):
        tool, _ = self.run_tool(["--list"])
        self.assertEqual(tool.args.frontier, "myfrontier")
        self.assertEqual(tool.hcf_manager.frontier, "myfrontier")
        tool, _ = self.run_tool(["--list", "--frontier", "other"])
        self.assertEqual(tool.hcf_manager.frontier, "other")

    def test_frontier_option_required_without_attribute(self):
        with self.assertRaises(SystemExit), patch("sys.stderr"):
            self.run_tool(["--list"], cls=FrontierTool)
        tool, _ = self.run_tool(["--list", "--frontier", "other"], cls=FrontierTool)
        self.assertEqual(tool.args.frontier, "other")

    def test_add_seeds_and_list_count_dump(self):
        self.run_tool(["slot_a", "-a", "s1", "-a", "s2"])
        self.run_tool(["slot_b", "-a", "s3"])
        self.assertEqual(STORE.fps("slot_a"), ["s1", "s2"])

        _, out = self.run_tool(["--list"])
        self.assertEqual(out, ["slot_a", "slot_b"])
        _, out = self.run_tool(["slot_a", "--count"])
        self.assertEqual(out, ["2"])
        _, out = self.run_tool(["slot_a", "--max-seeds", "1"])
        self.assertEqual(out, ["Dumping seeds on slot 'slot_a'", "b1 s1"])

    def test_add_seeds_flushes_by_chunks(self):
        with script_args([]):
            tool = MyFrontierTool()
        tool.add_seeds_flush_size = 2
        count = tool.add_seeds("slot_a", ["s1", "s2", "s3", "s1"], qdata={"k": "v"})
        self.assertEqual(count, 4)
        # duplicated fingerprint discarded by the frontier, flushed each 2 seeds and at the end
        self.assertEqual(STORE.fps("slot_a"), ["s1", "s2", "s3"])
        self.assertEqual(len(STORE.batches["slot_a"]), 2)
        self.assertEqual(STORE.batches["slot_a"][0]["requests"][0], ["s1", {"k": "v"}])

    def test_missing_slot(self):
        with self.assertRaises(SystemExit), patch("sys.stderr"):
            self.run_tool(["-a", "s1"])

    def test_delete_batch_and_slot(self):
        self.run_tool(["slot_a", "-a", "s1"])
        self.run_tool(["slot_a", "-a", "s2"])
        self.run_tool(["slot_a", "--delete-batch", "b1"])
        self.assertEqual(STORE.fps("slot_a"), ["s2"])
        self.run_tool(["slot_a", "--delete-slot"])
        self.assertEqual(STORE.fps("slot_a"), [])

    def test_move_slot_keeps_qdata_and_empties_source(self):
        with script_args([]):
            tool = MyFrontierTool()
        tool.move_read_size = 2
        tool.add_seeds("source", ["s1"], qdata={"k": "v"})
        tool.add_seeds("source", ["s2", "s3"])
        tool.add_seeds("source", ["s4"])
        tool.add_seeds("target", ["s4"])
        self.assertEqual(tool.move_slot("source", "target"), 4)
        self.assertEqual(STORE.batches["source"], [])
        # s4 was already seen by the target slot
        self.assertEqual(STORE.fps("target"), ["s4", "s1", "s2", "s3"])
        self.assertEqual(STORE.batches["target"][1]["requests"], [["s1", {"k": "v"}]])

    def test_move_from_command_line(self):
        self.run_tool(["source", "-a", "s1", "-a", "s2"])
        self.run_tool(["source", "--move", "target"])
        self.assertEqual(STORE.fps("source"), [])
        self.assertEqual(STORE.fps("target"), ["s1", "s2"])

    def test_missing_hcf_backend(self):
        blocked = {"hcf_backend": None, "hcf_backend.manager": None, "hcf_backend.utils.hcfpal": None}
        with patch.dict("sys.modules", blocked), self.assertRaisesRegex(ImportError, "with-hcf-backend"):
            real_import_hcf_backend()


class FakeTagsSpider:
    name = "mysite_tags"
    canonical_name = "mysite"
    seeds_source = "tags"


class FakePlainSpider:
    name = "othersite"
    canonical_name = None


FAKE_SPIDERS = {"mysite_tags": FakeTagsSpider, "othersite": FakePlainSpider}


class FakeSpiderLoader:
    def __init__(self, settings=None):
        pass

    def list(self):
        return list(FAKE_SPIDERS)

    def load(self, name):
        return FAKE_SPIDERS[name]


class MySeedsCrawlManager(HCFSeedsCrawlManager):
    name = "crawlmanager"
    frontier_name = "myfrontier"
    fatal_outcomes = ("account_suspended",)
    ENABLED_SPIDERS = {
        SpiderName("mysite_tags"): SeedsSpiderConfig(SEEDS_PER_JOB=2, RUNNING_JOBS=2, SC_UNITS=3),
        SpiderName("othersite"): SeedsSpiderConfig(),
    }

    def filter_out_seed(self, seed, qdata):
        return seed.startswith("#")


class DefaultsSeedsCrawlManager(HCFSeedsCrawlManager):
    name = "crawlmanager"
    frontier_name = "myfrontier"
    ENABLED_SPIDERS = {SpiderName("othersite"): SeedsSpiderConfig()}


class HCFSeedsCrawlManagerTest(TestCase):
    def setUp(self):
        global STORE
        STORE = FakeFrontierStore()
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"
        self.sc_settings: Dict[str, str] = {}
        for p in (
            patch("shub_workflow.script.BaseScript.get_sc_project_settings", new=lambda _: self.sc_settings),
            patch("shub_workflow.script.BaseScript.get_jobs"),
            patch("shub_workflow.script.BaseScript.add_job_tags"),
            patch("shub_workflow.script.SpiderLoader", new=FakeSpiderLoader),
            patch("shub_workflow.utils.hcf._import_hcf_backend", new=lambda: (FakeHCFManager, FakeHCFPal)),
        ):
            p.start()
            self.addCleanup(p.stop)

    def make(self, spider, cls=MySeedsCrawlManager):
        with script_args([spider]):
            return cls()

    def add(self, slot, *batches):
        for batch in batches:
            STORE.add(slot, [(fp, {}) if isinstance(fp, str) else fp for fp in batch])
            STORE.flush(slot)

    def test_spider_not_enabled(self):
        FAKE_SPIDERS["disabled"] = FakePlainSpider
        self.addCleanup(FAKE_SPIDERS.pop, "disabled")
        with self.assertRaises(SystemExit), patch("sys.stderr"):
            self.make("disabled")

    def test_slot_names(self):
        manager = self.make("mysite_tags")
        self.assertEqual(manager.get_slotname(), "mysite_tags")
        self.assertEqual(manager.get_slotname(2), "mysite_tags_2")
        self.assertEqual(self.make("othersite").get_slotname(), "othersite")
        self.assertEqual(manager.get_slotname(1, SpiderName("othersite")), "othersite_1")

    def test_shared_slot(self):
        class FakeTagsVariantSpider(FakeTagsSpider):
            # a variant of the same source that misses its own seeds_source, so it inherits "tags"
            name = "mysite_users"

        FAKE_SPIDERS["mysite_users"] = FakeTagsVariantSpider
        self.addCleanup(FAKE_SPIDERS.pop, "mysite_users")

        # a variant that is not enabled (i.e. a periodic seeder) may share the slot
        self.assertEqual(self.make("mysite_tags").get_slotname(), "mysite_tags")

        class SharedManager(MySeedsCrawlManager):
            ENABLED_SPIDERS = {**MySeedsCrawlManager.ENABLED_SPIDERS, SpiderName("mysite_users"): SeedsSpiderConfig()}

        for spider in ("mysite_tags", "mysite_users"):
            with self.assertRaises(SystemExit), patch("sys.stderr", new_callable=StringIO) as stderr:
                self.make(spider, cls=SharedManager)
            self.assertIn("are both fed from slot(s) mysite_tags.", stderr.getvalue())
        # the managers of the other spiders run normally
        self.make("othersite", cls=SharedManager)

    def test_shared_split_slot(self):
        class SplitSlotSpider(FakePlainSpider):
            name = "mysite_tags_1"

        FAKE_SPIDERS["mysite_tags_1"] = SplitSlotSpider
        self.addCleanup(FAKE_SPIDERS.pop, "mysite_tags_1")

        class SplitManager(MySeedsCrawlManager):
            SLOT_SPLITS = {"mysite": 2}
            ENABLED_SPIDERS = {**MySeedsCrawlManager.ENABLED_SPIDERS, SpiderName("mysite_tags_1"): SeedsSpiderConfig()}

        manager = self.make("othersite", cls=SplitManager)
        self.assertEqual(manager.get_slotnames(SpiderName("mysite_tags")), ["mysite_tags", "mysite_tags_1"])
        with self.assertRaises(SystemExit), patch("sys.stderr", new_callable=StringIO) as stderr:
            self.make("mysite_tags", cls=SplitManager)
        self.assertIn(
            "Spiders mysite_tags and mysite_tags_1 are both fed from slot(s) mysite_tags_1.", stderr.getvalue()
        )

    def test_shared_slot_enabled_spider_not_found(self):
        class MissingManager(MySeedsCrawlManager):
            ENABLED_SPIDERS = {**MySeedsCrawlManager.ENABLED_SPIDERS, SpiderName("removed"): SeedsSpiderConfig()}

        with self.assertLogs("shub_workflow.utils.hcf", level="WARNING") as logs:
            self.make("mysite_tags", cls=MissingManager)
        self.assertIn("Enabled spider removed not found in project.", logs.output[0])

    def test_schedule_args_from_slot(self):
        self.add("mysite_tags", ["s1", "s2", "#s3"], ["s4", "s5"])
        manager = self.make("mysite_tags")
        params = list(manager.set_parameters_gen())
        self.assertEqual(
            params,
            [
                {"seeds": '["s1", "s2"]', "units": 3},
                {"seeds": '["s4", "s5"]', "units": 3},
            ],
        )
        self.assertEqual(STORE.batches["mysite_tags"], [])
        self.assertEqual(manager.stats.get_value("hcf/batches/mysite_tags/read"), 2)
        self.assertEqual(manager.stats.get_value("hcf/seeds/mysite_tags/filtered"), 1)

    def test_defaults(self):
        self.add("othersite", ["s1", ("#s2", {"processed": True})])
        manager = self.make("othersite", cls=DefaultsSeedsCrawlManager)
        self.assertEqual(
            list(manager.set_parameters_gen()), [{"seeds": '["s1"]', "units": 1}, {"seeds": '["#s2"]', "units": 1}]
        )
        self.assertIsNone(manager.stats.get_value("hcf/seeds/othersite/filtered"))

    def test_running_jobs_from_sc_settings(self):
        self.assertEqual(self.make("mysite_tags").max_running_jobs, 2)
        manager = self.make("othersite")
        self.assertEqual(manager.max_running_jobs, 1)
        self.sc_settings["OTHERSITE_RUNNING_JOBS"] = "0"
        self.assertEqual(manager.max_running_jobs, 0)

    def test_split_slots_rotation(self):
        class SplitManager(MySeedsCrawlManager):
            SLOT_SPLITS = {"mysite": 3}

        self.add("mysite_tags_1", ["a1", "a2"])
        self.add("mysite_tags_2", ["b1", "b2"])
        manager = self.make("mysite_tags", cls=SplitManager)
        # slot 0 is empty: moves on to slot 1; the next read starts at slot 2, then wraps around to an empty frontier
        self.assertEqual(manager.read_seeds(), ["a1", "a2"])
        self.assertEqual(manager.read_seeds(), ["b1", "b2"])
        self.assertEqual(manager.read_seeds(), [])

    def test_process_seeds_and_empty_frontier_hooks(self):
        class FanOutManager(MySeedsCrawlManager):
            empty_calls = 0

            def process_seeds(self, seeds):
                return [f"{s}|{k}" for s in seeds for k in ("k1", "k2")]

            def on_empty_frontier(self):
                self.empty_calls += 1

        self.add("mysite_tags", ["s1"])
        manager = self.make("mysite_tags", cls=FanOutManager)
        self.assertEqual(list(manager.set_parameters_gen()), [{"seeds": '["s1|k1", "s1|k2"]', "units": 3}])
        self.assertEqual(manager.empty_calls, 1)

    def test_fatal_outcome_stops_the_crawl(self):
        manager = self.make("mysite_tags")
        self.assertIn("account_suspended", manager.failed_outcomes)
        manager.workflow_loop_enabled = True
        manager.bad_outcome_hook(
            SpiderName("mysite_tags"), Outcome("account_suspended"), JobParams(), JobKey("999/1/1")
        )
        self.assertFalse(manager.workflow_loop_enabled)
