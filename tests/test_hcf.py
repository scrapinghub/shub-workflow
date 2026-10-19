import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.utils.contexts import script_args
from shub_workflow.utils.hcf import FrontierToolMixin, _import_hcf_backend as real_import_hcf_backend


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


class FrontierTool(FrontierToolMixin):
    frontier_name = "myfrontier"


class NoNameFrontierTool(FrontierToolMixin):
    pass


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

    def run_tool(self, argv, cls=FrontierTool):
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
            self.run_tool(["--list"], cls=NoNameFrontierTool)
        tool, _ = self.run_tool(["--list", "--frontier", "other"], cls=NoNameFrontierTool)
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
            tool = FrontierTool()
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
            tool = FrontierTool()
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
