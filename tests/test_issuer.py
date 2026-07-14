import os
import gzip
import json
import shutil
import tempfile
from typing import List, cast
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.utils.contexts import script_args
from shub_workflow.script import SpiderName
from shub_workflow.issuer import (
    IssuerScriptWithSCJobInput,
    IssuerScriptWithFileSystemInput,
    IssuerItem,
    ItemId,
    InputSource,
    Source,
)


# --------------------------------------------------------------------------- #
# Fakes for the Scrapy Cloud / spider-loader / filesystem boundary            #
# --------------------------------------------------------------------------- #

# spiders visible to the (patched) SpiderLoader; set per test.
FAKE_SPIDERS: dict = {}


class FakeBaseSpider:
    canonical_name = None
    seeds_fields = ()


def make_spider(name: str, canonical=None):
    return type(name, (FakeBaseSpider,), {"canonical_name": canonical})


class FakeSpiderLoader:
    def __init__(self, settings=None):
        pass

    def list(self):
        return list(FAKE_SPIDERS)

    def load(self, name):
        return FAKE_SPIDERS[name]


class FakeItems:
    def __init__(self, records):
        self._records = records

    def iter(self):
        return iter(self._records)


class FakeJob:
    def __init__(self, records, metadata=None, key="999/1/1"):
        self.items = FakeItems(records)
        self.metadata = metadata or {}
        self.key = key


class FakeFSHelper:
    """Minimal fshelper: serves configured input files (as gzip) and records fs side effects."""

    def __init__(self):
        self.files: dict = {}   # fname -> list of records
        self.removed: List[str] = []
        self.moved: List = []

    def exists(self, path):
        return False

    def list_path(self, prefix):
        return [f for f in self.files if f.startswith(prefix)]

    def list_folder(self, prefix):
        return self.list_path(prefix)

    def download_file(self, src, dst=None):
        dst = dst or os.path.basename(src)
        with gzip.open(dst, "wt") as w:
            for rec in self.files[src]:
                w.write(json.dumps(rec) + "\n")

    def rm_file(self, path):
        self.removed.append(path)

    def mv_file(self, src, dst):
        self.moved.append((src, dst))


class MyItem(IssuerItem):
    url: str


def issued_items(issuer):
    """All items currently enqueued for output (before any flush)."""
    out = []
    for _slot, sources in issuer.items_queue.items():
        for _source, by_id in sources.items():
            out.extend(by_id.values())
    return out


# --------------------------------------------------------------------------- #
# Test issuers                                                                #
# --------------------------------------------------------------------------- #


class RecordingSCIssuer(IssuerScriptWithSCJobInput[MyItem]):
    output_folder = "out"

    def __init__(self):
        super().__init__()
        self.written: List = []   # (destfile, [items])
        self.events: List = []    # ordered "process"/"post"/"write" markers

    def build_item_id(self, item) -> ItemId:
        return ItemId(item["url"])

    def process_item(self, item, input_source):
        self.events.append(("process", item["url"]))
        super().process_item(item, input_source)

    def post_process_input_items(self, spider_job, args):
        self.events.append(("post", None))

    def write_items_file(self, items, destfile) -> int:
        items = list(items)
        self.events.append(("write", len(items)))
        self.written.append((destfile, items))
        return len(items)


class ExplodingSCIssuer(RecordingSCIssuer):
    explode_input_items = "items"


class ConservingSCIssuer(RecordingSCIssuer):
    set_item_source = False


class PerJobFlushSCIssuer(RecordingSCIssuer):
    flush_on_each_input = True


class RotatingSCIssuer(IssuerScriptWithSCJobInput[MyItem]):
    output_folder = "out"

    def build_item_id(self, item) -> ItemId:
        return ItemId(item["url"])


class MergeSCIssuer(IssuerScriptWithSCJobInput[MyItem]):
    """Accumulate-then-merge delivery pattern: one merged record per job."""

    output_folder = "out"
    dedupe = False
    flush_on_each_input = True

    def __init__(self):
        super().__init__()
        self._acc: List = []
        self.written: List = []

    def build_item_id(self, item) -> ItemId:
        return ItemId("null")

    def process_item(self, item, input_source):
        self._acc.append(item)

    def post_process_input_items(self, spider_job, args):
        merged = {"url": "merged", "source": "s", "input_source": args[0]["key"], "count": len(self._acc)}
        merged["id"] = ItemId("merged")
        self.issue_item(cast(MyItem, merged))
        self._acc = []

    def write_items_file(self, items, destfile) -> int:
        items = list(items)
        self.written.append((destfile, items))
        return len(items)


class RecordingFSIssuer(IssuerScriptWithFileSystemInput[MyItem]):
    input_folder = "in"
    output_folder = "out"

    def __init__(self):
        super().__init__()
        self.written: List = []

    def init_fshelper(self):
        return FakeFSHelper()

    def build_item_id(self, item) -> ItemId:
        return ItemId(item["url"])

    def write_items_file(self, items, destfile) -> int:
        items = list(items)
        self.written.append((destfile, items))
        return len(items)


class ExplodingFSIssuer(RecordingFSIssuer):
    explode_input_items = "items"


def sc_args(spidername="a", canonical=None, key="999/1/1"):
    canonical = canonical or spidername
    spidercls = FAKE_SPIDERS.get(spidername) or make_spider("FakeBaseSpider")
    jdict = {"key": key, "spider_args": {}}
    return jdict, SpiderName(spidername), SpiderName(canonical), spidercls


class IssuerTestBase(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"
        global FAKE_SPIDERS
        FAKE_SPIDERS = {"a": make_spider("FakeBaseSpider")}
        # Start shared patches here (not as class decorators): class-level @patch decorators do not apply to
        # subclass test methods, so setUp is the reliable place to make them cover every test.
        for target in (
            patch("shub_workflow.script.BaseScript.get_sc_project_settings", new=lambda _: {}),
            patch("shub_workflow.script.SpiderLoader", new=FakeSpiderLoader),
        ):
            target.start()
            self.addCleanup(target.stop)
        self._cwd = os.getcwd()
        self._tmp = tempfile.mkdtemp()
        os.chdir(self._tmp)   # keep the livedup.bloom file out of the repo, isolated per test

    def tearDown(self):
        os.chdir(self._cwd)
        shutil.rmtree(self._tmp, ignore_errors=True)

    def make(self, cls, argv):
        with script_args(argv):
            return cls()


# --------------------------------------------------------------------------- #
# explode_input_record (pure)                                                 #
# --------------------------------------------------------------------------- #


class ExplodeInputRecordTest(IssuerTestBase):
    def test_no_explode_returns_record_as_single_item(self):
        issuer = self.make(RecordingSCIssuer, ["spider:a"])
        rec = {"url": "u1"}
        self.assertEqual(issuer.explode_input_record(rec), [rec])

    def test_explode_selects_the_list(self):
        issuer = self.make(ExplodingSCIssuer, ["spider:a"])
        rec = {"items": [{"url": "u1"}, {"url": "u2"}]}
        self.assertEqual(issuer.explode_input_record(rec), [{"url": "u1"}, {"url": "u2"}])

    def test_explode_missing_path_returns_empty(self):
        issuer = self.make(ExplodingSCIssuer, ["spider:a"])
        self.assertEqual(issuer.explode_input_record({"other": 1}), [])

    def test_explode_nested_jmespath(self):
        FAKE_SPIDERS["a"] = make_spider("FakeBaseSpider")

        class NestedIssuer(RecordingSCIssuer):
            explode_input_items = "data.images"

        issuer = self.make(NestedIssuer, ["spider:a"])
        rec = {"data": {"images": [{"url": "u1"}, {"url": "u2"}, {"url": "u3"}]}}
        self.assertEqual(len(issuer.explode_input_record(rec)), 3)


# --------------------------------------------------------------------------- #
# IssuerScriptWithSCJobInput.process_input                                     #
# --------------------------------------------------------------------------- #


@patch("shub_workflow.script.BaseScript.add_job_tags")
class SCJobInputProcessInputTest(IssuerTestBase):
    def test_basic_issue_and_source_stamped(self, _tags):
        issuer = self.make(RecordingSCIssuer, ["spider:a"])
        records = [{"url": "u1"}, {"url": "u2"}]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records)):
            issuer.process_input(InputSource("999/1/1"), sc_args(canonical="canonA"))
        items = issued_items(issuer)
        self.assertEqual(sorted(i["url"] for i in items), ["u1", "u2"])
        # set_item_source default True -> source stamped with the scanned canonical name
        self.assertEqual({i["source"] for i in items}, {"canonA"})
        self.assertEqual({i["input_source"] for i in items}, {"999/1/1"})

    def test_dedup_within_run(self, _tags):
        issuer = self.make(RecordingSCIssuer, ["spider:a"])
        records = [{"url": "dup"}, {"url": "dup"}, {"url": "unique"}]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records)):
            issuer.process_input(InputSource("999/1/1"), sc_args())
        items = issued_items(issuer)
        self.assertEqual(sorted(i["url"] for i in items), ["dup", "unique"])
        self.assertEqual(issuer.stats.get_value("urls/dupes"), 1)

    def test_set_item_source_false_conserves_source(self, _tags):
        issuer = self.make(ConservingSCIssuer, ["spider:a"])
        records = [{"url": "u1", "source": "upstream"}]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records)):
            issuer.process_input(InputSource("999/1/1"), sc_args(canonical="canonA"))
        self.assertEqual(issued_items(issuer)[0]["source"], "upstream")

    def test_explode_input_items(self, _tags):
        issuer = self.make(ExplodingSCIssuer, ["spider:a"])
        records = [
            {"items": [{"url": "u1"}, {"url": "u2"}]},
            {"items": [{"url": "u3"}]},
        ]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records)):
            issuer.process_input(InputSource("999/1/1"), sc_args())
        self.assertEqual(sorted(i["url"] for i in issued_items(issuer)), ["u1", "u2", "u3"])

    def test_send_file_triggered_by_default_filesize(self, _tags):
        issuer = self.make(RecordingSCIssuer, ["spider:a"])
        issuer.default_filesize = 2   # flush a slot when it reaches 2 items
        records = [{"url": f"u{i}"} for i in range(5)]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records)):
            issuer.process_input(InputSource("999/1/1"), sc_args())
        # 5 items, filesize 2 -> two files of 2 written mid-read, 1 left queued (not flushed yet)
        wrote = sum(len(items) for _, items in issuer.written)
        self.assertEqual(wrote, 4)
        self.assertEqual(len(issued_items(issuer)), 1)

    def test_flush_on_each_input(self, _tags):
        # NB: the two issuers share the per-test livedup.bloom (same cwd), so use distinct urls to avoid the
        # first issuer's items being seen as dupes by the second.
        default_issuer = self.make(RecordingSCIssuer, ["spider:a"])
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob([{"url": "u1"}, {"url": "u2"}])):
            default_issuer.process_input(InputSource("999/1/1"), sc_args())
        # default flush_on_each_input=False -> nothing written yet (still queued)
        self.assertEqual(default_issuer.written, [])
        self.assertEqual(len(issued_items(default_issuer)), 2)

        perjob_issuer = self.make(PerJobFlushSCIssuer, ["spider:a"])
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob([{"url": "u3"}, {"url": "u4"}])):
            perjob_issuer.process_input(InputSource("999/1/1"), sc_args())
        # flush_on_each_input=True -> written at end of the job
        self.assertEqual(sum(len(items) for _, items in perjob_issuer.written), 2)

    def test_post_process_runs_after_read_and_before_flush(self, _tags):
        issuer = self.make(PerJobFlushSCIssuer, ["spider:a"])
        records = [{"url": "u1"}, {"url": "u2"}]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records)):
            issuer.process_input(InputSource("999/1/1"), sc_args())
        kinds = [e[0] for e in issuer.events]
        self.assertEqual(kinds, ["process", "process", "post", "write"])

    def test_remove_inputs_tags_consumed(self, mocked_add_job_tags):
        issuer = self.make(RecordingSCIssuer, ["spider:a"])
        issuer.remove_inputs([InputSource("999/1/7")])
        # tags the consumed job with the CONSUMED tag (there may be other add_job_tags calls at startup).
        mocked_add_job_tags.assert_any_call("999/1/7", [issuer.CONSUMED_TAG])


# --------------------------------------------------------------------------- #
# get_new_inputs spider rotation (cross-loop fairness)                         #
# --------------------------------------------------------------------------- #


@patch("shub_workflow.script.BaseScript.get_jobs")
class GetNewInputsRotationTest(IssuerTestBase):
    def _five_spiders(self):
        global FAKE_SPIDERS
        FAKE_SPIDERS = {name: make_spider("FakeBaseSpider") for name in ["a", "b", "c", "d", "e"]}

    def test_rotates_starting_spider_across_loops(self, mocked_get_jobs):
        self._five_spiders()
        # each spider has a big backlog
        mocked_get_jobs.side_effect = lambda **kw: [{"key": f"{kw['spider']}/1/{i}"} for i in range(10)]
        issuer = self.make(RotatingSCIssuer, ["class:FakeBaseSpider"])

        firsts = []
        for _loop in range(6):
            first = None
            processed = 0
            for _jkey, args in issuer.get_new_inputs():
                if first is None:
                    first = args[1]
                processed += 1
                if processed == 3:   # simulate _issuer_workflow_loop breaking at max_inputs_per_loop
                    break
            firsts.append(first)
        self.assertEqual(firsts, ["a", "b", "c", "d", "e", "a"])

    def test_full_pass_visits_every_spider_once(self, mocked_get_jobs):
        self._five_spiders()
        mocked_get_jobs.side_effect = lambda **kw: [{"key": f"{kw['spider']}/1/0"}]
        issuer = self.make(RotatingSCIssuer, ["class:FakeBaseSpider"])
        spiders = [args[1] for _jkey, args in issuer.get_new_inputs()]
        self.assertEqual(sorted(spiders), ["a", "b", "c", "d", "e"])

    def test_target_class_matches_all_and_spider_matches_one(self, mocked_get_jobs):
        self._five_spiders()
        mocked_get_jobs.side_effect = lambda **kw: [{"key": f"{kw['spider']}/1/0"}]
        issuer = self.make(RotatingSCIssuer, ["spider:c"])
        spiders = [args[1] for _jkey, args in issuer.get_new_inputs()]
        self.assertEqual(spiders, ["c"])


# --------------------------------------------------------------------------- #
# scope_input_to_flow_id                                                       #
# --------------------------------------------------------------------------- #


class ScopedIssuer(RotatingSCIssuer):
    scope_input_to_flow_id = True


@patch("shub_workflow.script.BaseScript.get_jobs")
class ScopeInputToFlowIdTest(IssuerTestBase):
    def test_scopes_get_jobs_to_the_flow_id_tag(self, mocked_get_jobs):
        mocked_get_jobs.side_effect = lambda **kw: [{"key": "a/1/0"}]
        issuer = self.make(ScopedIssuer, ["spider:a", "--flow-id=f1"])
        list(issuer.get_new_inputs())
        _args, kwargs = mocked_get_jobs.call_args
        self.assertEqual(kwargs.get("has_tag"), ["FLOW_ID=f1"])

    def test_no_flow_id_tag_when_flag_unset(self, mocked_get_jobs):
        mocked_get_jobs.side_effect = lambda **kw: [{"key": "a/1/0"}]
        issuer = self.make(RotatingSCIssuer, ["spider:a", "--flow-id=f1"])   # flag defaults False
        list(issuer.get_new_inputs())
        _args, kwargs = mocked_get_jobs.call_args
        self.assertNotIn("has_tag", kwargs)

    def test_no_scope_without_flow_id(self, mocked_get_jobs):
        mocked_get_jobs.side_effect = lambda **kw: [{"key": "a/1/0"}]
        issuer = self.make(ScopedIssuer, ["spider:a"])   # flag set but no flow_id -> reads everything
        list(issuer.get_new_inputs())
        _args, kwargs = mocked_get_jobs.call_args
        self.assertNotIn("has_tag", kwargs)


# --------------------------------------------------------------------------- #
# separate_output_by_source                                                    #
# --------------------------------------------------------------------------- #


class NoSeparateIssuer(RecordingSCIssuer):
    set_item_source = False            # items already carry distinct sources
    separate_output_by_source = False


class SeparateIssuer(RecordingSCIssuer):
    set_item_source = False            # keep the per-record source (default separation)


@patch("shub_workflow.script.BaseScript.add_job_tags")
class SeparateOutputBySourceTest(IssuerTestBase):
    RECORDS = [{"url": "u1", "source": "s1"}, {"url": "u2", "source": "s2"}]

    def test_collapses_all_sources_into_one_slot_bucket(self, _tags):
        issuer = self.make(NoSeparateIssuer, ["spider:a"])
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(self.RECORDS)):
            issuer.process_input(InputSource("999/1/1"), sc_args())
        buckets = issuer.items_queue[None]           # parallel_outputs=1 -> slot None
        self.assertEqual(list(buckets.keys()), [""])  # single collapsed (empty) source bucket
        self.assertEqual(sorted(i["url"] for i in buckets[""].values()), ["u1", "u2"])

    def test_default_keeps_one_bucket_per_source(self, _tags):
        issuer = self.make(SeparateIssuer, ["spider:a"])
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(self.RECORDS)):
            issuer.process_input(InputSource("999/1/1"), sc_args())
        self.assertEqual(sorted(issuer.items_queue[None].keys()), ["s1", "s2"])

    def test_default_filename_drops_source_when_collapsed(self, _tags):
        issuer = self.make(NoSeparateIssuer, ["spider:a"])
        collapsed = os.path.basename(issuer.compute_destination_filename(None, Source(SpiderName(""))))
        sourced = os.path.basename(issuer.compute_destination_filename(None, Source(SpiderName("s1"))))
        self.assertTrue(sourced.startswith("s1_"))
        self.assertFalse(collapsed.startswith("s1_"))


# --------------------------------------------------------------------------- #
# Accumulate-then-merge (delivery pattern)                                     #
# --------------------------------------------------------------------------- #


@patch("shub_workflow.script.BaseScript.add_job_tags")
class AccumulateMergeTest(IssuerTestBase):
    def test_one_merged_record_per_job(self, _tags):
        issuer = self.make(MergeSCIssuer, ["spider:a"])
        records = [{"url": "u1"}, {"url": "u2"}, {"url": "u3"}]
        with patch("shub_workflow.script.BaseScript.get_job", return_value=FakeJob(records, key="999/1/5")):
            issuer.process_input(InputSource("999/1/5"), sc_args(key="999/1/5"))
        # flush_on_each_input=True -> the single merged record is written for this job
        self.assertEqual(len(issuer.written), 1)
        _destfile, items = issuer.written[0]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["count"], 3)
        self.assertEqual(items[0]["input_source"], "999/1/5")


# --------------------------------------------------------------------------- #
# IssuerScriptWithFileSystemInput                                              #
# --------------------------------------------------------------------------- #


class FileSystemInputTest(IssuerTestBase):
    def test_reads_batch_file_and_issues(self):
        issuer = self.make(RecordingFSIssuer, [])
        issuer.fshelper.files = {"in/0_x.jl.gz": [{"url": "u1", "source": "s"}, {"url": "u2", "source": "s"}]}
        issuer.process_input(InputSource("in/0_x.jl.gz"), ())
        self.assertEqual(sorted(i["url"] for i in issued_items(issuer)), ["u1", "u2"])

    def test_get_new_inputs_lists_input_folder(self):
        issuer = self.make(RecordingFSIssuer, [])
        issuer.fshelper.files = {"in/0_a.jl.gz": [], "in/0_b.jl.gz": [], "other/x.jl.gz": []}
        inputs = [fname for fname, _args in issuer.get_new_inputs()]
        self.assertEqual(sorted(inputs), ["in/0_a.jl.gz", "in/0_b.jl.gz"])

    def test_explode_on_file_input(self):
        issuer = self.make(ExplodingFSIssuer, [])
        issuer.fshelper.files = {
            "in/0_x.jl.gz": [{"items": [{"url": "u1", "source": "s"}, {"url": "u2", "source": "s"}]}]
        }
        issuer.process_input(InputSource("in/0_x.jl.gz"), ())
        self.assertEqual(sorted(i["url"] for i in issued_items(issuer)), ["u1", "u2"])

    def test_remove_inputs_deletes_when_no_processed_folder(self):
        issuer = self.make(RecordingFSIssuer, [])
        issuer.remove_inputs([InputSource("in/0_x.jl.gz")])
        self.assertEqual(issuer.fshelper.removed, ["in/0_x.jl.gz"])
