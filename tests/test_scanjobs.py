import os
import json
import time
import shutil
import tempfile
from typing import Any, Dict, List, Optional
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.utils.contexts import script_args
from shub_workflow.utils.scanjobs import ScanJobs


NOW_MS = int(time.time() * 1000)
HOUR_MS = 3600 * 1000


def make_jobdict(key: str, hours_ago: float = 1, spider: str = "myspider", **meta) -> Dict[str, Any]:
    finished = NOW_MS - int(hours_ago * HOUR_MS)
    jobdict: Dict[str, Any] = {
        "key": key,
        "spider": spider,
        "finished_time": finished,
        "running_time": finished - 60000,
    }
    jobdict.update(meta)
    return jobdict


class FakeLogs:
    def __init__(self, lines: List[Dict[str, Any]]):
        self.lines = lines

    def iter(self):
        return iter(self.lines)


class FakeItems:
    def __init__(self, items: List[Dict[str, Any]]):
        self.items = items

    def iter(self, meta=None, startts=None):
        return iter(self.items)


class FakeJob:
    def __init__(self, logs: Optional[List[Dict[str, Any]]] = None, items: Optional[List[Dict[str, Any]]] = None):
        self.logs = FakeLogs(logs or [])
        self.items = FakeItems(items or [])


class ScanJobsTest(TestCase):
    """
    Runs ScanJobs end to end over fake jobs, capturing the datapoints written with --write.
    """

    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"
        self.tmpdir = tempfile.mkdtemp()
        self.jobs: List[Dict[str, Any]] = []
        self.job_contents: Dict[str, FakeJob] = {}
        self.get_jobs_kwargs: Dict[str, Any] = {}

        def get_jobs(**kwargs):
            self.get_jobs_kwargs = kwargs
            return iter(self.jobs)

        for p in (
            patch("shub_workflow.script.BaseScript.get_sc_project_settings", new=lambda _: {}),
            patch("shub_workflow.script.BaseScript.get_jobs", side_effect=get_jobs),
            patch(
                "shub_workflow.script.BaseScript.get_job",
                side_effect=lambda key: self.job_contents.get(key, FakeJob()),
            ),
        ):
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def scan(self, *argv: str, write: bool = True) -> List[Any]:
        """
        Runs the scan with the given arguments and returns the written datapoints.
        """
        outfile = os.path.join(self.tmpdir, "out.jl")
        args = list(argv) + ["-z", "UTC"]
        if write:
            args += ["-w", outfile]
        with script_args(args):
            script = ScanJobs()
        script.run()
        if not write:
            return []
        script.args.write.close()
        with open(outfile) as f:
            return [json.loads(line) if line.startswith(("{", "[")) else line.strip() for line in f]

    # job selection

    def test_scans_only_jobs_inside_period(self):
        self.jobs = [
            make_jobdict("1/1/3", hours_ago=1, scrapystats={"mystat": 3}),
            make_jobdict("1/1/2", hours_ago=10, scrapystats={"mystat": 2}),
            make_jobdict("1/1/1", hours_ago=30, scrapystats={"mystat": 1}),
        ]
        result = self.scan("myspider", "-s", "mystat", "--data-headers", "mystat")
        self.assertEqual([r["mystat"] for r in result], [3, 2])

    def test_end_time_skips_newer_jobs(self):
        self.jobs = [
            make_jobdict("1/1/2", hours_ago=1, scrapystats={"mystat": 2}),
            make_jobdict("1/1/1", hours_ago=5, scrapystats={"mystat": 1}),
        ]
        result = self.scan("myspider", "-s", "mystat", "--data-headers", "mystat", "-e", "3 hours ago")
        self.assertEqual([r["mystat"] for r in result], [1])

    def test_finished_jobs_only_by_default(self):
        self.scan("myspider")
        self.assertEqual(self.get_jobs_kwargs["state"], ["finished"])
        self.assertEqual(self.get_jobs_kwargs["spider"], "myspider")
        self.scan("*", "--include-running-jobs")
        self.assertEqual(self.get_jobs_kwargs["state"], ["finished", "running"])
        self.assertIsNone(self.get_jobs_kwargs["spider"])

    def test_spiders_only_and_scripts_only(self):
        self.jobs = [
            make_jobdict("1/1/2", spider="myspider", scrapystats={"mystat": 2}),
            make_jobdict("1/2/1", spider="py:myscript.py", scrapystats={"mystat": 1}),
        ]
        result = self.scan("*", "--spiders-only", "-s", "mystat", "--data-headers", "mystat")
        self.assertEqual([r["mystat"] for r in result], [2])
        result = self.scan("*", "--scripts-only", "-s", "mystat", "--data-headers", "mystat")
        self.assertEqual([r["mystat"] for r in result], [1])

    def test_tag_pattern(self):
        self.jobs = [
            make_jobdict("1/1/2", tags=["FLOW_ID=abc"], scrapystats={"mystat": 2}),
            make_jobdict("1/1/1", tags=["OTHER"], scrapystats={"mystat": 1}),
        ]
        result = self.scan("myspider", "--tag-pattern", "^FLOW_ID=", "-s", "mystat", "--data-headers", "mystat")
        self.assertEqual([r["mystat"] for r in result], [2])
        self.assertIn("tags", self.get_jobs_kwargs["meta"])

    def test_has_tag_is_passed_to_get_jobs(self):
        self.scan("myspider", "--has-tag", "DAILY")
        self.assertEqual(self.get_jobs_kwargs["has_tag"], ["DAILY"])

    def test_argument_pattern_spider(self):
        self.jobs = [
            make_jobdict("1/1/2", spider_args={"source": "youtube"}, scrapystats={"mystat": 2}),
            make_jobdict("1/1/1", spider_args={"source": "tiktok"}, scrapystats={"mystat": 1}),
        ]
        result = self.scan(
            "myspider", "-a", "source:you", "-s", "mystat", "--data-headers", "mystat", "--capture-spiderargs"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["mystat"], 2)
        self.assertEqual(result[0]["source"], "youtube")

    def test_argument_pattern_script(self):
        self.jobs = [
            make_jobdict("1/2/2", spider="py:myscript.py", job_cmd=["myscript.py", "--slot=3"]),
            make_jobdict("1/2/1", spider="py:myscript.py", job_cmd=["myscript.py", "--slot=4"]),
        ]
        result = self.scan("py:myscript.py", "-a", "slot=3")
        self.assertEqual(result, ["https://app.zyte.com/p/1/2/2/stats"])

    def test_no_patterns_writes_job_links(self):
        self.jobs = [make_jobdict("1/1/2"), make_jobdict("1/1/1")]
        result = self.scan("myspider")
        self.assertEqual(result, ["https://app.zyte.com/p/1/1/2/stats", "https://app.zyte.com/p/1/1/1/stats"])

    # stats extraction

    def test_stats_pattern_groups_and_value(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"ipType/datacenter": 10, "records_read": 3, "other": 1})]
        result = self.scan("myspider", "-s", "ipType/(.+)", "-s", "records_read")
        self.assertEqual(len(result), 1)
        tstamp, *groups = result[0]
        self.assertEqual(groups, ["datacenter", "10", "3"])

    def test_stats_safe_default(self):
        self.jobs = [
            make_jobdict("1/1/2", scrapystats={"a": 1, "b": 2}),
            make_jobdict("1/1/1", scrapystats={"a": 3}),
        ]
        result = self.scan("myspider", "-s", "^a$", "-s", "^b$", "-d", "b", "--data-headers", "a,b")
        self.assertEqual([(r["a"], r["b"]) for r in result], [(1, 2), (3, 0)])

    def test_stats_separate_matches_per_target(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"status/200": 8, "status/500": 2})]
        result = self.scan(
            "myspider", "-s", "status/(.+)", "--separate-matches-per-target", "--data-headers", "status,count"
        )
        self.assertEqual([(r["status"], r["count"]) for r in result], [(200, 8), (500, 2)])

    def test_data_headers_auto(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"a": 1, "b": 2})]
        result = self.scan("myspider", "-s", "(a|b)", "--data-headers", "auto", "--capture-joblink")
        self.assertEqual(result[0]["a"], 1)
        self.assertEqual(result[0]["b"], 2)
        self.assertEqual(result[0]["job_link"], "https://app.zyte.com/p/1/1/1/stats")
        self.assertIn("tstamp", result[0])

    def test_post_process(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"errors": 5, "requests": 20})]
        result = self.scan("myspider", "-s", "^errors$", "-s", "^requests$", "-c", "div", "--data-headers", "rate")
        self.assertEqual(result[0]["rate"], 0.25)

    def test_post_process_zero_division_drops_datapoint(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"errors": 5, "requests": 0})]
        result = self.scan("myspider", "-s", "^errors$", "-s", "^requests$", "-c", "div", "--data-headers", "rate")
        self.assertEqual(result, [])

    # log and item extraction

    def test_log_pattern_groups(self):
        self.jobs = [make_jobdict("1/1/1")]
        self.job_contents["1/1/1"] = FakeJob(
            logs=[
                {"time": NOW_MS - HOUR_MS, "message": "Wrote 10 records"},
                {"time": NOW_MS - HOUR_MS, "message": "nothing here"},
                {"time": NOW_MS - HOUR_MS, "message": "Wrote 20 records"},
            ]
        )
        result = self.scan("myspider", "-l", r"Wrote (\d+)", "--data-headers", "count")
        self.assertEqual([r["count"] for r in result], [10, 20])

    def test_log_pattern_count_and_max_items(self):
        self.jobs = [make_jobdict("1/1/1")]
        self.job_contents["1/1/1"] = FakeJob(
            logs=[{"time": NOW_MS - HOUR_MS, "message": f"Wrote {i} records"} for i in range(5)]
        )
        result = self.scan("myspider", "-l", r"Wrote (\d+)", "--count", "--max-items-per-job", "3")
        self.assertEqual([r[1:] for r in result], [[1], [1], [1]])

    def test_log_pattern_skips_lines_before_period(self):
        self.jobs = [make_jobdict("1/1/1")]
        self.job_contents["1/1/1"] = FakeJob(
            logs=[
                {"time": NOW_MS - 30 * HOUR_MS, "message": "Wrote 10 records"},
                {"time": NOW_MS - HOUR_MS, "message": "Wrote 20 records"},
            ]
        )
        result = self.scan("myspider", "-l", r"Wrote (\d+)", "--data-headers", "count")
        self.assertEqual([r["count"] for r in result], [20])

    def test_first_match_only(self):
        self.jobs = [make_jobdict("1/1/1")]
        self.job_contents["1/1/1"] = FakeJob(
            logs=[{"time": NOW_MS - HOUR_MS, "message": f"Wrote {i} records"} for i in range(3)]
        )
        result = self.scan("myspider", "-l", r"Wrote (\d+)", "--first-match-only", "--data-headers", "count")
        self.assertEqual([r["count"] for r in result], [0])

    def test_hold_combines_log_and_stats(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"requests": 40})]
        self.job_contents["1/1/1"] = FakeJob(logs=[{"time": NOW_MS - HOUR_MS, "message": "Wrote 10 records"}])
        result = self.scan(
            "myspider", "-l", r"Wrote (\d+)", "-s", "^requests$", "-c", "hold div", "--data-headers", "rate"
        )
        self.assertEqual([r["rate"] for r in result], [0.25])

    def test_item_field_pattern(self):
        self.jobs = [make_jobdict("1/1/1")]
        self.job_contents["1/1/1"] = FakeJob(
            items=[
                {"_ts": NOW_MS - HOUR_MS, "source": "youtube_42"},
                {"_ts": NOW_MS - HOUR_MS, "source": "tiktok_7"},
                {"_ts": NOW_MS - HOUR_MS, "other": "x"},
            ]
        )
        result = self.scan("myspider", "-i", r"source:youtube_(\d+)", "--data-headers", "num")
        self.assertEqual([r["num"] for r in result], [42])
        result = self.scan("myspider", "-i", "source:", "--data-headers", "source")
        self.assertEqual([r["source"] for r in result], ["youtube_42", "tiktok_7"])

    # job metadata extraction (--meta-pattern)

    def test_meta_pattern_fields_are_requested(self):
        self.scan("myspider", "-m", "close_reason:", "-m", "version:^abc")
        self.assertIn("close_reason", self.get_jobs_kwargs["meta"])
        self.assertIn("version", self.get_jobs_kwargs["meta"])

    def test_meta_pattern_census(self):
        self.jobs = [
            make_jobdict("1/1/3", close_reason="finished", version="abc-main"),
            make_jobdict("1/1/2", close_reason="memusage_exceeded", version="abc-main"),
            make_jobdict("1/1/1", close_reason="killed by oom", version="def-main"),
        ]
        result = self.scan("myspider", "-m", "close_reason:", "-m", "version:", "--data-headers", "cr,version")
        self.assertEqual(
            [(r["cr"], r["version"]) for r in result],
            [("finished", "abc-main"), ("memusage_exceeded", "abc-main"), ("killed by oom", "def-main")],
        )

    def test_meta_pattern_filters_jobs(self):
        self.jobs = [
            make_jobdict("1/1/3", close_reason="finished"),
            make_jobdict("1/1/2", close_reason="memusage_exceeded"),
            make_jobdict("1/1/1", close_reason="killed by oom"),
        ]
        result = self.scan("myspider", "-m", "close_reason:memusage|oom", "--data-headers", "cr")
        self.assertEqual([r["cr"] for r in result], ["memusage_exceeded", "killed by oom"])

    def test_meta_pattern_all_patterns_must_match(self):
        self.jobs = [
            make_jobdict("1/1/2", close_reason="finished", version="abc-main"),
            make_jobdict("1/1/1", close_reason="finished", version="def-main"),
        ]
        result = self.scan("myspider", "-m", "close_reason:finished", "-m", "version:^def", "--data-headers", "cr,v")
        self.assertEqual([r["v"] for r in result], ["def-main"])

    def test_meta_pattern_captures_first_group(self):
        self.jobs = [make_jobdict("1/1/1", version="015273e-main")]
        result = self.scan("myspider", "-m", "version:^([0-9a-f]+)-", "--data-headers", "commit")
        self.assertEqual(result[0]["commit"], "015273e")

    def test_meta_pattern_missing_field_excludes_job(self):
        self.jobs = [make_jobdict("1/1/2", units=2), make_jobdict("1/1/1")]
        result = self.scan("myspider", "-m", "units:", "--data-headers", "units")
        self.assertEqual([r["units"] for r in result], [2])

    def test_meta_pattern_non_string_value(self):
        self.jobs = [make_jobdict("1/1/2", units=1), make_jobdict("1/1/1", units=2)]
        result = self.scan("myspider", "-m", "units:^1$")
        self.assertEqual([r[1:] for r in result], [[1]])

    def test_meta_pattern_prepended_to_stats(self):
        self.jobs = [make_jobdict("1/1/1", close_reason="finished", scrapystats={"memusage/max": 512})]
        result = self.scan("myspider", "-m", "close_reason:", "-s", "memusage/max", "--data-headers", "cr,mem")
        self.assertEqual(result, [{"cr": "finished", "mem": 512, "tstamp": result[0]["tstamp"]}])

    def test_meta_pattern_record_for_job_without_stats(self):
        # a job killed before saving any stats still generates a datapoint with its metadata
        self.jobs = [
            make_jobdict("1/1/2", close_reason="finished", scrapystats={"memusage/max": 512}),
            make_jobdict("1/1/1", close_reason="killed by oom"),
        ]
        result = self.scan("myspider", "-m", "close_reason:", "-s", "memusage/max", "--data-headers", "cr,mem")
        self.assertEqual([(r["cr"], r.get("mem")) for r in result], [("finished", 512), ("killed by oom", None)])

    def test_meta_pattern_with_data_headers_auto(self):
        self.jobs = [make_jobdict("1/1/1", close_reason="finished", scrapystats={"memusage/max": 512})]
        result = self.scan("myspider", "-m", "close_reason:", "-s", "(memusage/max)", "--data-headers", "auto")
        self.assertEqual(result[0]["close_reason"], "finished")
        self.assertEqual(result[0]["memusage/max"], 512)

    def test_meta_pattern_not_post_processed(self):
        self.jobs = [make_jobdict("1/1/1", close_reason="finished", scrapystats={"errors": 5, "requests": 20})]
        result = self.scan(
            "myspider", "-m", "close_reason:", "-s", "^errors$", "-s", "^requests$", "-c", "div",
            "--data-headers", "cr,rate",
        )
        self.assertEqual((result[0]["cr"], result[0]["rate"]), ("finished", 0.25))

    def test_meta_pattern_without_data_headers(self):
        self.jobs = [make_jobdict("1/1/1", close_reason="finished", scrapystats={"memusage/max": 512})]
        result = self.scan("myspider", "-m", "close_reason:", "-s", "memusage/max")
        self.assertEqual(result[0][1:], ["finished", "512"])

    # argument validation and formatting

    def test_plot_requires_data_headers(self):
        with script_args(["myspider", "--plot", "title=x"]), self.assertRaises(SystemExit):
            ScanJobs()

    def test_spiders_only_requires_wildcard(self):
        with script_args(["myspider", "--spiders-only"]), self.assertRaises(SystemExit):
            ScanJobs()

    def test_zone_info_and_tstamp_format(self):
        self.jobs = [make_jobdict("1/1/1", scrapystats={"mystat": 1})]
        self.jobs[0]["finished_time"] = NOW_MS - (NOW_MS % (24 * HOUR_MS)) + 2 * HOUR_MS
        if self.jobs[0]["finished_time"] > NOW_MS:
            self.jobs[0]["finished_time"] -= 24 * HOUR_MS
        result = self.scan("myspider", "-s", "mystat", "--data-headers", "mystat", "--tstamp-format", "%H:%M")
        self.assertEqual(result[0]["tstamp"], "02:00")
