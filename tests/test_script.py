import os
from typing import List
from unittest import TestCase
from unittest.mock import MagicMock, patch

from scrapinghub.client.exceptions import ServerError

from shub_workflow.script import BaseScript, BaseLoopScript
from shub_workflow.utils.contexts import script_args


class MyLoopScript(BaseLoopScript):
    def workflow_loop(self) -> bool:
        return False


class MyScript(BaseScript):

    @property
    def description(self) -> str:
        return "My script description."

    def run(self):
        pass


@patch("shub_workflow.script.BaseScript.get_sc_project_settings", new=lambda _: {})
class BaseScriptTest(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"

    def test_description_in_help(self):
        with script_args([]):
            script = MyScript()
        self.assertIn("My script description.", script.argparser.format_help())
        self.assertNotIn("My script description.", script.argparser.format_usage())

    def _get_jobs_meta(self, **kwargs) -> List[str]:
        with script_args([]):
            script = MyScript()
        project = MagicMock()
        project.jobs.iter.return_value = []
        with patch.object(script, "get_project", return_value=project):
            list(script.get_jobs(**kwargs))
        return project.jobs.iter.call_args.kwargs.get("meta", [])

    def test_get_jobs_tags_meta(self):
        self.assertEqual(self._get_jobs_meta(state=["running"]), [])
        self.assertEqual(self._get_jobs_meta(meta=["spider"]), ["spider"])
        self.assertEqual(self._get_jobs_meta(has_tag=["A"]), ["tags"])
        self.assertEqual(self._get_jobs_meta(lacks_tag=["A"], meta=["spider"]), ["spider", "tags"])
        self.assertEqual(self._get_jobs_meta(has_tag=["A"], meta=["tags"]), ["tags"])

    def test_get_jobs_does_not_mutate_caller_meta(self):
        meta = ["spider"]
        self._get_jobs_meta(has_tag=["A"], meta=meta)
        self.assertEqual(meta, ["spider"])

    @patch("time.sleep")
    def test_get_jobs_retries_a_failed_page(self, _sleep):
        with script_args([]):
            script = MyScript()
        project = MagicMock()
        # the request runs while the jobs are iterated, not when get_jobs() is called
        project.jobs.iter.side_effect = [
            ServerError(),
            iter([{"key": "999/1/1"}, {"key": "999/1/2"}]),
            iter([]),
        ]
        with patch.object(script, "get_project", return_value=project):
            jobs = list(script.get_jobs(state=["running"]))
        self.assertEqual([job["key"] for job in jobs], ["999/1/1", "999/1/2"])
        self.assertEqual(project.jobs.iter.call_count, 3)

    def test_memory_stats_set_on_upload(self):
        with script_args([]):
            script = MyLoopScript()
        usage = {"rss": 3 * 2**20, "rss/anon": 2**20, "cgroup/limit": 6 * 2**30}
        with patch("shub_workflow.script.get_memory_usage", return_value=usage), \
                patch("shub_workflow.script.BaseScript.upload_stats"):
            script.upload_stats()
        for name, value in usage.items():
            self.assertEqual(script.stats.get_value(f"memusage/{name}"), value)
