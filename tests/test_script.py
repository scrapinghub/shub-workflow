import os
from typing import List
from unittest import TestCase
from unittest.mock import MagicMock, patch

from shub_workflow.script import BaseScript
from shub_workflow.utils.contexts import script_args


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
