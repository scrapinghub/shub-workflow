import os
from io import StringIO
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.base import WorkFlowManager
from shub_workflow.utils.contexts import script_args


@patch("shub_workflow.base.WorkFlowManager.get_job_tags")
@patch("shub_workflow.base.WorkFlowManager._update_metadata")
@patch("shub_workflow.script.BaseScript.get_sc_project_settings", new=lambda _: {})
class WorkFlowManagerTest(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"

    @patch("sys.stderr", new_callable=StringIO)
    def test_name_required_not_set(self, mocked_stderr, mocked_update_metadata, mocked_get_job_tags):
        class TestManager(WorkFlowManager):
            def workflow_loop(self):
                return True

        mocked_get_job_tags.side_effect = [[], []]

        with script_args([]):
            with self.assertRaises(SystemExit):
                TestManager()
        self.assertTrue("the following arguments are required: name" in mocked_stderr.getvalue())

    def test_name_required_set(self, mocked_update_metadata, mocked_get_job_tags):
        class TestManager(WorkFlowManager):
            def workflow_loop(self):
                return True

        mocked_get_job_tags.side_effect = [[], []]

        with script_args(["my_fantasy_name"]):
            manager = TestManager()
        self.assertEqual(manager.name, "my_fantasy_name")
        self.assertEqual(manager.project_id, 999)
        self.assertEqual(manager.get_project().key, '999')

    @patch("shub_workflow.base.WorkFlowManager._check_resume_workflow")
    def test_check_resume_workflow_not_called(
        self, mocked_check_resume_workflow, mocked_update_metadata, mocked_get_job_tags
    ):
        class TestManager(WorkFlowManager):
            def workflow_loop(self):
                return True

        mocked_get_job_tags.side_effect = [[], []]

        with script_args(["my_fantasy_name"]):
            manager = TestManager()
        self.assertEqual(manager.name, "my_fantasy_name")

        manager._on_start()
        self.assertFalse(mocked_check_resume_workflow.called)

    @patch("shub_workflow.base.WorkFlowManager._check_resume_workflow")
    def test_check_resume_workflow_called(
        self, mocked_check_resume_workflow, mocked_update_metadata, mocked_get_job_tags
    ):
        class TestManager(WorkFlowManager):
            def workflow_loop(self):
                return True

        mocked_get_job_tags.side_effect = [[], []]

        with script_args(["my_fantasy_name", "--flow-id=3456"]):
            manager = TestManager()
        self.assertEqual(manager.name, "my_fantasy_name")

        manager._on_start()
        self.assertTrue(mocked_check_resume_workflow.called)

    def test_project_id_override(self, mocked_update_metadata, mocked_get_job_tags):
        class TestManager(WorkFlowManager):
            def workflow_loop(self):
                return True

        mocked_get_job_tags.side_effect = [[], []]

        with script_args(["my_fantasy_name", "--project-id=888"]):
            manager = TestManager()
        self.assertEqual(manager.project_id, 888)
        self.assertEqual(manager.get_project().key, '888')
