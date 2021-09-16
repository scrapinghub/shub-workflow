import os
import re
from io import StringIO

from unittest import TestCase
from unittest.mock import patch, Mock, call

from shub_workflow.graph import GraphManager
from shub_workflow.graph.task import Task, SpiderTask, Resource

from .utils.contexts import script_args


class TestManager(GraphManager):

    name = "test"

    def configure_workflow(self):
        # define jobs
        jobA = Task(
            task_id="jobA",
            command="commandA",
            init_args=["argA", "--optionA"],
            retry_args=["argA"],
            tags=["tag1", "tag2"],
            units=2,
        )
        jobB = Task(task_id="jobB", command="commandB", init_args=["argB", "--optionB"])
        jobC = Task(task_id="jobC", command="commandC", init_args=["argC"])
        jobD = Task(task_id="jobD", command="commandD", init_args=["argD"])

        # connect them
        jobA.add_next_task(jobC)
        jobC.add_next_task(jobD)
        jobC.add_wait_for(jobB)

        # return root jobs
        return jobA, jobB


class TestManager2(GraphManager):

    name = "test"

    def configure_workflow(self):
        # define jobs
        jobA = Task(
            task_id="jobA",
            command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
            init_args=["argA", "--optionA"],
            retry_args=["argA"],
            tags=["tag1", "tag2"],
            retries=2,
        )
        jobB = Task(
            task_id="jobB",
            command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
            init_args=["argB", "--optionB"],
        )
        jobC = Task(task_id="jobC", command="commandC", init_args=["argC"], retries=2)
        jobD = Task(task_id="jobD", command="commandD")

        # connect them
        jobA.add_next_task(jobB)
        jobA.add_next_task(jobD)
        jobB.add_next_task(jobC)
        jobD.add_wait_for(jobB)

        # return root jobs
        return (jobA,)


class TestManager3(GraphManager):

    name = "test"

    def configure_workflow(self):
        # define jobs
        jobA = Task(
            task_id="jobA",
            command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
            init_args=["argA", "--optionA"],
            retry_args=["argA"],
            tags=["tag1", "tag2"],
        )
        jobB = Task(
            task_id="jobB",
            command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
            init_args=["argB", "--optionB"],
        )
        jobC = Task(task_id="jobC", command="commandC", init_args=["argC"])
        jobD = Task(task_id="jobD", command="commandD")
        jobE = Task(task_id="jobE", command="commandE")

        # connect them
        jobA.add_next_task(jobB)
        jobA.add_next_task(jobC)
        jobC.add_next_task(jobD)
        jobD.add_next_task(jobE)
        jobE.add_wait_for(jobB)

        # return root jobs
        return (jobA,)

    def get_job_tags(self, jobid=None):
        if jobid is None:
            return ["FLOW_ID=mytagsflowid"]
        return super().get_job_tags(jobid)


class BaseTestCase(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"


class ManagerTest(BaseTestCase):
    def test_full_specs(self):
        with script_args(["--starting-job=jobA", "--starting-job=jobB"]):
            manager = TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/1/1", "999/1/2"]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 2)
        manager.schedule_script.assert_any_call(
            ["commandA", "argA", "--optionA"], tags=["tag1", "tag2"], units=2, project_id=None,
        )
        manager.schedule_script.assert_any_call(
            ["commandB", "argB", "--optionB"], tags=None, units=None, project_id=None
        )

        # second loop, something went wrong with jobA, retry with retry_args instead
        manager.is_finished = lambda x: "failed" if x == "999/1/1" else None
        manager.schedule_script.reset_mock()
        manager.schedule_script.side_effect = ["999/1/3"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(
            ["commandA", "argA"], tags=["tag1", "tag2"], units=2, project_id=None
        )

        # third loop, both jobs are still running
        manager.is_finished = lambda x: None
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # fourth loop, jobA finished (but has to wait for jobB for next job)
        manager.is_finished = lambda x: "finished" if x == "999/1/3" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # fifth loop, jobB finished, now we can run jobC
        manager.is_finished = lambda x: "finished" if x == "999/1/2" else None
        manager.schedule_script.side_effect = ["999/1/4"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # sixth loop, jobC finished, schedule jobD
        manager.is_finished = lambda x: "finished" if x == "999/1/4" else None
        manager.schedule_script.reset_mock()
        manager.schedule_script.side_effect = ["999/1/5"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandD", "argD"], tags=None, units=None, project_id=None)

        # last loop, jobD finished, workflow finished
        manager.is_finished = lambda x: "finished" if x == "999/1/5" else None
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertFalse(result)
        self.assertFalse(manager.schedule_script.called)

    @patch("sys.stderr", new_callable=StringIO)
    def test_invalid_job(self, mock_stderr):
        """
        Test error when a bad job was provided on command line.
        """
        with script_args(["--starting-job=jobA", "--starting-job=jobN"]):
            manager = TestManager()
        with self.assertRaises(SystemExit):
            manager._WorkFlowManager__on_start()
        self.assertTrue(
            "Invalid job: jobN. Available jobs: dict_keys(['jobA', 'jobC', 'jobD', 'jobB'])" in mock_stderr.getvalue()
        )

    @patch("sys.stderr", new_callable=StringIO)
    def test_no_starting_job(self, mock_stderr):
        """
        Test error when no starting job was provided.
        """
        with script_args([]):
            manager = TestManager()
        with self.assertRaises(SystemExit):
            manager._WorkFlowManager__on_start()
        self.assertTrue("You must provide either --starting-job or --root-jobs." in mock_stderr.getvalue())

    @patch("sys.stderr", new_callable=StringIO)
    def test_starting_job_and_root_jobs(self, mock_stderr):
        """
        Test error when no starting job was provided.
        """
        with script_args(["-s", "jobA", "--root-jobs"]):
            with self.assertRaises(SystemExit):
                manager = TestManager()
        self.assertTrue("You can't provide both --starting-job and --root-jobs" in mock_stderr.getvalue())

    def test_root_jobs(self):
        with script_args(["--root-jobs"]):
            manager = TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/1/1", "999/1/2"]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 2)
        manager.schedule_script.assert_any_call(
            ["commandA", "argA", "--optionA"], tags=["tag1", "tag2"], units=2, project_id=None,
        )
        manager.schedule_script.assert_any_call(
            ["commandB", "argB", "--optionB"], tags=None, units=None, project_id=None
        )

    def test_retry_job(self):
        """
        Test that failed job is retried only the specified number of times
        """
        with script_args(["--starting-job=jobA"]):
            manager = TestManager2()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )

        # second loop still running job A
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # third loop, job A_0 fails, must be retried
        manager.is_finished = lambda x: "failed" if x == "999/1/1" else None
        manager.schedule_script.side_effect = ["999/1/5"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(
            ["commandA", "--parg=0", "argA"], tags=["tag1", "tag2"], units=None, project_id=None,
        )

        # fourth loop, job A_0 fails, must be retried
        manager.schedule_script.reset_mock()
        manager.is_finished = lambda x: "failed" if x == "999/1/5" else None
        manager.schedule_script.side_effect = ["999/1/6"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(
            ["commandA", "--parg=0", "argA"], tags=["tag1", "tag2"], units=None, project_id=None,
        )

        # fifth loop, job A_0 fails again, cannot be retried (retries=2)
        manager.schedule_script.reset_mock()
        manager.is_finished = lambda x: "failed" if x == "999/1/6" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # sixth loop, job A_1 fails, must be retried
        manager.schedule_script.reset_mock()
        manager.is_finished = lambda x: "failed" if x == "999/1/2" else None
        manager.schedule_script.side_effect = ["999/1/7"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(
            ["commandA", "--parg=1", "argA"], tags=["tag1", "tag2"], units=None, project_id=None,
        )

        # 7th loop, job A_1 fails again, must be retried
        manager.schedule_script.reset_mock()
        manager.is_finished = lambda x: "failed" if x == "999/1/7" else None
        manager.schedule_script.side_effect = ["999/1/8"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(
            ["commandA", "--parg=1", "argA"], tags=["tag1", "tag2"], units=None, project_id=None,
        )

        # 8th loop, job A_1 fails again, cannot be retried (retries=2)
        manager.schedule_script.reset_mock()
        manager.is_finished = lambda x: "failed" if x == "999/1/8" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

    def test_retry_job_without_retry_args(self):
        """
        Test that failed job without retry args is retried with init args
        """
        with script_args(["--starting-job=jobC"]):
            manager = TestManager2()

        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/3/1"]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_any_call(["commandC", "argC"], tags=None, units=None, project_id=None)

        # second loop, job C fails, must be retried.
        manager.is_finished = lambda x: "failed" if x == "999/3/1" else None
        manager.schedule_script.side_effect = ["999/3/2"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 2)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

    def test_max_retries(self):
        """
        Test max retries
        """
        with script_args(["--starting-job=jobC"]):
            manager = TestManager2()

        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/3/1"]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)

        # second loop, job C fails, must be retried.
        manager.is_finished = lambda x: "failed" if x == "999/3/1" else None
        manager.schedule_script.side_effect = ["999/3/2"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 2)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # third loop, job C fails again, must be retried (last retry).
        manager.is_finished = lambda x: "failed" if x == "999/3/2" else None
        manager.schedule_script.side_effect = ["999/3/3"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 3)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # fourth loop, job C fails again, give up.
        manager.is_finished = lambda x: "failed" if x == "999/3/3" else None
        result = manager.workflow_loop()
        self.assertFalse(result)
        self.assertEqual(manager.schedule_script.call_count, 3)

    def test_parallel_job(self):
        """
        Test correct scheduling of a job with parallelization
        """
        with script_args(["--starting-job=jobA"]):
            manager = TestManager2()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )

        # second loop still running job A
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # third loop, job A_0 fails, must be resumed
        manager.is_finished = lambda x: "failed" if x == "999/1/1" else None
        manager.schedule_script.side_effect = ["999/1/5"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(
            ["commandA", "--parg=0", "argA"], tags=["tag1", "tag2"], units=None, project_id=None,
        )

        # fourth loop, job A finishes, will start now parallel job B
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # fifth loop, jobB finishes partially, neither jobD nor jobC can still be scheduled
        manager.is_finished = lambda x: "finished" if x == "999/2/1" else None
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # sixth loop, jobB finishes, jobD now can run, also jobC is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/3/1", "999/3/2"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandC", "argC"], tags=None, units=None, project_id=None)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

    def test_tags(self):
        with script_args(["--starting-job=jobA", "--tag=tag3", "--tag=tag4"]):
            manager = TestManager3()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )
        self.assertEqual(
            set(manager._make_tags(["tag1", "tag2"])),
            set(["tag1", "tag2", "tag3", "tag4", f"FLOW_ID={manager.flow_id}"]),
        )

    def test_flow_id_from_command_line(self):
        with script_args(["--starting-job=jobA", "--flow-id=myflowid"]):
            manager = TestManager3()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )
        self.assertEqual(
            set(manager._make_tags(["tag1", "tag2"])), set(["tag1", "tag2", "FLOW_ID=myflowid"]),
        )

    def test_flow_id_from_job_tags(self):
        with script_args(["--starting-job=jobA"]):
            manager = TestManager3()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )
        self.assertEqual(
            set(manager._make_tags(["tag1", "tag2"])), set(["tag1", "tag2", "FLOW_ID=mytagsflowid"]),
        )

    def test_skip_job(self):
        with script_args(["--starting-job=jobA", "--skip-job=jobC"]):
            manager = TestManager3()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )

        # second loop, jobA finishes, jobB is scheduled, not jobC or next ones as it was skipped
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # last loop, jobB finished, workflow finished
        manager.is_finished = lambda x: "finished" if x.startswith("999/2") else None
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertFalse(result)
        self.assertFalse(manager.schedule_script.called)

    def test_wait_for_already_finished_job(self):
        with script_args(["--starting-job=jobA"]):
            manager = TestManager3()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )

        # second loop, jobA finishes, jobB and jobC are scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
            "999/3/1",
        ]
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )
        manager.schedule_script.assert_any_call(["commandC", "argC"], tags=None, units=None, project_id=None)

        # third loop, jobB finishes
        manager.is_finished = lambda x: "finished" if x.startswith("999/2/") else None
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

        # fourth loop, jobC finishes, jobD is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

        # fifth loop, jobD finishes, jobE is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_no_wait_for_a_job_that_will_not_be_run(self):
        with script_args(["--starting-job=jobD"]):
            manager = TestManager3()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/4/1"]
        manager._WorkFlowManager__on_start()

        # first loop, jobD is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

        # second loop, jobD finishes, jobE is scheduled, regardless it should wait for jobB to finish.
        # However, as defined in the start jobs, jobB will never be scheduled (i.e. could have been
        # already scheduled/finished by another instance of the manager)
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_skip_job_no_wait_for_skipped(self):
        with script_args(["--starting-job=jobA", "--skip-job=jobB"]):
            manager = TestManager3()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=["tag1", "tag2"], units=None, project_id=None,
            )

        # second loop, jobA finishes, jobC is scheduled, but not jobB as it is skipped.
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/3/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandC", "argC"], tags=None, units=None, project_id=None)

        # third loop, jobC finishes, jobD is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

        # fourth loop, jobD finishes, jobE is scheduled (will not wait for B as it was skipped)
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_start_job_wait_for_another_in_start_jobs(self):
        with script_args(["--starting-job=jobB", "--starting-job=jobE"]):
            manager = TestManager3()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop, jobB is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # second loop, jobB finishes, jobE is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_wait_for_a_starting_parallel_job(self):
        with script_args(["--starting-job=jobB"]):
            manager = TestManager2()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop, jobB is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # second loop, one jobB finishes, jobC must not still be scheduled
        manager.is_finished = lambda x: "finished" if x == "999/2/1" else None
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertFalse(manager.schedule_script.called)

    def test_start_job_wait_for_a_job_after_another_start_job(self):
        """
        One of the starting jobs must wait for a job triggered on the other start job finish:
        jobC -> jobD
        jobE must wait for jobD
        """

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobA = Task(
                    task_id="jobA",
                    command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
                    init_args=["argA", "--optionA"],
                    retry_args=["argA"],
                    tags=["tag1", "tag2"],
                )
                jobB = Task(
                    task_id="jobB",
                    command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
                    init_args=["argB", "--optionB"],
                )
                jobC = Task(task_id="jobC", command="commandC", init_args=["argC"])
                jobD = Task(task_id="jobD", command="commandD")
                jobE = Task(task_id="jobE", command="commandE")

                # connect them
                jobA.add_next_task(jobB)
                jobA.add_next_task(jobC)
                jobC.add_next_task(jobD)
                jobE.add_wait_for(jobD)

                return (jobA, jobE)

        with script_args(["--starting-job=jobC", "--starting-job=jobE"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/3/1"]
        manager._WorkFlowManager__on_start()

        # first loop, jobC is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # second loop, jobC finishes, jobD is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

        # third loop, jobD finishes, jobE is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_start_job_wait_for_another_that_must_wait_another(self):
        """
        One of the starting jobs must wait for a job triggered on the other start job finish:
        jobC -> jobD
        jobE must wait for jobD
        """

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobA = Task(
                    task_id="jobA",
                    command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
                    init_args=["argA", "--optionA"],
                    retry_args=["argA"],
                    tags=["tag1", "tag2"],
                )
                jobB = Task(
                    task_id="jobB",
                    command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
                    init_args=["argB", "--optionB"],
                )
                jobC = Task(task_id="jobC", command="commandC", init_args=["argC"])
                jobD = Task(task_id="jobD", command="commandD")
                jobE = Task(task_id="jobE", command="commandE")

                # connect them
                jobA.add_next_task(jobB)
                jobB.add_next_task(jobC)

                jobD.add_wait_for(jobC)
                jobE.add_wait_for(jobD)

                return (jobA, jobD, jobE)

        with script_args(["--starting-job=jobB", "--starting-job=jobD", "--starting-job=jobE"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop, jobB is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # second loop, jobB finishes, jobC is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/3/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # third loop, jobC finishes, jobD is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

        # fourth loop, jobD finishes, jobE is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_job_wait_for_another_that_must_wait_another_that_will_not_run(self):
        """
        One of the jobs must wait for a job that waits for another that will never run
        """

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobA = Task(
                    task_id="jobA",
                    command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
                    init_args=["argA", "--optionA"],
                    retry_args=["argA"],
                    tags=["tag1", "tag2"],
                )
                jobB = Task(
                    task_id="jobB",
                    command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
                    init_args=["argB", "--optionB"],
                )
                jobC = Task(task_id="jobC", command="commandC", init_args=["argC"])
                jobD = Task(task_id="jobD", command="commandD")
                jobE = Task(task_id="jobE", command="commandE")

                # connect them
                jobA.add_next_task(jobB)
                jobB.add_next_task(jobC)
                jobB.add_next_task(jobD)
                jobC.add_next_task(jobE)

                jobD.add_wait_for(jobA)
                jobE.add_wait_for(jobD)

                return (jobA,)

        with script_args(["--starting-job=jobB"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop, jobB is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # second loop, jobB finishes, jobC must be scheduled, but not jobD
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/3/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # third loop, jobC finishes, jobD is scheduled, but not jobE
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

        # fourth loop, jobD finishes, jobE is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

    def test_job_wait_for_another_that_must_wait_another_that_will_not_run_ii(self):
        """
        One of the jobs must wait for a job that waits for another that will never run. Second variant.
        """

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobA = Task(
                    task_id="jobA",
                    command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
                    init_args=["argA", "--optionA"],
                    retry_args=["argA"],
                    tags=["tag1", "tag2"],
                )
                jobB = Task(
                    task_id="jobB",
                    command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
                    init_args=["argB", "--optionB"],
                )
                jobC = Task(task_id="jobC", command="commandC", init_args=["argC"])
                jobD = Task(task_id="jobD", command="commandD")
                jobE = Task(task_id="jobE", command="commandE")

                # connect them
                jobA.add_next_task(jobB)
                jobB.add_next_task(jobC)
                jobB.add_next_task(jobE)
                jobC.add_next_task(jobD)

                jobD.add_wait_for(jobE)
                jobE.add_wait_for(jobA)

                return (jobA,)

        with script_args(["--starting-job=jobB"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop, jobB is scheduled
        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f"--parg={i}", "argB", "--optionB"], tags=None, units=None, project_id=None,
            )

        # second loop, jobB finishes, jobC must be scheduled, but not jobE
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/3/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_called_with(["commandC", "argC"], tags=None, units=None, project_id=None)

        # third loop, jobC finishes, jobE is scheduled, but not jobD
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/5/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandE"], tags=None, units=None, project_id=None)

        # fourth loop, jobE finishes, jobD is scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandD"], tags=None, units=None, project_id=None)

    def test_many_jobs_waiting_for_not_running_job(self):
        """
        When many jobs waits for the same that will not run, ensure that only one job at a time
        is choosen for run, except when we run a job with parallelization (in this case, all
        parallel jobs must run)
        """

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobA = Task(
                    task_id="jobA",
                    command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",
                    init_args=["argA", "--optionA"],
                )
                jobB = Task(task_id="jobB", command="commandB", init_args=["argB", "--optionB"])
                jobC = Task(task_id="jobC", command="commandC", init_args=["argC", "--optionC"])
                jobE = Task(task_id="jobE", command="commandE")

                # connect them
                jobA.add_wait_for(jobE)
                jobB.add_wait_for(jobE)
                jobC.add_wait_for(jobE)

                return (jobA, jobB, jobC, jobE)

        with script_args(["--starting-job=jobA", "--starting-job=jobB", "--starting-job=jobC"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/1/1",
            "999/1/2",
            "999/1/3",
            "999/1/4",
        ]
        manager._WorkFlowManager__on_start()

        # first loop, All jobs A must be scheduled
        self.assertTrue(manager.workflow_loop())
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandA", f"--parg={i}", "argA", "--optionA"], tags=None, units=None, project_id=None,
            )

        # second loop, jobs still running, nothing scheduled
        manager.is_finished = lambda x: None
        self.assertTrue(manager.workflow_loop())

        # third loop, jobs A finishes, jobB scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/2/1"]
        self.assertTrue(manager.workflow_loop())
        manager.schedule_script.assert_called_with(
            ["commandB", "argB", "--optionB"], tags=None, units=None, project_id=None
        )

        # fourth loop, jobs still running, nothing scheduled
        manager.is_finished = lambda x: None
        self.assertTrue(manager.workflow_loop())

        # 5th loop, job B finishes, jobC scheduled
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/3/1"]
        self.assertTrue(manager.workflow_loop())
        manager.schedule_script.assert_called_with(
            ["commandC", "argC", "--optionC"], tags=None, units=None, project_id=None
        )

    def test_job_cyclic_dependency(self):
        """If at some point all pending jobs depend on each other, raise an error."""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobA = Task(task_id="jobA", command="commandA")
                jobB = Task(
                    task_id="jobB",
                    command="{% for i in range(4) %}commandB --parg={{ i }}\n{% endfor %}",
                    init_args=["argB", "--optionB"],
                )
                jobC = Task(task_id="jobC", command="commandC")
                jobD = Task(task_id="jobD", command="commandD")

                # connect them
                jobA.add_next_task(jobD)

                jobB.add_wait_for(jobC)
                jobC.add_wait_for(jobD)
                jobD.add_wait_for(jobB)

                return (jobA, jobB, jobC)

        with script_args(["--starting-job=jobA", "--starting-job=jobB", "--starting-job=jobC"]):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/2/1"]
        manager._WorkFlowManager__on_start()

        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandA"], tags=None, units=None, project_id=None)

        manager.is_finished = lambda x: "finished"
        with self.assertRaisesRegex(
            RuntimeError,
            re.escape(
                "Job dependency cycle detected:"
                " jobB_0 waits for ['jobC'],"
                " jobB_1 waits for ['jobC'],"
                " jobB_2 waits for ['jobC'],"
                " jobB_3 waits for ['jobC'],"
                " jobC waits for ['jobD'],"
                " jobD waits for ['jobB_0', 'jobB_1', 'jobB_2', 'jobB_3']"
            ),
        ):
            manager.workflow_loop()

    def test_job_required_resource(self):
        """If a jobs depends on an unavailable resource, it should not run."""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            fooR = Resource("foo")

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="commandA")
                jobB = Task("jobB", command="commandB")

                # set required resources
                jobA.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.fooR: 1})

                return jobA, jobB

        with script_args(["--starting-job=jobA", "--starting-job=jobB"]):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/2/1"]
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager._available_resources, {manager.fooR: 1})

        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandA"], tags=None, units=None, project_id=None)

        # If commandA is still running, commandB should not get started.
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/2/2"]

        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 0)

        manager.is_finished = lambda x: "finished"
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandB"], tags=None, units=None, project_id=None)

    def test_job_required_alternative_resources(self):
        """Test alternative resources."""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            fooR = Resource("foo")
            barR = Resource("bar")

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="commandA")
                jobB = Task("jobB", command="commandB")
                jobC = Task("jobC", command="commandC")
                jobD = Task("jobD", command="commandD")

                # set required resources
                jobA.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.fooR: 1})
                jobC.add_required_resources({self.fooR: 1})
                jobC.add_required_resources({self.barR: 1})
                jobD.add_required_resources({self.barR: 1})

                return jobA, jobB, jobC, jobD

        with script_args(
            ["--starting-job=jobA", "--starting-job=jobB", "--starting-job=jobC", "--starting-job=jobD",]
        ):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = ["999/1/1", "999/3/1"]
        manager._WorkFlowManager__on_start()

        # First loop: can run jobA and jobC
        result = manager.workflow_loop()
        self.assertTrue(result)
        manager.schedule_script.assert_any_call(["commandA"], tags=None, units=None, project_id=None)
        manager.schedule_script.assert_any_call(["commandC"], tags=None, units=None, project_id=None)

        # Second loop: if both commandA and command C are still running, nor commandB neither commandD can't start.
        manager.is_finished = lambda x: None
        manager.schedule_script.reset_mock()

        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 0)

        # third loop: commandC finishes so commandD can run
        manager.is_finished = lambda x: "finished" if x == "999/3/1" else None
        manager.schedule_script.side_effect = ["999/4/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandD"], tags=None, units=None, project_id=None)

        # fourth loop: commandD finishes. commandB, can't still run
        manager.schedule_script.reset_mock()
        manager.is_finished = lambda x: "finished" if x == "999/4/1" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 0)

        # fifth loop: commandA finishes so commandB can run
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = ["999/2/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandB"], tags=None, units=None, project_id=None)

    def test_parallel_job_required_resource(self):
        """If a job with required resource is parallel, divide the resource among all parallel subjobs"""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            fooR = Resource("foo")

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",)
                jobB = Task("jobB", command="commandB")

                # set required resources
                jobA.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.fooR: 1})

                return jobA, jobB

        with script_args(["--starting-job=jobA", "--starting-job=jobB"]):
            manager = _TestManager()

        def generate_job_keys(proj, spider):
            for i in range(1000):
                yield f"{proj}/{spider}/{i}"

        manager.schedule_script = Mock(side_effect=generate_job_keys(999, 2))
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager._available_resources, {manager.fooR: 1})

        # First loop: schedule jobA
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        calls = [call(["commandA", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(4)]
        manager.schedule_script.assert_has_calls(calls)

        # Second loop. If commandA is still running, commandB should not get started.
        manager.is_finished = lambda x: None
        manager.schedule_script.reset_mock()

        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 0)

        # Third loop. All jobA subjobs finished. Schedule jobB.
        manager.is_finished = lambda x: "finished"
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 1)
        manager.schedule_script.assert_called_with(["commandB"], tags=None, units=None, project_id=None)

    def test_parallel_job_required_double_resource(self):
        """Double resources with parallel jobs"""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            fooR = Resource("foo")
            barR = Resource("bar")

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",)
                jobB = Task("jobB", command="commandB")
                jobC = Task("jobC", command="{% for i in range(4) %}commandC --parg={{ i }}\n{% endfor %}",)

                # set required resources
                jobA.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.barR: 1})
                jobC.add_required_resources({self.fooR: 1, self.barR: 1})

                return jobA, jobB, jobC

        with script_args(["--starting-job=jobA", "--starting-job=jobB", "--starting-job=jobC"]):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [f"999/1/{i}" for i in range(1, 5)] + ["999/2/1"]
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager._available_resources, {manager.fooR: 1, manager.barR: 1})

        # first loop: run jobA and jobB
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 5)
        calls = [call(["commandA", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(4)] + [
            call(["commandB"], tags=None, units=None, project_id=None)
        ]
        manager.schedule_script.assert_has_calls(calls)

        # If only jobA is finished, jobC can't run
        manager.is_finished = lambda x: "finished" if x.startswith("999/1/") else None
        manager.schedule_script.reset_mock()

        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 0)

        # finished jobB, jobC can run
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.side_effect = [f"999/3/{i}" for i in range(1, 5)]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        calls = [call(["commandC", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(4)]
        manager.schedule_script.assert_has_calls(calls)

    def test_parallel_job_alternative_required_resource(self):
        """Alternative resources with parallel jobs"""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            fooR = Resource("foo")
            barR = Resource("bar")

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",)
                jobB = Task("jobB", command="commandB")
                jobC = Task("jobC", command="{% for i in range(4) %}commandC --parg={{ i }}\n{% endfor %}",)

                # set required resources
                jobA.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.fooR: 1})
                jobB.add_required_resources({self.barR: 1})
                jobC.add_required_resources({self.fooR: 1})
                jobC.add_required_resources({self.barR: 1})

                return jobA, jobB, jobC

        with script_args(["--starting-job=jobA", "--starting-job=jobB", "--starting-job=jobC"]):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [f"999/1/{i}" for i in range(1, 5)] + ["999/2/1"]
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager._available_resources, {manager.fooR: 1, manager.barR: 1})

        # first loop: run commandA and command B
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 5)
        calls = [call(["commandA", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(4)] + [
            call(["commandB"], tags=None, units=None, project_id=None)
        ]
        manager.schedule_script.assert_has_calls(calls)

        # If any job is finished, jobC can run
        manager.is_finished = lambda x: "finished" if x.startswith("999/1/") else None
        manager.schedule_script.reset_mock()

        manager.schedule_script.side_effect = [f"999/3/{i}" for i in range(1, 5)]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 4)
        calls = [call(["commandC", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(4)]
        manager.schedule_script.assert_has_calls(calls)

    def test_parallel_job_partial_resource(self):
        """When two parallel jobs share same resource, if one partially finishes,
           the other can partially acquire the resource."""

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            fooR = Resource("foo")

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",)
                jobC = Task("jobC", command="{% for i in range(4) %}commandC --parg={{ i }}\n{% endfor %}",)

                # set required resources
                jobA.add_required_resources({self.fooR: 1})
                jobC.add_required_resources({self.fooR: 1})

                return jobA, jobC

        with script_args(["--starting-job=jobA", "--starting-job=jobC", "--max-running-job=3"]):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [f"999/1/{i}" for i in range(1, 5)] + [f"999/2/{i}" for i in range(1, 5)]
        manager._WorkFlowManager__on_start()

        # first loop: run three jobs of jobA
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 3)
        calls = [call(["commandA", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(3)]
        manager.schedule_script.assert_has_calls(calls)

        # second loop: run remaining jobA and two jobC
        manager.is_finished = lambda x: "finished"
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 3)
        calls = [call(["commandA", "--parg=3"], tags=None, units=None, project_id=None)] + [
            call(["commandC", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(2)
        ]
        manager.schedule_script.assert_has_calls(calls)

        # third loop: run remaining two jobC
        manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 2)
        calls = [call(["commandC", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(2, 4)]
        manager.schedule_script.assert_has_calls(calls)

    def test_max_running_jobs(self):
        """
        Test max running jobs
        """

        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define tasks
                jobA = Task("jobA", command="{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}",)
                jobB = Task("jobB", command="commandB")

                return jobA, jobB

        with script_args(["--starting-job=jobA", "--starting-job=jobB", "--max-running-jobs=2"]):
            manager = _TestManager()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager._WorkFlowManager__on_start()
        # will test that all 5 jobs (4 for A and 1 for B) will run one by one
        side_effects = [f"999/1/{i+1}" for i in range(4)] + ["999/2/1"]
        for i in range(3):
            side_effect, side_effects = side_effects[:2], side_effects[2:]
            manager.schedule_script.side_effect = side_effect
            result = manager.workflow_loop()
            self.assertTrue(result)
            self.assertEqual(manager.schedule_script.call_count, len(side_effect))
            manager.is_finished = lambda x: "finished"
            manager.schedule_script.reset_mock()
        result = manager.workflow_loop()
        self.assertFalse(result)

    def test_only_starting_jobs(self):
        with script_args(["--starting-job=jobA", "--starting-job=jobB", "--only-starting-jobs"]):
            manager = TestManager()
        manager.is_finished = lambda x: None
        manager.schedule_script = Mock()
        manager._WorkFlowManager__on_start()

        # first loop: job A and job B are scheduled
        manager.schedule_script.side_effect = ["999/1/1", "999/2/1"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_script.call_count, 2)
        manager.is_finished = lambda x: "finished"

        result = manager.workflow_loop()
        self.assertFalse(result)

    def test_multiple_parallel_arg_substitution(self):
        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                command = """{% for i in range(4) %}commandB --config='{"topic": {{ i }}, "file": "ds_dump_{{ i }}"}'\n{% endfor %}"""
                jobB = Task("jobB", command=command, init_args=["argB", "--optionB"])
                return (jobB,)

        with script_args(["--starting-job=jobB"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "999/2/1",
            "999/2/2",
            "999/2/3",
            "999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f'--config={{"topic": {i}, "file": "ds_dump_{i}"}}', "argB", "--optionB",],
                tags=None,
                units=None,
                project_id=None,
            )

    def test_custom_target_project_id(self):
        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                jobB = Task(
                    "jobB",
                    command="""{% for i in range(4) %}commandB --config='{"topic": {{ i }}, "file": "ds_dump_{{ i }}"}'\n{% endfor %}""",
                    init_args=["argB", "--optionB"],
                    project_id=1999,
                )
                return (jobB,)

        with script_args(["--starting-job=jobB"]):
            manager = _TestManager()
        manager.schedule_script = Mock()
        manager.schedule_script.side_effect = [
            "1999/2/1",
            "1999/2/2",
            "1999/2/3",
            "1999/2/4",
        ]
        manager._WorkFlowManager__on_start()

        result = manager.workflow_loop()
        self.assertTrue(result)
        for i in range(4):
            manager.schedule_script.assert_any_call(
                ["commandB", f'--config={{"topic": {i}, "file": "ds_dump_{i}"}}', "argB", "--optionB",],
                tags=None,
                units=None,
                project_id=1999,
            )

    @patch("shub_workflow.graph.time")
    def test_wait_time(self, mocked_time):
        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                jobA = Task("jobA", "{% for i in range(4) %}commandA --parg={{ i }}\n{% endfor %}", wait_time=3600,)
                jobB = Task("jobB", "commandB")
                jobB.add_wait_for(jobA)

                return (jobA, jobB)

        mocked_time.side_effect = [0] * 8
        with script_args(["--starting-job=jobA", "--starting-job=jobB"]):
            manager = _TestManager()

        manager.schedule_script = Mock()
        manager._WorkFlowManager__on_start()

        # first loop, jobA still can't run
        self.assertTrue(manager.workflow_loop())
        self.assertEqual(manager.schedule_script.call_count, 0)

        # second loop, jobA can run now
        mocked_time.side_effect = [3660] * 4
        manager.schedule_script.side_effect = [f"999/1/{i}" for i in range(1, 5)]

        self.assertTrue(manager.workflow_loop())
        self.assertEqual(manager.schedule_script.call_count, 4)
        calls = [call(["commandA", f"--parg={i}"], tags=None, units=None, project_id=None) for i in range(4)]
        manager.schedule_script.assert_has_calls(calls)

        # third loop: jobA finishes, can run jobB
        manager.schedule_script.side_effect = ["999/2/1"]
        manager.is_finished = lambda x: "finished"
        self.assertTrue(manager.workflow_loop())
        self.assertEqual(manager.schedule_script.call_count, 5)
        manager.schedule_script.assert_called_with(["commandB"], tags=None, units=None, project_id=None)

        # fourth loop: joB finishes, all finishes
        self.assertFalse(manager.workflow_loop())

    def test_spider_task(self):
        class _TestManager(GraphManager):
            project_id = 999
            name = "test"

            def configure_workflow(self):
                # define jobs
                jobS = SpiderTask(task_id="jobS", spider="myspiderS", tags=["tag1"], units=1, argA="valA", argB="valB",)
                jobA = Task(task_id="jobA", command="commandA --optionA=A")
                jobT = SpiderTask(task_id="jobT", spider="myspiderT", argC="valC", argD="valD")

                # connect them
                jobS.add_next_task(jobA)
                jobA.add_next_task(jobT)

                # return starting jobs
                return (jobS,)

        with script_args(["--starting-job=jobS"]):
            manager = _TestManager()
        manager.schedule_spider = Mock()
        manager.schedule_spider.side_effect = ["999/1/1"]
        manager._WorkFlowManager__on_start()

        # first loop, run jobS
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(manager.schedule_spider.call_count, 1)
        manager.schedule_spider.assert_any_call(
            "myspiderS", tags=["tag1"], units=1, project_id=None, argA="valA", argB="valB",
        )
