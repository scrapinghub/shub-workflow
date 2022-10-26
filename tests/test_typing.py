import os
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.script import BaseScript, BaseScriptProtocol
from shub_workflow.base import WorkFlowManager, WorkFlowManagerProtocol
from shub_workflow.graph import GraphManager
from shub_workflow.graph.task import Task
from shub_workflow.crawl import GeneratorCrawlManager, AsyncSchedulerCrawlManagerMixin
from shub_workflow.deliver import BaseDeliverScript
from shub_workflow.utils.contexts import script_args


class MyDeliverScript(BaseDeliverScript):
    SCRAPERNAME_NARGS = 1


class MyMixin(BaseScriptProtocol):
    def __init__(self):
        super().__init__()
        self.target = 0

    def append_flow_tag(self, tag: str):
        self.target = 1


class MyWFMixin(WorkFlowManagerProtocol):
    def __init__(self):
        super().__init__()
        self.target = 0

    def append_flow_tag(self, tag: str):
        self.target = 1


class MyScript(MyMixin, BaseScript):
    def run(self):
        pass


class MyWorkFlowManager(MyMixin, WorkFlowManager):
    def workflow_loop(self) -> bool:
        pass


class MyWorkFlowManagerTwo(MyWFMixin, WorkFlowManager):
    def workflow_loop(self) -> bool:
        pass


class MyGraphManager(MyMixin, GraphManager):
    def configure_workflow(self):
        jobA = Task("jobA", "py:command.py")
        return (jobA,)


class MyAsyncCrawlManager(AsyncSchedulerCrawlManagerMixin, GeneratorCrawlManager):  # type: ignore
    def set_parameters_gen(self):
        yield from ()


class TypingTest(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"

    @patch("shub_workflow.script.BaseScript.add_job_tags")
    def test_script_instantiation(self, mocked_add_job_tags):
        with script_args([]):
            script = MyScript()
            script.append_flow_tag("mytag")
        # self.assertEqual(mocked_add_job_tags.call_count, 0)
        self.assertEqual(script.target, 1)

    @patch("shub_workflow.script.BaseScript.add_job_tags")
    def test_workflow_manager_instantiation(self, mocked_add_job_tags):
        with script_args(["myname"]):
            manager = MyWorkFlowManager()
            manager.append_flow_tag("mytag")
        # self.assertEqual(mocked_add_job_tags.call_count, 0)
        self.assertEqual(manager.target, 1)

    @patch("shub_workflow.script.BaseScript.add_job_tags")
    def test_workflow_manager_instantiation_two(self, mocked_add_job_tags):
        with script_args(["myname"]):
            manager = MyWorkFlowManagerTwo()
            manager.append_flow_tag("mytag")
        # self.assertEqual(mocked_add_job_tags.call_count, 0)
        self.assertEqual(manager.target, 1)

    @patch("shub_workflow.script.BaseScript.add_job_tags")
    def test_graph_manager_instantiation(self, mocked_add_job_tags):
        with script_args(["myname"]):
            manager = MyGraphManager()
            manager.append_flow_tag("mytag")
        # self.assertEqual(mocked_add_job_tags.call_count, 0)
        self.assertEqual(manager.target, 1)

    def test_asyncrawlmanager(self):
        with script_args(["myname", "myspider"]):
            MyAsyncCrawlManager()
