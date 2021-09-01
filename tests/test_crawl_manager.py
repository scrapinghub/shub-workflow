import os
from unittest import TestCase
from unittest.mock import patch, Mock

from shub_workflow.crawl import CrawlManager, PeriodicCrawlManager, GeneratorCrawlManager

from .utils.contexts import script_args


class TestManager(CrawlManager):

    name = "test"


class PeriodicTestManager(PeriodicCrawlManager):

    name = "test"


class ListTestManager(GeneratorCrawlManager):

    name = "test"
    default_max_jobs = 2

    def set_parameters_gen(self):
        parameters_list = [
            {"argA": "valA"},
            {"argA": "valB"},
            {"argB": "valC"},
        ]
        for args in parameters_list:
            yield args


class TestManagerWithSpider(CrawlManager):

    name = "test"
    spider = "myimplicitspider"


class CrawlManagerTest(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider(self, mocked_super_schedule_spider):
        with script_args(["myspider"]):
            manager = TestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # second loop: spider still running. Continue.
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)

        # third loop: spider is finished. Stop.
        manager.is_finished = lambda x: "finished" if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        result = manager.workflow_loop()

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_implicit_spider(self, mocked_super_schedule_spider):
        with script_args([]):
            manager = TestManagerWithSpider()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myimplicitspider", units=None, job_settings={})

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_badoutcome(self, mocked_super_schedule_spider):
        with script_args(["myspider"]):
            manager = TestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # second loop: spider still running. Continue.
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)

        # third loop: spider is finished. Stop.
        manager.is_finished = lambda x: "cancelled" if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        result = manager.workflow_loop()

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_with_resume(self, mocked_super_schedule_spider):
        with script_args(["myspider", "--flow-id=3a20", "--resume-workflow"]):
            manager = TestManager()

        manager.get_owned_jobs = Mock()
        manager.get_owned_jobs.side_effect = [[{"key": "999/1/1"}]]
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager.get_owned_jobs.call_count, 1)

        # first loop: spider still running in workflow. Continue.
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)

        # second loop: spider is finished. Stop.
        manager.is_finished = lambda x: "finished" if x == "999/1/1" else None
        result = manager.workflow_loop()

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_periodic(self, mocked_super_schedule_spider):
        with script_args(["myspider"]):
            manager = PeriodicTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # second loop: spider still running. Continue.
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)

        # third loop: spider is finished. Schedule again.
        manager.is_finished = lambda x: "finished" if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        result = manager.workflow_loop()

        self.assertTrue(result)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # four loop: spider is cancelled. Schedule again.
        manager.is_finished = lambda x: "cancelled" if x == "999/1/2" else None
        mocked_super_schedule_spider.reset_mock()
        mocked_super_schedule_spider.side_effect = ["999/1/3"]
        result = manager.workflow_loop()

        self.assertTrue(result)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list(self, mocked_super_schedule_spider):
        with script_args(["myspider"]):
            manager = ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/1/2", "999/1/3"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider with first set of arguments
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, argA="valA", job_settings={})

        # second loop: schedule spider again with second set of arguments
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, argA="valB", job_settings={})

        # third loop: still no job finished. Wait for a free slot
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)

        # fourth loop: finish one job. We can schedule last one with third set of arguments
        manager.is_finished = lambda x: "finished" if x == "999/1/1" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, argB="valC", job_settings={})

        # fifth loop: waiting jobs to finish
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        # fourth loop: second job finished
        manager.is_finished = lambda x: "cancelled" if x == "999/1/2" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        # fifth loop: last job finished. Exit
        manager.is_finished = lambda x: "finished" if x == "999/1/3" else None
        result = manager.workflow_loop()
        self.assertFalse(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
