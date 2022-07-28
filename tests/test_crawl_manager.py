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

    def bad_outcome_hook(self, spider, outcome, spider_args_override, jobkey):
        if "argR" not in spider_args_override:
            spider_args_override.update({"argR": "valR"})
            self.add_job(spider, spider_args_override)
        elif "argS" not in spider_args_override:
            spider_args_override.update({"argS": "valS"})
            self.add_job(spider, spider_args_override)


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
    def test_schedule_spider_bad_outcome(self, mocked_super_schedule_spider):
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

        manager.get_jobs = Mock()
        manager.get_jobs.side_effect = [[{"key": "999/1/1", "tags": ["FLOW_ID=3a20", "PARENT_NAME=test"]}]]
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager.get_jobs.call_count, 1)

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
    def test_schedule_spider_with_resume_no_owned(self, mocked_super_schedule_spider):
        with script_args(["myspider", "--flow-id=3a20", "--resume-workflow"]):
            manager = TestManager()

        manager.get_jobs = Mock()
        manager.get_jobs.side_effect = [[{"key": "999/1/1", "tags": ["FLOW_ID=3a20", "PARENT_NAME=testa"]}]]
        manager._WorkFlowManager__on_start()
        self.assertEqual(manager.get_jobs.call_count, 1)

        # first loop: no spider, schedule one.
        manager.is_finished = lambda x: None
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)

        # second loop: spider is finished. Stop.
        manager.is_finished = lambda x: "finished" if x == "999/1/2" else None
        result = manager.workflow_loop()

        self.assertFalse(result)

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
    def test_schedule_spider_list_bb(self, mocked_super_schedule_spider):
        with script_args(["myspider"]):
            manager = ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/1/2", "999/1/3", "999/1/4", "999/1/5"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider with first set of arguments
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["JOBSEQ=0000000001"], job_settings={}
        )
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valB", tags=["JOBSEQ=0000000002"], job_settings={}
        )

        # second loop: still no job finished. Wait for a free slot
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)

        # third loop: finish one job. We can schedule last one with third set of arguments
        manager.is_finished = lambda x: "finished" if x == "999/1/1" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argB="valC", tags=["JOBSEQ=0000000003"], job_settings={}
        )

        # fourth loop: waiting jobs to finish
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        # fifth loop: second job finished with failed outcome. Retry according
        # to bad outcome hook
        manager.is_finished = lambda x: "cancelled (stalled)" if x == "999/1/2" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valB", tags=["JOBSEQ=0000000002.r1"], argR="valR", job_settings={}
        )

        # sixth loop: third job finished.
        manager.is_finished = lambda x: "finished" if x == "999/1/3" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)

        # seventh loop: retried job failed again. Retry with new argument.
        manager.is_finished = lambda x: "memusage_exceeded" if x == "999/1/4" else None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 5)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valB", tags=["JOBSEQ=0000000002.r2"], argR="valR", job_settings={}
        )

        # eighth loop: retried job finished. Exit.
        manager.is_finished = lambda x: "finished" if x == "999/1/5" else None
        result = manager.workflow_loop()
        self.assertFalse(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 5)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_explicit_spider(self, mocked_super_schedule_spider):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = "myspider"

            def set_parameters_gen(self):
                parameters_list = [
                    {"argA": "valA"},
                    {"argA": "valB", "spider": "myspidertwo"},
                ]
                for args in parameters_list:
                    yield args

        with script_args([]):
            manager = _ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/1/2"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider with first set of arguments
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["JOBSEQ=0000000001"], job_settings={}
        )
        mocked_super_schedule_spider.assert_any_call(
            "myspidertwo", units=None, argA="valB", tags=["JOBSEQ=0000000002"], job_settings={}
        )

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_scrapy_cloud_params(self, mocked_super_schedule_spider):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = "myspider"

            def set_parameters_gen(self):
                parameters_list = [
                    {
                        "argA": "valA",
                        "units": 2,
                        "tags": ["CHECKED"],
                        "project_id": 999,
                        "job_settings": {"CONCURRENT_REQUESTS": 2},
                    },
                ]
                for args in parameters_list:
                    yield args

        with script_args([]):
            manager = _ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/1/2"]
        manager._WorkFlowManager__on_start()

        # first loop: schedule spider with first set of arguments
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "myspider",
            units=2,
            argA="valA",
            job_settings={"CONCURRENT_REQUESTS": 2},
            project_id=999,
            tags=["CHECKED", "JOBSEQ=0000000001"],
        )

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_with_resume(self, mocked_super_schedule_spider):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = "myspider"

            def set_parameters_gen(self):
                parameters_list = [
                    {"argA": "valA"},
                    {"argA": "valB", "spider": "myspidertwo"},
                ]
                for args in parameters_list:
                    yield args

        with script_args(["--flow-id=3a20", "--resume-workflow"]):
            manager = _ListTestManager()

        manager.get_jobs = Mock()
        manager.get_jobs.side_effect = [
            [],
            [
                {
                    "spider": "myspider",
                    "key": "999/1/1",
                    "tags": ["FLOW_ID=3a20", "PARENT_NAME=test", "JOBSEQ=0000000001"],
                    "spider_args": {"argA": "valA"},
                }
            ],
        ]
        mocked_super_schedule_spider.side_effect = ["999/2/1"]
        manager._WorkFlowManager__on_start()

        # first loop: only second task was scheduled. First one already completed before resuming.
        manager.is_finished = lambda x: None
        result = manager.workflow_loop()
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "myspidertwo", argA="valB", tags=["JOBSEQ=0000000002"], job_settings={}, units=None
        )

        # second loop: finished second spider. Finish execution
        manager.is_finished = lambda x: "finished" if x == "999/2/1" else None
        result = manager.workflow_loop()
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        self.assertFalse(result)
