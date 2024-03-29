import os
from unittest import TestCase
from unittest.mock import patch

from shub_workflow.crawl import CrawlManager, PeriodicCrawlManager, GeneratorCrawlManager
from shub_workflow.utils.contexts import script_args
from shub_workflow.script import SpiderName, Outcome


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

    def bad_outcome_hook(self, spider, outcome, job_args_override, jobkey):
        spider_args = job_args_override.setdefault("spider_args", {})
        if "argR" not in spider_args:
            spider_args.update({"argR": "valR"})
            self.add_job(spider, job_args_override)
        elif "argS" not in spider_args:
            spider_args.update({"argS": "valS"})
            self.add_job(spider, job_args_override)


class TestManagerWithSpider(CrawlManager):

    name = "test"
    spider = SpiderName("myimplicitspider")


@patch("shub_workflow.script.BaseScript.get_jobs")
@patch("shub_workflow.script.BaseScript.add_job_tags")
class CrawlManagerTest(TestCase):
    def setUp(self):
        os.environ["SH_APIKEY"] = "ffff"
        os.environ["PROJECT_ID"] = "999"

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):

        with script_args(["myspider"]):
            manager = TestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._on_start()

        # first loop: schedule spider
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # second loop: spider still running. Continue.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)

        # third loop: spider is finished. Stop.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        result = next(manager._run_loops())

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_implicit_spider(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):

        with script_args([]):
            manager = TestManagerWithSpider()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._on_start()

        # first loop: schedule spider
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myimplicitspider", units=None, job_settings={})

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_bad_outcome(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):

        with script_args(["myspider"]):
            manager = TestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._on_start()

        # first loop: schedule spider
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # second loop: spider still running. Continue.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)

        # third loop: spider is cancelled. Stop. Manager must be closed with cancelled close reason
        manager.is_finished = lambda x: Outcome("cancelled") if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        result = next(manager._run_loops())

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)

        self.assertEqual(manager.get_close_reason(), "cancelled")

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_with_resume(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):
        with script_args(["myspider", "--flow-id=3a20"]):
            manager = TestManager()

        mocked_get_jobs_side_effect = [
            # the resumed job
            [{"tags": ["FLOW_ID=3a20", "NAME=test", "OTHER=other"], "key": "999/10/1"}],
            # the owned running job
            [{"spider": "myspider", "key": "999/1/1", "tags": ["FLOW_ID=3a20", "PARENT_NAME=test"]}],
            # the owned finished jobs
            [],
        ]
        mocked_get_jobs.side_effect = mocked_get_jobs_side_effect
        manager._on_start()
        self.assertTrue(manager.is_resumed)
        self.assertEqual(len(manager._running_job_keys), 1)

        for v in manager._running_job_keys.values():
            self.assertEqual(set(v[1].keys()), {"spider_args", "tags"})

        self.assertEqual(mocked_get_jobs.call_count, len(mocked_get_jobs_side_effect))
        mocked_add_job_tags.assert_any_call(tags=["FLOW_ID=3a20", "NAME=test", "OTHER=other"])

        # first loop: spider still running in workflow. Continue.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)

        # second loop: spider is finished. Stop.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/1" else None
        result = next(manager._run_loops())

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_with_resume_not_found(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        with script_args(["myspider", "--flow-id=3a20"]):
            manager = TestManager()

        mocked_get_jobs_side_effect = [
            # the not resumed job (different flow id)
            [{"tags": ["FLOW_ID=3344", "NAME=othertest"], "key": "999/10/1"}],
        ]
        mocked_get_jobs.side_effect = mocked_get_jobs_side_effect
        manager._on_start()
        self.assertFalse(manager.is_resumed)
        self.assertEqual(len(manager._running_job_keys), 0)
        self.assertEqual(mocked_get_jobs.call_count, len(mocked_get_jobs_side_effect))

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_with_resume_not_owned(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        with script_args(["myspider", "--flow-id=3a20"]):
            manager = TestManager()

        mocked_get_jobs.side_effect = [
            # the resumed job
            [{"tags": ["FLOW_ID=3a20", "NAME=test"], "key": "999/10/1"}],
            # the not owned job
            [{"key": "999/1/1", "tags": ["FLOW_ID=3a20", "PARENT_NAME=testa"]}],
            # owned finished jobs
            [],
        ]
        manager._on_start()
        self.assertTrue(manager.is_resumed)
        self.assertEqual(len(manager._running_job_keys), 0)

        # first loop: no spider, schedule one.
        manager.is_finished = lambda x: None
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)

        # second loop: spider is finished. Stop.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/2" else None
        result = next(manager._run_loops())

        self.assertFalse(result)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_periodic(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):
        with script_args(["myspider"]):
            manager = PeriodicTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._on_start()

        # first loop: schedule spider
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # second loop: spider still running. Continue.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)

        # third loop: spider is finished. Schedule again.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        result = next(manager._run_loops())

        self.assertTrue(result)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

        # four loop: spider is cancelled. Schedule again.
        manager.is_finished = lambda x: Outcome("cancelled") if x == "999/1/2" else None
        mocked_super_schedule_spider.reset_mock()
        mocked_super_schedule_spider.side_effect = ["999/1/3"]
        result = next(manager._run_loops())

        self.assertTrue(result)
        mocked_super_schedule_spider.assert_any_call("myspider", units=None, job_settings={})

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_bad_outcome_hook(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        with script_args(["myspider"]):
            manager = ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/1/2", "999/1/3", "999/1/4", "999/1/5"]
        manager._on_start()

        # first loop: schedule spider with first set of arguments
        result = next(manager._run_loops())

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
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)

        # third loop: finish one job. We can schedule last one with third set of arguments
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/1" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argB="valC", tags=["JOBSEQ=0000000003"], job_settings={}
        )

        # fourth loop: waiting jobs to finish
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        # fifth loop: second job finished with failed outcome. Retry according
        # to bad outcome hook
        manager.is_finished = lambda x: Outcome("cancelled (stalled)") if x == "999/1/2" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valB", tags=["JOBSEQ=0000000002.r1"], argR="valR", job_settings={}
        )

        # sixth loop: third job finished.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/3" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)

        # seventh loop: retried job failed again. Retry with new argument.
        manager.is_finished = lambda x: Outcome("memusage_exceeded") if x == "999/1/4" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 5)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valB", tags=["JOBSEQ=0000000002.r2"], argR="valR", job_settings={}
        )

        # eighth loop: retried job finished. Exit.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/5" else None
        result = next(manager._run_loops())
        self.assertFalse(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 5)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_explicit_spider(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = SpiderName("myspider")

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
        manager._on_start()

        # first loop: schedule spider with first set of arguments
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["JOBSEQ=0000000001"], job_settings={}
        )
        mocked_super_schedule_spider.assert_any_call(
            "myspidertwo", units=None, argA="valB", tags=["JOBSEQ=0000000002"], job_settings={}
        )

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_scrapy_cloud_params(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = SpiderName("myspider")

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
        manager._on_start()

        # first loop: schedule spider with first set of arguments
        result = next(manager._run_loops())

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
        jobid = GeneratorCrawlManager.get_job_unique_id(
            {"spider": SpiderName("myspider"), "spider_args": {"argA": "valA"}}
        )
        self.assertTrue(jobid in manager._jobuids)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_with_resume(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = SpiderName("myspider")

            def set_parameters_gen(self):
                parameters_list = [
                    {"argA": "valA"},
                    {"argA": "valB", "spider": "myspidertwo"},
                ]
                for args in parameters_list:
                    yield args

        with script_args(["--flow-id=3a20"]):
            manager = _ListTestManager()

        mocked_get_jobs.side_effect = [
            # the resumed job
            [{"tags": ["FLOW_ID=3a20", "NAME=test"], "key": "999/10/1"}],
            # running spiders
            [],
            # finished spiders
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
        manager._on_start()
        self.assertTrue(manager.is_resumed)
        self.assertEqual(len(manager._running_job_keys), 0)

        # first loop: only second task will be scheduled. First one already completed before resuming.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "myspidertwo", argA="valB", tags=["JOBSEQ=0000000002"], job_settings={}, units=None
        )

        # second loop: finished second spider. Finish execution
        manager.is_finished = lambda x: Outcome("finished") if x == "999/2/1" else None
        result = next(manager._run_loops())
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        self.assertFalse(result)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider_list_resumed_running_job_with_bad_outcome(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        class _ListTestManager(ListTestManager):

            name = "test"
            default_max_jobs = 2
            spider = SpiderName("myspider")

            def set_parameters_gen(self):
                parameters_list = [
                    {"argA": "valA"},
                ]
                for args in parameters_list:
                    yield args

        with script_args(["--flow-id=3a20"]):
            manager = _ListTestManager()

        mocked_get_jobs.side_effect = [
            # the resumed job
            [{"tags": ["FLOW_ID=3a20", "NAME=test"], "key": "999/10/1"}],
            # running spiders
            [
                {
                    "spider": "myspider",
                    "key": "999/1/1",
                    "tags": ["FLOW_ID=3a20", "PARENT_NAME=test", "JOBSEQ=0000000001"],
                    "spider_args": {"argA": "valA"},
                }
            ],
            # finished spiders
            [],
        ]
        mocked_super_schedule_spider.side_effect = ["999/2/1"]
        manager._on_start()
        self.assertTrue(manager.is_resumed)
        self.assertEqual(len(manager._running_job_keys), 1)

        for v in manager._running_job_keys.values():
            self.assertEqual(set(v[1].keys()), {"spider_args", "tags"})

        # first loop: acquire running job.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 0)

        # second loop: second job finished with failed outcome. Retry according
        # to bad outcome hook
        manager.is_finished = lambda x: Outcome("cancelled (stalled)")
        mocked_super_schedule_spider.side_effect = ["999/2/2"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "myspider",
            units=None,
            argA="valA",
            tags=["FLOW_ID=3a20", "PARENT_NAME=test", "JOBSEQ=0000000001.r1"],
            argR="valR",
            job_settings={},
        )

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_default_bad_outcome_no_retry(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 1
            spider = SpiderName("myspider")

            def set_parameters_gen(self):
                parameters_list = [
                    {"argA": "valA"},
                ]
                for args in parameters_list:
                    yield args

        with script_args([]):
            manager = _ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1"]
        manager._on_start()

        # first loop: schedule spider
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["JOBSEQ=0000000001"], job_settings={}
        )

        # second loop: finish job with bad outcome, but there is no retry. Stop.
        manager.is_finished = lambda x: Outcome("cancelled (stalled)") if x == "999/1/1" else None
        result = next(manager._run_loops())
        self.assertFalse(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_default_bad_outcome_with_retries(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):
        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            default_max_jobs = 2
            spider = SpiderName("myspider")

            MAX_RETRIES = 2

            def set_parameters_gen(self):
                parameters_list = [
                    {"argA": "valA"},
                    {"argB": "valB"},
                ]
                for args in parameters_list:
                    yield args

        with script_args([]):
            manager = _ListTestManager()

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/2/1"]
        manager._on_start()

        # first loop: schedule spiders
        result = next(manager._run_loops())

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["JOBSEQ=0000000001"], job_settings={}
        )

        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argB="valB", tags=["JOBSEQ=0000000002"], job_settings={}
        )

        # second loop: finish first job with bad outcome, retry 1.
        manager.is_finished = lambda x: Outcome("cancelled (stalled)") if x == "999/1/1" else None
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["RETRIED_FROM=999/1/1", "JOBSEQ=0000000001.r1"], job_settings={}
        )

        # second loop: second job finishes with "cancelled", don't retry it.
        manager.is_finished = lambda x: Outcome("cancelled") if x == "999/2/1" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        # third loop: first job finishes again with abnormal reason, retry it.
        manager.is_finished = lambda x: Outcome("cancelled (stalled)") if x == "999/1/2" else None
        mocked_super_schedule_spider.side_effect = ["999/1/3"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        mocked_super_schedule_spider.assert_any_call(
            "myspider", units=None, argA="valA", tags=["RETRIED_FROM=999/1/2", "JOBSEQ=0000000001.r2"], job_settings={}
        )

        # fourth loop: first job finishes again with abnormal reason, but max retries reached. Stop.
        manager.is_finished = lambda x: Outcome("cancelled (stalled)") if x == "999/1/3" else None
        result = next(manager._run_loops())
        self.assertFalse(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)

        manager._close()

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_max_jobs_per_spider(self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs):

        """Notice delayed jobs queue got filled in first loop with all next jobs params.
        This is unavoidable if we don't have information about the spiders that
        will come in new next params. If crawl manager stops prematurely, all the delayed
        params will be lost. But effect can be limited. See next test."""

        spider_list = ("spiderA", "spiderB", "spiderC")

        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            max_jobs_per_spider = 1
            spider = SpiderName("")

            def set_parameters_gen(self):
                for i in range(100):
                    for spider in spider_list:
                        yield {"spider": spider, "argA": i}

        with script_args([]):
            manager = _ListTestManager()

        manager._on_start()

        # first loop.

        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/2/1", "999/3/1"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        for seq, spider in enumerate(spider_list, start=1):
            mocked_super_schedule_spider.assert_any_call(
                spider, units=None, argA=0, tags=[f"JOBSEQ={seq:010d}"], job_settings={}
            )
        self.assertEqual(len(manager.get_delayed_jobs()), 297)

        # second loop. No job finished.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)

        # third loop. spiderB job finished. Can schedule new spiderB job.
        mocked_super_schedule_spider.side_effect = ["999/2/2"]
        manager.is_finished = lambda x: Outcome("finished") if x == "999/2/1" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        mocked_super_schedule_spider.assert_any_call(
            "spiderB", units=None, argA=1, tags=["JOBSEQ=0000000004"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 296)

        # fourth loop. spiderB job finished. Can schedule new spiderB job.
        mocked_super_schedule_spider.side_effect = ["999/2/3"]
        manager.is_finished = lambda x: Outcome("finished") if x == "999/2/2" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 5)
        mocked_super_schedule_spider.assert_any_call(
            "spiderB", units=None, argA=2, tags=["JOBSEQ=0000000005"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 295)

        # fifth loop. spiderC job finished. Can schedule new spiderC job.
        mocked_super_schedule_spider.side_effect = ["999/3/2"]
        manager.is_finished = lambda x: Outcome("finished") if x == "999/3/1" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 6)
        mocked_super_schedule_spider.assert_any_call(
            "spiderC", units=None, argA=1, tags=["JOBSEQ=0000000006"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 294)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_max_jobs_per_spider_with_max_next_params_limit(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        """Delayed jobs queue got filled in 5th loop with all next jobs params, when no new
        param is yielded. This is unavoidable if we don't have information about the spiders
        that will come in new next params. If crawl manager stops prematurely, all the delayed
        params will be lost. But effect can be limited further. See next test."""

        spider_list = ("spiderA", "spiderB", "spiderC")

        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            max_jobs_per_spider = 1
            spider = SpiderName("")

            def set_parameters_gen(self):
                for i in range(100):
                    for spider in spider_list:
                        yield {"spider": spider, "argA": i}

            def get_max_next_params(self) -> int:
                return min(1, super().get_max_next_params())

        with script_args([]):
            manager = _ListTestManager()

        manager._on_start()

        # 1st loop. Schedule spiderA
        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/2/1", "999/3/1"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "spiderA", units=None, argA=0, tags=["JOBSEQ=0000000001"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 2nd loop. Schedule spiderB
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call(
            "spiderB", units=None, argA=0, tags=["JOBSEQ=0000000002"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 3rd loop. SpiderA finishes, but spiderC is next.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/1" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
        mocked_super_schedule_spider.assert_any_call(
            "spiderC", units=None, argA=0, tags=["JOBSEQ=0000000003"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 4th loop. Schedule spiderA
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        mocked_super_schedule_spider.assert_any_call(
            "spiderA", units=None, argA=1, tags=["JOBSEQ=0000000004"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 5th loop. Nothing scheduled. But delayed jobs queue got filled with all next jobs params
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        self.assertEqual(len(manager.get_delayed_jobs()), 296)

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_max_jobs_per_spider_with_known_spiders(
        self, mocked_super_schedule_spider, mocked_add_job_tags, mocked_get_jobs
    ):
        """If we limit max next params but also we know which spiders can be scheduled, we can avoid the delayed
        queue fill effect while every spider has slots available. But once one spider run out of params and
        there no running ones (loop 8th in the test), we get again the fill effect. Again, this is unavoidable if we
        don't have information about wether there are spiderA next params available."""

        spider_list = ("spiderA", "spiderB", "spiderC")

        class _ListTestManager(GeneratorCrawlManager):

            name = "test"
            max_jobs_per_spider = 1
            spider = SpiderName("")

            def set_parameters_gen(self):
                for i in range(100):
                    for spider in spider_list:
                        if spider != "spiderA" or i < 4:
                            yield {"spider": spider, "argA": i}

            def get_max_next_params(self) -> int:
                if self.get_running_spiders() == set(spider_list):
                    return 0
                return min(1, super().get_max_next_params())

        with script_args([]):
            manager = _ListTestManager()

        manager._on_start()

        # 1st loop. Schedule spiderA
        mocked_super_schedule_spider.side_effect = ["999/1/1", "999/2/1", "999/3/1"]
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call(
            "spiderA", units=None, argA=0, tags=["JOBSEQ=0000000001"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 2nd loop. Schedule spiderB
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 2)
        mocked_super_schedule_spider.assert_any_call(
            "spiderB", units=None, argA=0, tags=["JOBSEQ=0000000002"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 3rd loop. SpiderA finishes, but spiderC is next.
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/1" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 3)
        mocked_super_schedule_spider.assert_any_call(
            "spiderC", units=None, argA=0, tags=["JOBSEQ=0000000003"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 4th loop. Schedule spiderA
        mocked_super_schedule_spider.side_effect = ["999/1/2"]
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        mocked_super_schedule_spider.assert_any_call(
            "spiderA", units=None, argA=1, tags=["JOBSEQ=0000000004"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 5th loop. Nothing scheduled, and nothing added to delayed jobs.
        manager.is_finished = lambda x: None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 4)
        self.assertEqual(len(manager.get_delayed_jobs()), 0)

        # 6th loop. SpiderA finishes and rescheduled. SpiderB and SpiderC need to go to delayed jobs.
        mocked_super_schedule_spider.side_effect = ["999/1/3"]
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/2" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 5)
        mocked_super_schedule_spider.assert_any_call(
            "spiderA", units=None, argA=2, tags=["JOBSEQ=0000000005"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 2)
        self.assertEqual(manager.get_delayed_spiders(), {"spiderB", "spiderC"})

        # 7th loop. SpiderA finishes, reschedule one more. Two more spiderB and spiderC next params added to
        # delayed jobs
        mocked_super_schedule_spider.side_effect = ["999/1/4"]
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/3" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 6)
        mocked_super_schedule_spider.assert_any_call(
            "spiderA", units=None, argA=3, tags=["JOBSEQ=0000000006"], job_settings={}
        )
        self.assertEqual(len(manager.get_delayed_jobs()), 4)
        self.assertEqual(manager.get_delayed_spiders(), {"spiderB", "spiderC"})

        # 8th loop. SpiderA finishes, but there are no more next params for spiderA. Delayed job become filled
        # with all next params
        manager.is_finished = lambda x: Outcome("finished") if x == "999/1/4" else None
        result = next(manager._run_loops())
        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 6)
        self.assertEqual(len(manager.get_delayed_jobs()), 198)
        self.assertEqual(manager.get_delayed_spiders(), {"spiderB", "spiderC"})
