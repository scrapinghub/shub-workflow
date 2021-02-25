import os
from unittest import TestCase
from unittest.mock import patch, Mock, call

from shub_workflow.crawl import CrawlManager

from .utils.contexts import script_args


class TestManager(CrawlManager):

    name = 'test'
    
class CrawlManagerTest(TestCase):

    def setUp(self):
        os.environ['SH_APIKEY'] = 'ffff'
        os.environ['PROJECT_ID'] = '999'

    @patch("shub_workflow.crawl.WorkFlowManager.schedule_spider")
    def test_schedule_spider(self, mocked_super_schedule_spider):
        with script_args(['myspider']):
            manager = TestManager()

        mocked_super_schedule_spider.side_effect = ['999/1/1']
        manager.on_start()


        # first loop: schedule spider
        result = manager.workflow_loop()

        self.assertTrue(result)
        self.assertEqual(mocked_super_schedule_spider.call_count, 1)
        mocked_super_schedule_spider.assert_any_call('myspider', units=None, job_settings={})

        # second loop: spider is finished. Stop.
        manager.is_finished = lambda x: "finished" if x == "999/1/1" else None
        mocked_super_schedule_spider.reset_mock()
        result = manager.workflow_loop()

        self.assertFalse(result)
        self.assertFalse(mocked_super_schedule_spider.called)
