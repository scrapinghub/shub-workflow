"""
Script for easy managing of consumer spiders from multiple slots. It checks available slots and schedules
a job for each one.
"""
import json

from shub_workflow.base import WorkFlowManager


class CrawlManager(WorkFlowManager):

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument('spider', help='Spider name')
        self.argparser.add_argument('--spider-args', help='Spider arguments dict in json format', default='{}')
        self.argparser.add_argument('--job-settings', help='Job settings dict in json format', default='{}')
        self.argparser.add_argument('--tag', help='Add given tag to the job. Can be given multiple times.',
                                    action='append')
        self.argparser.add_argument('--units', help='Set number of ScrapyCloud units for each job', type=int)

    def get_spider_args(self, override=None):
        spider_args = json.loads(self.args.spider_args)
        if override:
            spider_args.update(override)
        return spider_args

    def get_job_settings(self, override=None):
        job_settings = json.loads(self.args.job_settings)
        if override:
            job_settings.update(override)
        return job_settings

    def schedule_spider(self, spider_args_override=None, job_settings_override=None):
        spider_args = self.get_spider_args(spider_args_override)
        job_settings = self.get_job_settings(job_settings_override)
        spider_args['job_settings'] = job_settings
        return super().schedule_spider(self.args.spider, tags=self.args.tag, units=self.args.units, **spider_args)

    def workflow_loop(self):
        self.schedule_spider()
        return True
