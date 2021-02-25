"""
Base script class for spiders crawl managers.
"""
import json
import logging


from shub_workflow.base import WorkFlowManager


_LOG = logging.getLogger(__name__)


class CrawlManager(WorkFlowManager):
    """
    Schedules a single spider job. If loop mode is enabled, it will shutdown only after the scheduled spider
    finished. Close reason of the manager will be inherited from spider one.
    """

    def __init__(self):
        super().__init__()
        self._running_job_keys = []
        self._bad_outcomes = {}

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("spider", help="Spider name")
        self.argparser.add_argument(
            "--spider-args", help="Spider arguments dict in json format", default="{}"
        )
        self.argparser.add_argument(
            "--job-settings", help="Job settings dict in json format", default="{}"
        )
        self.argparser.add_argument(
            "--units", help="Set number of ScrapyCloud units for each job", type=int
        )

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
        spider_args["job_settings"] = job_settings
        self._running_job_keys.append(
            super().schedule_spider(
                self.args.spider, units=self.args.units, **spider_args
            )
        )

    def check_running_jobs(self):
        outcomes = {}
        for jobkey in list(self._running_job_keys):
            outcome = self.is_finished(jobkey)
            if outcome is None:
                continue
            _LOG.info(f"Job {jobkey} finished with outcome {outcome}.")
            if outcome == "finished":
                self._running_job_keys.remove(jobkey)
            else:
                self._bad_outcomes[jobkey] = outcome
            outcomes[jobkey] = outcome
            return outcomes

    def workflow_loop(self):
        outcomes = self.check_running_jobs()
        if outcomes:
            return False
        elif not self._running_job_keys:
            self.schedule_spider()
        return True

    def on_close(self):
        job = self.get_job()
        if job:
            close_reason = None
            for outcome in self._bad_outcomes.values():
                close_reason = outcome
                break
            self.finish(job.key, close_reason=close_reason)


class PeriodicCrawlManager(CrawlManager):
    """
    Schedule a spider periodically, waiting for the previous job to finish before scheduling it again with same
    parameters. Don't forget to set loop_mode.
    """

    def workflow_loop(self):
        self.check_running_jobs()
        if not self._running_job_keys:
            self.schedule_spider()
        return True

    def on_close(self):
        pass
