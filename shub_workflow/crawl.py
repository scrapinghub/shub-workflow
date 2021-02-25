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
    parameters. Don't forget to set loop mode.
    """

    def workflow_loop(self):
        self.check_running_jobs()
        if not self._running_job_keys:
            self.schedule_spider()
        return True

    def on_close(self):
        pass


class ListCrawlManager(PeriodicCrawlManager):
    """
    Schedule a spider periodically, each time with different parameters taken from a list, until emptied.
    Number of simultaneos spider jobs will be limited by max running jobs (see WorkFlowManager).
    Instructions:
    - Override set_parameters_list() method.
    - Don't forget to set loop mode and max jobs (which defaults to infinite).
    - The parameters list is a list of dictionaries, each one being the spider arguments passed to the spider
      on each successive job.
    """

    def __init__(self):
        super().__init__()
        self.parameters_list = None

    def set_parameters_list(self):
        pass

    def on_start(self):
        self.set_parameters_list()
        assert (
            self.parameters_list is not None
        ), "You must implement set_parameters_list() method."

    def workflow_loop(self):
        self.check_running_jobs()
        if self.parameters_list:
            if len(self._running_job_keys) < self.max_running_jobs:
                self.schedule_spider(spider_args_override=self.parameters_list.pop(0))
        elif not self._running_job_keys:
            return False
        return True
