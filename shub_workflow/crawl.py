"""
Base script class for spiders crawl managers.
"""
import abc
import json
import logging
from random import shuffle

from bloom_filter import BloomFilter

from shub_workflow.base import WorkFlowManager
from shub_workflow.utils import hashstr


_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


def get_spider_args_from_params(params):
    spider_args = params.copy()
    for key in "spider", "units", "project_id", "tags":
        spider_args.pop(key, None)
    return spider_args


def get_jobseq(tags):
    jobseq, repetition = 0, 0
    for tag in tags:
        if tag.startswith("JOBSEQ="):
            tag = tag.replace("JOBSEQ=", "")
            jobseq, *rep = tag.split(".r")
            rep.append(0)
            repetition = int(rep[0])
            jobseq = int(jobseq)
    return jobseq, repetition


class CrawlManager(WorkFlowManager):
    """
    Schedules a single spider job. If loop mode is enabled, it will shutdown only after the scheduled spider
    finished. Close reason of the manager will be inherited from spider one.
    """

    spider = None
    MIN_CHECK_JOBS = 20

    def __init__(self):
        super().__init__()
        self.__close_reason = None

        # running jobs represents the state of a crawl manager.
        # a dict job key : (spider, spider_args_override)
        self._running_job_keys = {}

    @property
    def description(self):
        return self.__doc__

    def add_argparser_options(self):
        super().add_argparser_options()
        if self.spider is None:
            self.argparser.add_argument("spider", help="Spider name")
        self.argparser.add_argument("--spider-args", help="Spider arguments dict in json format", default="{}")
        self.argparser.add_argument("--job-settings", help="Job settings dict in json format", default="{}")
        self.argparser.add_argument("--units", help="Set default number of ScrapyCloud units for each job", type=int)

    def parse_args(self):
        args = super().parse_args()
        if self.spider is None:
            self.spider = args.spider
        return args

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

    def schedule_spider(self, spider=None, spider_args_override=None):
        if spider_args_override is not None:
            spider_args_override.copy()
        spider = spider or self.spider
        spider_args = self.get_spider_args(spider_args_override)
        job_settings_override = spider_args.pop("job_settings", None)
        job_settings = self.get_job_settings(job_settings_override)
        spider_args["job_settings"] = job_settings
        spider_args["units"] = spider_args.get("units", self.args.units)
        jobkey = super().schedule_spider(spider, **spider_args)
        if jobkey is not None:
            self._running_job_keys[jobkey] = spider, spider_args_override
        return jobkey

    def check_running_jobs(self):
        outcomes = {}
        running_job_keys = list(self._running_job_keys)
        shuffle(running_job_keys)
        removed = 0
        for count, jobkey in enumerate(running_job_keys, start=1):
            if (outcome := self.is_finished(jobkey)) is not None:
                _LOG.info(f"Job {jobkey} finished with outcome {outcome}.")
                spider, spider_args_override = self._running_job_keys.pop(jobkey)
                removed += 1
                if outcome in self.failed_outcomes:
                    if spider_args_override is not None:
                        spider_args_override = spider_args_override.copy()
                    self.bad_outcome_hook(spider, outcome, spider_args_override, jobkey)
                outcomes[jobkey] = outcome
            else:
                _LOG.info(f"Job {jobkey} still running.")
                # if some jobs are running don't waste unneeded requests to get status of other jobs.
                # this is particularly important on crawl managers that handle hundreds of jobs in parallel.
                # so here we limit number of checked jobs.
                # However, we also need to ensure that a minimal number of jobs are checked for faster
                # detection of free slots and scheduling of new jobs.
                if count - removed > self.MIN_CHECK_JOBS:
                    break

        return outcomes

    def bad_outcome_hook(self, spider, outcome, spider_args_override, jobkey):
        if self.__close_reason is None:
            self.__close_reason = outcome

    def workflow_loop(self):
        outcomes = self.check_running_jobs()
        if outcomes:
            return False
        elif not self._running_job_keys:
            self.schedule_spider()
        return True

    def resume_workflow(self):
        running_jobs = []
        count = 0
        for job in self.get_owned_jobs(state=["running", "pending"], meta=["spider_args", "tags", "spider"]):
            key = job["key"]
            spider_args_override = job.get("spider_args", {}).copy()
            spider_args_override["tags"] = job["tags"]
            self._running_job_keys[key] = job["spider"], spider_args_override
            _LOG.info(f"added running job {key}")
            running_jobs.append(job)
            count += 1
        _LOG.info(f"Added a total of {count} running jobs.")
        return running_jobs

    def on_close(self):
        job = self.get_job()
        if job:
            close_reason = self.__close_reason
            self.finish(close_reason=close_reason)


class PeriodicCrawlManager(CrawlManager):
    """
    Schedule a spider periodically, waiting for the previous job to finish before scheduling it again with same
    parameters. Don't forget to set loop mode.
    """

    def bad_outcome_hook(self, spider, outcome, spider_args_override, jobkey):
        pass

    def workflow_loop(self):
        self.check_running_jobs()
        if not self._running_job_keys:
            self.schedule_spider()
        return True

    def on_close(self):
        pass


class GeneratorCrawlManager(CrawlManager):
    """
    Schedule a spider periodically, each time with different parameters yielded by a generator, until stop.
    Number of simultaneos spider jobs will be limited by max running jobs (see WorkFlowManager).
    Instructions:
    - Override set_parameters_gen() method. It must implement a generator of dictionaries, each one being
      the spider arguments (argument name: argument value) passed to the spider on each successive job.
    - Don't forget to set loop mode and max jobs (which defaults to infinite).
    """

    MAX_TOTAL_JOBS = 1000000

    def __init__(self):
        super().__init__()
        self.__parameters_gen = self.set_parameters_gen()
        self.__additional_jobs = []
        self.__next_job_seq = 1
        self.__jobids = BloomFilter(max_elements=self.MAX_TOTAL_JOBS, error_rate=0.001)

    def bad_outcome_hook(self, spider, outcome, spider_args_override, jobkey):
        pass

    def add_job(self, spider, spider_args_override):
        params = (spider_args_override or {}).copy()
        params["spider"] = spider
        self.__additional_jobs.append(params)

    @abc.abstractmethod
    def set_parameters_gen(self):
        for i in []:
            yield i

    def __add_jobseq_tag(self, params):
        tags = params.setdefault("tags", [])
        jobseq_tag = None
        for tag in tags:
            if tag.startswith("JOBSEQ="):
                jobseq_tag = tag
                break
        if jobseq_tag is None:
            jobseq_tag = f"JOBSEQ={self.__next_job_seq:010d}"
            self.__next_job_seq += 1
        else:
            tags.remove(jobseq_tag)
            jobseq, repn = get_jobseq([jobseq_tag])
            jobseq_tag = f"JOBSEQ={jobseq:010d}.r{repn + 1}"
        tags.append(jobseq_tag)

    def workflow_loop(self):
        self.check_running_jobs()
        while len(self._running_job_keys) < self.max_running_jobs:
            try:
                if self.__additional_jobs:
                    next_params = self.__additional_jobs.pop(0)
                else:
                    next_params = next(self.__parameters_gen)
                spider = next_params.pop("spider", self.spider)
                assert spider is not None, "No spider set."
                spider_args = get_spider_args_from_params(next_params)
                jobid = self.get_job_id({"spider": spider, "spider_args": spider_args})
                if jobid not in self.__jobids:
                    self.__add_jobseq_tag(next_params)
                    self.schedule_spider(spider=spider, spider_args_override=next_params)
                    self.__jobids.add(jobid)
            except StopIteration:
                if self._running_job_keys:
                    break
                else:
                    return False
        return True

    @staticmethod
    def get_job_id(job):
        jdict = job.get("spider_args", {}).copy()
        jdict["spider"] = job["spider"]
        for k, v in jdict.items():
            jdict[k] = str(v)
        jid = json.dumps(jdict, sort_keys=True)
        return hashstr(jid)

    def resume_workflow(self):
        for job in super().resume_workflow():
            jobid = self.get_job_id(job)
            self.__jobids.add(jobid)
            self.__next_job_seq = max(self.__next_job_seq, get_jobseq(job["tags"])[0] + 1)
        count = 0
        diff_count = 0
        for job in self.get_owned_jobs(spider=self.spider, state=["finished"], meta=["spider_args", "tags", "spider"]):
            jobid = self.get_job_id(job)
            if jobid not in self.__jobids:
                diff_count += 1
                self.__jobids.add(jobid)
            self.__next_job_seq = max(self.__next_job_seq, get_jobseq(job["tags"])[0] + 1)
            count += 1
        _LOG.info(f"Added a total of {count} completed jobs ({diff_count} different ones)")
        _LOG.info(f"Next job sequence number: {self.__next_job_seq}")

    def on_close(self):
        pass
