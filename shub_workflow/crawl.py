"""
Base script class for spiders crawl managers.
"""
import abc
import json
import asyncio
import logging
from copy import deepcopy
from argparse import Namespace
from typing import Optional, List, Tuple, Dict, NewType, cast, Generator, Any, AsyncGenerator

from bloom_filter2 import BloomFilter
from typing_extensions import TypedDict, NotRequired, Protocol

from shub_workflow.script import (
    JobKey,
    JobDict,
    SpiderName,
    Outcome,
    BaseLoopScriptAsyncMixin,
)
from shub_workflow.base import WorkFlowManager
from shub_workflow.utils import hashstr
from shub_workflow.utils.dupefilter import DupesFilterProtocol


_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


SpiderArgs = NewType("SpiderArgs", Dict[str, str])


class JobParams(TypedDict):
    units: NotRequired[int]
    tags: NotRequired[List[str]]
    job_settings: NotRequired[Dict[str, str]]
    project_id: NotRequired[int]
    spider_args: NotRequired[Dict[str, str]]


class FullJobParams(JobParams):
    spider: NotRequired[SpiderName]


def get_spider_args_from_params(params: JobParams) -> SpiderArgs:
    spider_args = params.get("spider_args", None) or {}
    result = cast(SpiderArgs, deepcopy(params))
    for key in tuple(JobParams.__annotations__):
        result.pop(key, None)
    result.update(spider_args)
    return result


def get_jobseq(tags: List[str]) -> Tuple[int, int]:
    for tag in tags:
        if tag.startswith("JOBSEQ="):
            tag = tag.replace("JOBSEQ=", "")
            jobseq, *rep = tag.split(".r")
            rep.append("0")
            return int(jobseq), int(rep[0])
    return 0, 0


class CrawlManager(WorkFlowManager):
    """
    Schedules a single spider job. If loop mode is enabled, it will shutdown only after the scheduled spider
    finished. Close reason of the manager will be inherited from spider one.
    """

    spider: Optional[SpiderName] = None

    def __init__(self):
        super().__init__()

        # running jobs represents the state of a crawl manager.
        # a dict job key : (spider, job_args_override)
        self._running_job_keys: Dict[JobKey, Tuple[SpiderName, JobParams]] = {}

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

    def parse_args(self) -> Namespace:
        args = super().parse_args()
        if self.spider is None:
            self.spider = args.spider
        return args

    def get_job_settings(self, override: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        job_settings = json.loads(self.args.job_settings)
        if override:
            job_settings.update(override)
        return job_settings

    def schedule_spider_with_jobargs(
        self,
        job_args_override: Optional[JobParams] = None,
        spider: Optional[SpiderName] = None,
    ) -> Optional[JobKey]:
        job_args_override = job_args_override or {}
        spider = spider or self.spider
        if spider is not None:
            schedule_args: Dict[str, Any] = json.loads(self.args.spider_args)
            spider_args = get_spider_args_from_params(job_args_override)
            schedule_args.update(job_args_override)
            schedule_args.pop("spider_args", None)
            schedule_args.update(spider_args)

            job_settings_override = schedule_args.get("job_settings", None)
            schedule_args["job_settings"] = self.get_job_settings(job_settings_override)
            schedule_args["units"] = schedule_args.get("units", self.args.units)
            jobkey = self.schedule_spider(spider, **schedule_args)
            if jobkey is not None:
                self._running_job_keys[jobkey] = spider, job_args_override
                self.stats.inc_value("crawlmanager/scheduled_jobs")
            return jobkey
        return None

    def check_running_jobs(self) -> Dict[JobKey, Outcome]:
        outcomes = {}
        running_job_keys = list(self._running_job_keys)
        while running_job_keys:
            jobkey = running_job_keys.pop()
            if (outcome := self.is_finished(jobkey)) is not None:
                spider, job_args_override = self._running_job_keys.pop(jobkey)
                if outcome in self.failed_outcomes:
                    _LOG.warning(f"Job {jobkey} finished with outcome {outcome}.")
                    if job_args_override is not None:
                        job_args_override = job_args_override.copy()
                    self.bad_outcome_hook(spider, outcome, job_args_override, jobkey)
                else:
                    self.finished_ok_hook(spider, outcome, job_args_override, jobkey)
                outcomes[jobkey] = outcome
        _LOG.info(f"There are {len(self._running_job_keys)} jobs still running.")

        return outcomes

    def bad_outcome_hook(self, spider: SpiderName, outcome: Outcome, job_args_override: JobParams, jobkey: JobKey):
        if self.get_close_reason() is None:
            self.set_close_reason(outcome)

    def finished_ok_hook(self, spider: SpiderName, outcome: Outcome, job_args_override: JobParams, jobkey: JobKey):
        pass

    def workflow_loop(self) -> bool:
        outcomes = self.check_running_jobs()
        if outcomes:
            return False
        if not self._running_job_keys:
            self.schedule_spider_with_jobargs()
        return True

    def resume_running_job_hook(self, job: JobDict):
        key = job["key"]
        job_args_override = cast(JobParams, job.get("spider_args", {}).copy())
        job_args_override["tags"] = job["tags"]
        self._running_job_keys[key] = job["spider"], job_args_override
        _LOG.info(f"added running job {key}")

    def on_close(self):
        job = self.get_job()
        if job:
            close_reason = self.get_close_reason()
            self.finish(close_reason=close_reason)


class PeriodicCrawlManager(CrawlManager):
    """
    Schedule a spider periodically, waiting for the previous job to finish before scheduling it again with same
    parameters. Don't forget to set loop mode.
    """

    def bad_outcome_hook(self, spider: str, outcome: Outcome, job_args_override: JobParams, jobkey: JobKey):
        pass

    def workflow_loop(self) -> bool:
        self.check_running_jobs()
        if not self._running_job_keys:
            self.schedule_spider_with_jobargs()
        return True

    def on_close(self):
        pass


class GeneratorCrawlManagerProtocol(Protocol):

    _running_job_keys: Dict[JobKey, Tuple[SpiderName, JobParams]]
    _jobuids: DupesFilterProtocol
    spider: Optional[SpiderName]

    @property
    @abc.abstractmethod
    def max_running_jobs(self) -> int:
        ...

    @abc.abstractmethod
    def _workflow_step_gen(self, max_next_params: int) -> Generator[Tuple[str, Optional[JobKey]], None, None]:
        ...

    @abc.abstractmethod
    def check_running_jobs(self) -> Dict[JobKey, Outcome]:
        ...

    @abc.abstractmethod
    def schedule_spider_with_jobargs(
        self,
        job_args_override: Optional[JobParams] = None,
        spider: Optional[SpiderName] = None,
    ) -> Optional[JobKey]:
        ...

    @abc.abstractmethod
    def get_job_settings(self, override: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        ...


class GeneratorCrawlManager(CrawlManager, GeneratorCrawlManagerProtocol):
    """
    Schedule a spider periodically, each time with different parameters yielded by a generator, until stop.
    Number of simultaneos spider jobs will be limited by max running jobs (see WorkFlowManager).
    Instructions:
    - Override set_parameters_gen() method. It must implement a generator of dictionaries, each one being
      the spider arguments (argument name: argument value) passed to the spider on each successive job.
    - Don't forget to set loop mode and max jobs (which defaults to infinite).
    """

    MAX_RETRIES = 0
    max_jobs_per_spider = 1000

    def __init__(self):
        super().__init__()
        self.__parameters_gen = self.set_parameters_gen()
        self.__additional_jobs: List[FullJobParams] = []
        self.__delayed_jobs: List[FullJobParams] = []
        self.__next_job_seq = 1
        self._jobuids = self.create_dupe_filter()

    @classmethod
    def create_dupe_filter(cls) -> DupesFilterProtocol:
        return BloomFilter(max_elements=1e6, error_rate=1e-6)

    def spider_running_count(self, spider: SpiderName) -> int:
        count = 0
        for spidername, _ in self._running_job_keys.values():
            if spidername == spider:
                count += 1
        return count

    def bad_outcome_hook(self, spider: SpiderName, outcome: Outcome, job_args_override: JobParams, jobkey: JobKey):
        if outcome == "cancelled":
            return
        if self.MAX_RETRIES == 0:
            return
        spider_args = job_args_override.setdefault("spider_args", {})
        tags = job_args_override["tags"]
        retries = get_jobseq(tags)[1]
        if retries < self.MAX_RETRIES:
            for t in list(tags):
                if t.startswith("RETRIED_FROM="):
                    tags.remove(t)
                    break
            tags.append(f"RETRIED_FROM={jobkey}")
            spider_args["retry_num"] = str(retries + 1)
            self.add_job(spider, job_args_override)
            _LOG.info(f"Job {jobkey} failed with reason '{outcome}'. Retrying ({retries + 1} of {self.MAX_RETRIES}).")

    def add_job(self, spider: SpiderName, job_args_override: JobParams):
        params = cast(FullJobParams, (job_args_override or {}).copy())
        params["spider"] = spider
        self.__additional_jobs.append(params)

    @abc.abstractmethod
    def set_parameters_gen(self) -> Generator[Dict[str, Any], None, None]:
        ...

    def __add_jobseq_tag(self, params: FullJobParams):
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

    def _workflow_step_gen(self, max_next_params: int) -> Generator[Tuple[str, Optional[JobKey]], None, None]:
        new_params = []
        for _ in range(max_next_params):
            try:
                if self.__delayed_jobs:
                    next_params = self.__delayed_jobs.pop(0)
                elif self.__additional_jobs:
                    next_params = self.__additional_jobs.pop(0)
                else:
                    next_params = next(self.__parameters_gen)
                new_params.append(next_params)
            except StopIteration:
                break

        running_counts: Dict[SpiderName, int] = {}
        for next_params in new_params:
            spider = next_params.get("spider", self.spider)
            assert spider, f"No spider set for parameters {next_params}"
            next_params["spider"] = spider
            if spider not in running_counts:
                running_counts[spider] = self.spider_running_count(spider)

        for next_params in new_params:
            if running_counts.setdefault(next_params["spider"], 0) >= self.max_jobs_per_spider:
                self.__delayed_jobs.append(next_params)
                continue
            spider = next_params.pop("spider")
            running_counts[spider] += 1
            spider_args = get_spider_args_from_params(next_params)
            jobuid = self.get_job_unique_id({"spider": spider, "spider_args": spider_args})
            if jobuid not in self._jobuids:
                self.__add_jobseq_tag(next_params)
                yield jobuid, self.schedule_spider_with_jobargs(next_params, spider)
            else:
                _LOG.warning(f"Job with spider {spider} and parameters {next_params} was already scheduled. Skipped.")

    def workflow_loop(self) -> bool:
        self.check_running_jobs()
        max_next_params = self.max_running_jobs - len(self._running_job_keys)
        retval = False
        for jobuid, jobid in self._workflow_step_gen(max_next_params):
            retval = True
            if jobid is not None:
                self._jobuids.add(jobuid)
        return retval or bool(self._running_job_keys)

    @staticmethod
    def get_job_unique_id(job: JobDict) -> str:
        jdict = job.get("spider_args", {}).copy()
        jdict["spider"] = job["spider"]
        for k, v in jdict.items():
            jdict[k] = str(v)
        jid = json.dumps(jdict, sort_keys=True)
        return hashstr(jid)

    def resume_running_job_hook(self, job: JobDict):
        super().resume_running_job_hook(job)
        jobuid = self.get_job_unique_id(job)
        self._jobuids.add(jobuid)
        self.__next_job_seq = max(self.__next_job_seq, get_jobseq(job["tags"])[0] + 1)

    def resume_finished_job_hook(self, job: JobDict):
        jobuid = self.get_job_unique_id(job)
        if jobuid not in self._jobuids:
            self._jobuids.add(jobuid)
        self.__next_job_seq = max(self.__next_job_seq, get_jobseq(job["tags"])[0] + 1)

    def resume_workflow(self):
        super().resume_workflow()
        _LOG.info(f"Next job sequence number: {self.__next_job_seq}")

    def on_close(self):
        self._jobuids.close()


class AsyncSchedulerCrawlManagerMixin(BaseLoopScriptAsyncMixin, GeneratorCrawlManagerProtocol):
    async def _async_workflow_step_gen(self, max_next_params: int) -> AsyncGenerator[Tuple[str, JobKey], None]:
        jobuids_cors = list(self._workflow_step_gen(max_next_params))
        if jobuids_cors:
            jobuids, cors = zip(*jobuids_cors)
            results: List[JobKey] = await asyncio.gather(*cors)
            for jobuid, jobid in zip(jobuids, results):
                yield jobuid, jobid

    async def workflow_loop(self) -> bool:  # type: ignore
        self.check_running_jobs()
        max_next_params = self.max_running_jobs - len(self._running_job_keys)
        retval = False
        async for jobuid, jobid in self._async_workflow_step_gen(max_next_params):
            retval = True
            if jobid is not None:
                self._jobuids.add(jobuid)
        return retval or bool(self._running_job_keys)

    async def schedule_spider_with_jobargs(  # type: ignore
        self,
        job_args_override: Optional[JobParams] = None,
        spider: Optional[SpiderName] = None,
    ) -> Optional[JobKey]:
        job_args_override = job_args_override or {}
        spider = spider or self.spider
        assert spider is not None
        schedule_args: Dict[str, Any] = json.loads(self.args.spider_args)
        spider_args = get_spider_args_from_params(job_args_override)
        schedule_args.update(job_args_override)
        schedule_args.pop("spider_args", None)
        schedule_args.update(spider_args)

        job_settings_override = schedule_args.get("job_settings", None)
        schedule_args["job_settings"] = self.get_job_settings(job_settings_override)
        schedule_args["units"] = schedule_args.get("units", self.args.units)
        jobkey = await self.async_schedule_spider(spider, **schedule_args)
        if jobkey is not None:
            self._running_job_keys[jobkey] = spider, job_args_override
            self.stats.inc_value("crawlmanager/scheduled_jobs")
        return jobkey
