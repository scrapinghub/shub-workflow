"""
Base script class for spiders crawl managers.
"""
import abc
import json
import asyncio
import logging
from copy import deepcopy
from argparse import Namespace
from typing import Optional, List, Tuple, Dict, NewType, cast, Generator, Any, AsyncGenerator, Set, Protocol, TypedDict

from bloom_filter2 import BloomFilter

from shub_workflow.script import (
    JobKey,
    JobDict,
    JobMeta,
    SpiderName,
    Outcome,
    BaseLoopScriptAsyncMixin,
)
from shub_workflow.base import WorkFlowManager
from shub_workflow.utils import hashstr
from shub_workflow.utils.dupefilter import DupesFilterProtocol
from shub_workflow.utils.monitor import SpiderStatsAggregatorMixin


_LOG = logging.getLogger(__name__)


SpiderArgs = NewType("SpiderArgs", Dict[str, str])
ScheduleArgs = NewType("ScheduleArgs", Dict[str, Any])


class JobParams(TypedDict, total=False):
    units: int
    tags: List[str]
    job_settings: Dict[str, str]
    project_id: int
    spider_args: Dict[str, str]


class FullJobParams(JobParams, total=False):
    spider: SpiderName


class StopRetry(Exception):
    pass


def get_jobseq(tags: List[str]) -> Tuple[int, int]:
    """
    returns tuple (job sequence, retry index)
    """
    for tag in tags:
        if tag.startswith("JOBSEQ="):
            tag = tag.replace("JOBSEQ=", "")
            jobseq, *rep = tag.split(".r")
            rep.append("0")
            return int(jobseq), int(rep[0])
    return 0, 0


class CrawlManagerProtocol(Protocol):
    _running_job_keys: Dict[JobKey, Tuple[SpiderName, JobParams]]

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


class CrawlManager(SpiderStatsAggregatorMixin, WorkFlowManager, CrawlManagerProtocol):
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
            schedule_args: ScheduleArgs = json.loads(self.args.spider_args)
            spider_args = job_args_override.get("spider_args") or {}
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

                metadata = self.get_job_metadata(jobkey)
                self.finished_metadata_hook(jobkey, metadata)

                spider, job_args_override = self._running_job_keys.pop(jobkey)
                if outcome in self.failed_outcomes:
                    _LOG.warning(f"Job {jobkey} finished with outcome {outcome}.")
                    if job_args_override is not None:
                        job_args_override = job_args_override.copy()
                    self.bad_outcome_hook(spider, outcome, job_args_override, jobkey)
                else:
                    self.finished_ok_hook(spider, outcome, job_args_override, jobkey)
                outcomes[jobkey] = outcome

                scrapystats = self.get_metadata_key(metadata, "scrapystats")
                self.aggregate_spider_stats(JobDict(spider=spider, key=jobkey, scrapystats=scrapystats))
        _LOG.info(f"There are {len(self._running_job_keys)} jobs still running.")

        return outcomes

    def finished_metadata_hook(self, jobkey: JobKey, metadata: JobMeta):
        """
        allow to add some reaction on each finished job, based solely on its metadata.
        Use self.get_metadata_key(metadata, <key>) in order to get metadata with handled retries.
        """

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
        spider_args = job.get("spider_args", {}).copy()
        job_args_override = JobParams(
            {
                "tags": job["tags"],
                "spider_args": spider_args,
            }
        )
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


class GeneratorCrawlManagerProtocol(CrawlManagerProtocol, Protocol):

    _jobuids: DupesFilterProtocol
    spider: Optional[SpiderName]

    @abc.abstractmethod
    def _workflow_step_gen(self, max_next_params: int) -> Generator[Tuple[str, Optional[JobKey]], None, None]:
        ...

    @abc.abstractmethod
    def get_max_next_params(self) -> int:
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

    def __init__(self):
        super().__init__()
        self.__parameters_gen: Generator[ScheduleArgs, None, None] = self.set_parameters_gen()
        self.__additional_jobs: List[FullJobParams] = []
        self.__delayed_jobs: List[FullJobParams] = []
        self.__next_job_seq = 1
        self._jobuids = self.create_dupe_filter()

    def get_delayed_jobs(self) -> List[FullJobParams]:
        return deepcopy(self.__delayed_jobs)

    def add_delayed_jobs(self, params: FullJobParams):
        self.__delayed_jobs.append(params)

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
            try:
                retry_override = self.get_retry_override(spider, outcome, job_args_override, jobkey)
                for rtag in retry_override.pop("tags", []):
                    if rtag not in tags:
                        tags.append(rtag)
                job_args_override.setdefault("spider_args", {}).update(retry_override.pop("spider_args", {}))
                job_args_override.setdefault("job_settings", {}).update(retry_override.pop("job_settings", {}))
                job_args_override.update(retry_override)
            except StopRetry as e:
                _LOG.info(f"Job {jobkey} failed with reason '{outcome}'. Will not be retried. Reason: {e}")
            else:
                self.add_job(spider, job_args_override)
                _LOG.info(
                    f"Job {jobkey} failed with reason '{outcome}'. Retrying ({retries + 1} of {self.MAX_RETRIES})."
                )

    def get_retry_override(
        self, spider: SpiderName, outcome: Outcome, job_args_override: JobParams, jobkey: JobKey
    ) -> JobParams:
        return JobParams({})

    def add_job(self, spider: SpiderName, job_args_override: JobParams):
        params = cast(FullJobParams, (job_args_override or {}).copy())
        params["spider"] = spider
        self.__additional_jobs.append(params)

    @abc.abstractmethod
    def set_parameters_gen(self) -> Generator[ScheduleArgs, None, None]:
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

    def get_running_spiders(self) -> Set[SpiderName]:
        return set(sp for sp, _ in self._running_job_keys.values())

    def get_delayed_spiders(self) -> Set[SpiderName]:
        return set(np["spider"] for np in self.__delayed_jobs)

    def spider_delayed_count(self, spider: SpiderName) -> int:
        count = 0
        for np in self.__delayed_jobs:
            if np["spider"] == spider:
                count += 1
        return count

    def can_schedule_job_with_params(self, params: FullJobParams) -> bool:
        """
        If returns False, delay the scheduling of the params.
        """
        return True

    def _fulljobparams_from_spiderargs(self, schedule_args: ScheduleArgs) -> FullJobParams:
        spider = schedule_args.get("spider", self.spider)
        spider_args = deepcopy(schedule_args)
        for key in tuple(FullJobParams.__annotations__):
            spider_args.pop(key, None)
        result = FullJobParams(
            {
                "spider": spider,
                "spider_args": spider_args,
            }
        )
        if "units" in schedule_args:
            result["units"] = schedule_args["units"]
        if "tags" in schedule_args:
            result["tags"] = schedule_args["tags"]
        if "job_settings" in schedule_args:
            result["job_settings"] = schedule_args["job_settings"]
        if "project_id" in schedule_args:
            result["project_id"] = schedule_args["project_id"]
        return result

    def _workflow_step_gen(self, max_next_params: int) -> Generator[Tuple[str, Optional[JobKey]], None, None]:
        new_params: List[FullJobParams] = []
        next_params: Optional[FullJobParams]

        while len(new_params) < max_next_params:
            next_params = None
            for idx, np in enumerate(self.__delayed_jobs):
                if self.can_schedule_job_with_params(np):
                    next_params = np
                    self.__delayed_jobs = self.__delayed_jobs[:idx] + self.__delayed_jobs[idx + 1:]
                    break
            else:
                for idx, np in enumerate(self.__additional_jobs):
                    if self.can_schedule_job_with_params(np):
                        next_params = np
                        self.__additional_jobs = self.__additional_jobs[:idx] + self.__additional_jobs[idx + 1:]
                        break
                else:
                    try:
                        np = self._fulljobparams_from_spiderargs(next(self.__parameters_gen))
                    except StopIteration:
                        break
                    else:
                        spider = np.get("spider", self.spider)
                        assert spider, f"No spider set for parameters {np}"
                        np["spider"] = spider
                        if self.can_schedule_job_with_params(np):
                            next_params = np
                        else:
                            self.__delayed_jobs.append(np)
                            continue

            if next_params is None:
                break
            spider_args = next_params.get("spider_args") or {}
            jobuid = self.get_job_unique_id({"spider": next_params["spider"], "spider_args": spider_args})
            if jobuid in self._jobuids:
                _LOG.warning(f"Job with parameters {next_params} was already scheduled. Skipped.")
                continue
            new_params.append(next_params)

        for next_params in new_params:
            spider = next_params.pop("spider")
            self.__add_jobseq_tag(next_params)
            yield jobuid, self.schedule_spider_with_jobargs(next_params, spider)

    def get_max_next_params(self) -> int:
        return self.max_running_jobs - len(self._running_job_keys)

    def workflow_loop(self) -> bool:
        self.check_running_jobs()
        max_next_params = self.get_max_next_params()
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
        max_next_params = self.get_max_next_params()
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
        schedule_args: ScheduleArgs = json.loads(self.args.spider_args)
        spider_args = job_args_override.get("spider_args") or {}
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
