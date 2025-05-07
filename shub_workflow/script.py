"""
Implements common methods for ScrapyCloud scripts.
"""

import os
import sys
import abc
import json
import asyncio
import logging
import time
from functools import partial
import subprocess
from argparse import ArgumentParser as PythonArgumentParser, Namespace
from typing import (
    List,
    NewType,
    Optional,
    Tuple,
    Generator,
    Dict,
    Union,
    Any,
    AsyncGenerator,
    Awaitable,
    cast,
    Protocol,
    Type,
    Set,
    IO,
)
from pprint import pformat
from typing_extensions import TypedDict, NotRequired

import requests
from requests.auth import HTTPBasicAuth
from scrapy import Spider
from scrapy.utils.misc import load_object
from scrapy.spiderloader import SpiderLoader
from scrapy.statscollectors import StatsCollector
from scrapy.settings import BaseSettings
from scrapy.signalmanager import SignalManager
from scrapinghub import ScrapinghubClient, DuplicateJobError
from scrapinghub.client.jobs import Job, JobMeta
from scrapinghub.client.projects import Project

from shub_workflow.utils import get_project_settings, resolve_shub_jobkey
from shub_workflow.utils import (
    resolve_project_id,
    dash_retry_decorator,
)
from shub_workflow.utils.futils import FSHelper


logger = logging.getLogger(__name__)


JobKey = NewType("JobKey", str)  # ScrapyCloud job key
Outcome = NewType("Outcome", str)
SpiderName = NewType("SpiderName", str)


class JobDict(TypedDict):

    spider: NotRequired[SpiderName]
    key: NotRequired[JobKey]
    tags: NotRequired[List[str]]
    close_reason: NotRequired[str]
    spider_args: NotRequired[Dict[str, str]]
    job_cmd: NotRequired[List[str]]
    pending_time: NotRequired[int]
    running_time: NotRequired[int]
    finished_time: NotRequired[int]
    scrapystats: NotRequired[Dict[str, Any]]
    state: NotRequired[str]
    items: NotRequired[int]
    pages: NotRequired[int]


class ArgumentParser(PythonArgumentParser):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raise_parse_error = True
        self.programs: Dict[str, Dict[str, Any]] = {}

    def parse_args_no_error(self, args=None, namespace=None):
        self.raise_parse_error = False
        ns = super().parse_args(args, namespace)
        self.raise_parse_error = True
        return ns

    def error(self, message: str):
        if self.raise_parse_error:
            return super().error(message)

    def exit(self, *args, **kwargs):
        if self.raise_parse_error:
            return super().exit(*args, **kwargs)

    def print_usage(self, file: Optional[IO[str]] = None):
        if not self.programs:
            super().print_usage()

    def set_programs(self, programs: Dict[str, Dict[str, Any]]):
        self.programs = programs

    def _get_available_programs(self) -> str:
        available = ""
        for k, v in self.programs.items():
            available += f"* {k}: {v['description']}\n"
        return available

    def print_help(self, file: Optional[IO[str]] = None):
        if self.raise_parse_error:
            super().print_help(file)
            print(f"Programs available:\n{self._get_available_programs()}", file=sys.stderr)


class ArgumentParserScriptProtocol(Protocol):

    args: Namespace
    argparser: ArgumentParser

    @abc.abstractmethod
    def add_argparser_options(self):
        """"""

    @abc.abstractmethod
    def parse_args(self) -> Namespace:
        """"""

    @abc.abstractmethod
    def run(self):
        """"""


class ArgumentParserScript(ArgumentParserScriptProtocol):

    PROGRAMS: Dict[str, Dict[str, Any]] = {}

    def __init__(self):
        self.args: Namespace = self.parse_args()

    def add_argparser_options(self):
        self.argparser.add_argument("--program", "-g", help="Use command line program shortcut with the given alias.")
        self.argparser.add_argument(
            "--program-variables",
            "-v",
            help="Set keyword arguments for program shortcut. Format: key1:val1,key2:val2,...",
        )

    @property
    def description(self) -> str:
        return "You didn't set description for this script. Please set description property accordingly."

    def parse_args(self) -> Namespace:
        self.argparser = ArgumentParser(self.description)
        self.add_argparser_options()
        self.argparser.set_programs(self.PROGRAMS)
        args = self.argparser.parse_args_no_error()
        if args.program:
            if args.program in self.PROGRAMS:
                kvars: Dict[str, str] = {}
                if args.program_variables:
                    for keyval in args.program_variables.split(","):
                        k, v = keyval.split(":")
                        kvars.update({k: v})
                args = self.argparser.parse_args(
                    [arg.format(**kvars) for arg in self.PROGRAMS[args.program]["command_line"]]
                )
            else:
                self.argparser.error(
                    f"Program {args.program} is not defined.\nPrograms available:\n"
                    f"{self.argparser._get_available_programs()}"
                )
        else:
            args = self.argparser.parse_args()
        return args


class SCProjectClassProtocol(Protocol):

    project_id: Optional[int]


class SCProjectClass(SCProjectClassProtocol):
    def __init__(self):
        self.client = ScrapinghubClient(max_retries=100)
        super().__init__()

    def get_project(self, project_id: Optional[Union[int, str]] = None) -> Project:
        return self.client.get_project(project_id or self.project_id)


class _PseudoCrawler:
    def __init__(self, script):
        self.settings = script.project_settings
        self.signals = SignalManager(script)


class BaseScriptProtocol(ArgumentParserScriptProtocol, SCProjectClassProtocol, Protocol):

    name: str
    stats: StatsCollector
    project_settings: BaseSettings
    spider_loader: SpiderLoader
    fshelper: FSHelper
    _pseudo_crawler: _PseudoCrawler

    @abc.abstractmethod
    def append_flow_tag(self, tag: str):
        """"""

    @abc.abstractmethod
    def get_project(self, project_id: Optional[Union[int, str]] = None) -> Project:
        """"""

    @abc.abstractmethod
    def get_job_metadata(self, jobkey: Optional[JobKey] = None) -> JobMeta:
        """"""

    @abc.abstractmethod
    def get_job(self, jobkey: Optional[JobKey] = None) -> Job:
        """"""

    @abc.abstractmethod
    def get_job_tags(self, jobkey: Optional[JobKey] = None) -> List[str]:
        """"""

    @abc.abstractmethod
    def get_keyvalue_job_tag(self, key: str, tags: List[str]) -> Optional[str]:
        """"""

    @abc.abstractmethod
    def add_job_tags(self, jobkey: Optional[JobKey] = None, tags: Optional[List[str]] = None):
        """"""

    @abc.abstractmethod
    def schedule_script(self, cmd: List[str], tags=None, project_id=None, units=None, meta=None) -> Optional[JobKey]:
        """"""

    @abc.abstractmethod
    def schedule_spider(
        self,
        spider: str,
        tags: Optional[List[str]] = None,
        units: Optional[int] = None,
        project_id: Optional[int] = None,
        **kwargs,
    ) -> Optional[JobKey]:
        """"""

    @abc.abstractmethod
    def get_jobs(self, project_id: Optional[int] = None, **kwargs) -> Generator[JobDict, None, None]:
        """"""

    @abc.abstractmethod
    def get_jobs_with_tags(
        self, spider: str, tags: List[str], project_id: Optional[int] = None, **kwargs
    ) -> Generator[Job, None, None]:
        """"""

    @abc.abstractmethod
    def is_running(self, jobkey: JobKey) -> bool:
        """"""

    @abc.abstractmethod
    def is_finished(self, jobkey: JobKey) -> Optional[Outcome]:
        """"""

    @abc.abstractmethod
    def finish(self, jobkey: Optional[JobKey] = None, close_reason: Optional[str] = None):
        """"""

    @abc.abstractmethod
    def _make_children_tags(self, tags: Optional[List[str]]) -> Optional[List[str]]:
        """"""

    @abc.abstractmethod
    def get_canonical_spidername(self, spidername: SpiderName) -> str:
        """"""

    @abc.abstractmethod
    def get_project_running_spiders(
        self, canonical: bool = False, crawlmanagers: Tuple[str, ...] = ()
    ) -> Set[SpiderName]:
        """"""

    @abc.abstractmethod
    def get_sc_project_settings(self) -> Dict[str, Any]:
        """"""


class BaseScript(SCProjectClass, ArgumentParserScript, BaseScriptProtocol):

    name = ""  # optional, may be needed for some applications
    flow_id_required = False  # if True, script can only run in the context of a flow_id
    children_tags: Optional[List[str]] = None  # extra tags added to children
    default_project_id: Optional[int] = None  # If None, autodetect (see shub_workflow.utils.resolve_project_id)
    project_required: bool = True  # If False, don't fail because project not set (some applications don't need it)

    def __init__(self):
        self.close_reason: Optional[str] = None
        self.__flow_tags: List[str] = []
        self.project_settings = get_project_settings()
        self.spider_loader = SpiderLoader(self.project_settings)
        # this is the own project id, that is, where the actual script is running.
        # it is not configurable except if running outside SC (in this case, it will
        # take the value from self.project_id)
        self._own_project_id = None
        # this is the working project id, that is, where child jobs will be scheduled.
        # it is configurable
        self.project_id = None
        super().__init__()
        if self.args.load_sc_settings and resolve_shub_jobkey() is None:
            self.project_settings.setdict(self.get_sc_project_settings(), priority="project")
        self.set_flow_id_name(self.args)

        self._pseudo_crawler = _PseudoCrawler(self)

        stats_collector_class = self.project_settings["STATS_CLASS"]
        logger.debug(f"Stats collection class: {stats_collector_class}")
        self.stats = load_object(stats_collector_class)(self._pseudo_crawler)
        self.fshelper = FSHelper()

    def append_flow_tag(self, tag: str):
        """
        A flow tag is a tag that is transmited to children.
        """
        self.__flow_tags.append(tag)
        self.add_job_tags(tags=[tag])

    def generate_flow_id(self) -> str:
        raise NotImplementedError("generate_flow_id() must be implemented if flow_id_required is True.")

    def set_flow_id_name(self, args: Namespace):
        flowid_from_tag, name_from_tag = self._get_flowid_name_from_tags()
        self.flow_id = flowid_from_tag or args.flow_id
        tags = []
        if not self.flow_id and self.flow_id_required:
            self.flow_id = self.generate_flow_id()
        if self.flow_id:
            tags = [f"FLOW_ID={self.flow_id}"]
        self.name = name_from_tag or self.name
        if self.name:
            tags.append(f"NAME={self.name}")
        self.add_job_tags(tags=tags)

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "--project-id",
            help="Either numeric id, or entry keyword in scrapinghub.yml. Overrides target project id.",
            default=self.default_project_id,
        )
        self.argparser.add_argument("--flow-id", help="If given, use the given flow id.")
        self.argparser.add_argument(
            "--children-tag",
            "-t",
            help="Additional tag added to the scheduled jobs. Can be given multiple times.",
            action="append",
            default=self.children_tags or [],
        )
        self.argparser.add_argument(
            "--load-sc-settings",
            action="store_true",
            help="If provided, and running on a local environment, load scrapy cloud settings.",
        )

    def parse_project_id(self, args: Namespace) -> int:
        return args.project_id

    def parse_args(self) -> Namespace:
        args = super().parse_args()

        try:
            self._own_project_id = resolve_project_id()
        except ValueError:
            pass

        try:
            self.project_id = resolve_project_id(self.parse_project_id(args))
        except ValueError:
            if self.project_required:
                raise

        self._own_project_id = self._own_project_id or self.project_id

        if self.project_required and not self.project_id:
            self.argparser.error(
                "Project id not provided. Either provide one (via --project-id) or disable "
                "by setting project_required attribute to False."
            )
        logger.info(f"Running on project {self.project_id}")

        return args

    def get_own_jobkey_from_env(self) -> Optional[JobKey]:
        envjobkey = os.getenv("SHUB_JOBKEY")
        if envjobkey is not None:
            return JobKey(envjobkey)
        return None

    @dash_retry_decorator
    def get_job_metadata(self, jobkey: Optional[JobKey] = None) -> JobMeta:
        """If jobkey is None, get own metadata"""
        jobkey = jobkey or self.get_own_jobkey_from_env()
        if jobkey:
            project_id = jobkey.split("/", 1)[0]
            project = self.get_project(project_id)
            job = project.jobs.get(jobkey)
            return job.metadata
        logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    @dash_retry_decorator
    def get_job(self, jobkey: Optional[JobKey] = None) -> Job:
        """If jobkey is None, get own metadata"""
        jobkey = jobkey or self.get_own_jobkey_from_env()
        if jobkey:
            project_id = jobkey.split("/", 1)[0]
            project = self.get_project(project_id)
            return project.jobs.get(jobkey)
        logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    def get_job_tags(self, jobkey: Optional[JobKey] = None) -> List[str]:
        """If jobkey is None, get own tags"""
        metadata = self.get_job_metadata(jobkey)
        if metadata:
            return dict(self._list_metadata(metadata)).get("tags", [])
        return []

    def get_keyvalue_job_tag(self, key: str, tags: List[str]) -> Optional[str]:
        for tag in tags:
            if tag.startswith(f"{key}="):
                return tag.replace(f"{key}=", "")
        return None

    @staticmethod
    @dash_retry_decorator
    def _update_metadata(metadata: JobMeta, data: Dict[str, Any]):
        metadata.update(data)

    @staticmethod
    @dash_retry_decorator
    def _list_metadata(metadata: JobMeta) -> List[Tuple[str, Any]]:
        return metadata.list()

    @staticmethod
    @dash_retry_decorator
    def get_metadata_key(metadata: JobMeta, key: str) -> Any:
        return metadata.get(key)

    def add_job_tags(self, jobkey: Optional[JobKey] = None, tags: Optional[List[str]] = None):
        """If jobkey is None, add tags to own list of tags."""
        if tags:
            update = False
            job_tags = self.get_job_tags(jobkey)
            for tag in tags:
                if tag not in job_tags:
                    if tag.startswith("FLOW_ID="):
                        job_tags.insert(0, tag)
                    else:
                        job_tags.append(tag)
                    update = True
            if update:
                metadata = self.get_job_metadata(jobkey)
                if metadata:
                    self._update_metadata(metadata, {"tags": job_tags})

    def remove_job_tags(self, tags: List[str], jobkey: JobKey):
        update = False
        job_tags = self.get_job_tags(jobkey)
        for tag in tags:
            if tag in job_tags:
                if any([tag.startswith(s) for s in ("FLOW_ID=", "NAME=", "PARENT_NAME=")]):
                    logger.info(f"Cannot remove tag {tag}: not allowed.")
                else:
                    job_tags.remove(tag)
                    update = True
        if update:
            metadata = self.get_job_metadata(jobkey)
            if metadata:
                self._update_metadata(metadata, {"tags": job_tags})

    async def async_add_job_tags(self, jobkey: Optional[JobKey] = None, tags: Optional[List[str]] = None):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial(self.add_job_tags, jobkey, tags))

    def _get_flowid_name_from_tags(self, jobkey: Optional[JobKey] = None) -> Tuple[Optional[str], Optional[str]]:
        """If jobkey is None, get flowid from own tags"""
        tags = self.get_job_tags(jobkey)
        flow_id = self.get_keyvalue_job_tag("FLOW_ID", tags)
        name = self.get_keyvalue_job_tag("NAME", tags)
        return flow_id, name

    def _make_children_tags(self, tags: Optional[List[str]]) -> Optional[List[str]]:
        tags = tags or []
        tags.extend(self.args.children_tag)
        if self.flow_id:
            tags.append(f"FLOW_ID={self.flow_id}")
            if self.name:
                tags.append(f"PARENT_NAME={self.name}")
        tags.extend(self.__flow_tags)
        return sorted(set(tags)) or None

    @dash_retry_decorator
    def _schedule_job(self, spider: str, tags=None, units=None, project_id=None, **kwargs) -> Optional[JobKey]:
        project = self.get_project(project_id)
        schedule_kwargs = dict(
            spider=spider,
            add_tag=self._make_children_tags(tags),
            units=units,
            **kwargs,
        )
        logger.debug("Scheduling a job:\n%s", schedule_kwargs)
        try:
            job = project.jobs.run(**schedule_kwargs)
        except DuplicateJobError as e:
            logger.error(str(e))
            self.handle_schedule_duplicate_error(**schedule_kwargs)
        except Exception as e:
            logger.error(f"Failed to schedule job with arguments {schedule_kwargs}: {e}")
            self.handle_schedule_error(e, **schedule_kwargs)
        else:
            logger.info(f"Scheduled job {job.key}")
            return job.key
        return None

    def handle_schedule_duplicate_error(self, **schedule_kwargs):
        pass

    def handle_schedule_error(self, e: Exception, **schedule_kwargs):
        raise e

    def schedule_script(self, cmd: List[str], tags=None, project_id=None, units=None, meta=None) -> Optional[JobKey]:
        cmd = [str(x) for x in cmd]
        scriptname = cmd[0]
        if not scriptname.startswith("py:"):
            scriptname = "py:" + scriptname
        return self._schedule_job(
            spider=scriptname,
            tags=tags,
            units=units,
            project_id=project_id,
            cmd_args=subprocess.list2cmdline(cmd[1:]),
            meta=json.dumps(meta) if meta else None,
        )

    def schedule_spider(
        self,
        spider: str,
        tags: Optional[List[str]] = None,
        units: Optional[int] = None,
        project_id: Optional[int] = None,
        **kwargs,
    ) -> Optional[JobKey]:
        return self._schedule_job(spider=spider, tags=tags, units=units, project_id=project_id, **kwargs)

    @dash_retry_decorator
    def get_jobs(self, project_id: Optional[int] = None, **kwargs) -> Generator[JobDict, None, None]:
        meta = kwargs.get("meta", [])
        if "has_tag" or "lacks_tag" in kwargs and "tags" not in meta:
            meta.append("tags")
        if meta:
            kwargs["meta"] = meta
        kwargs = kwargs.copy()
        max_count = kwargs.get("count") or float("inf")
        kwargs["count"] = min(1000, max_count)
        kwargs["start"] = 0
        seen = set()
        while True:
            count = 0
            for job in self.get_project(project_id).jobs.iter(**kwargs):
                count += 1
                if job["key"] not in seen:
                    yield job
                    seen.add(job["key"])
                    if len(seen) == max_count:
                        count = 0
                        break
            if count == 0:
                break
            kwargs["start"] += count

    def get_jobs_with_tags(
        self, spider: str, tags: List[str], project_id: Optional[int] = None, **kwargs
    ) -> Generator[Job, None, None]:
        """
        Get jobs with target tags
        """
        has_tag, tags = tags[:1], tags[1:]
        for spider_job in self.get_jobs(project_id, spider=spider, has_tag=has_tag, **kwargs):
            if not set(tags).difference(spider_job["tags"]):
                yield self.get_job(spider_job["key"])

    @dash_retry_decorator
    def is_running(self, jobkey: JobKey) -> bool:
        """
        Checks whether a job is running (or pending)
        """
        project_id = jobkey.split("/", 1)[0]
        project = self.get_project(project_id)
        job = project.jobs.get(jobkey)
        if self.get_metadata_key(job.metadata, "state") in ("running", "pending"):
            return True
        return False

    def is_finished(self, jobkey: JobKey) -> Optional[Outcome]:
        """
        Checks whether a job is finished. if so, return close_reason. Otherwise return None.
        """
        metadata = self.get_job_metadata(jobkey)
        if self.get_metadata_key(metadata, "state") == "finished":
            return self.get_metadata_key(metadata, "close_reason")
        return None

    @dash_retry_decorator
    def finish(self, jobkey: Optional[JobKey] = None, close_reason: Optional[str] = None):
        close_reason = close_reason or "finished"
        if jobkey is None:
            self.close_reason = close_reason
            if close_reason == "finished":
                return
        jobkey = jobkey or self.get_own_jobkey_from_env()
        if jobkey:
            project_id = jobkey.split("/", 1)[0]
            hsp = self.client._hsclient.get_project(project_id)
            hsj = hsp.get_job(jobkey)
            hsp.jobq.finish(hsj, close_reason=close_reason)
        else:
            logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    @dash_retry_decorator
    def upload_stats(self):
        """
        this is a bit dirty, but it is needed for HubStorageStatsCollector and
        there is not a base upload method in base stats class. Will improve
        in future.
        """
        if hasattr(self.stats, "_upload_stats"):
            self.stats._upload_stats()

    def print_stats(self):
        logger.info(pformat(self.stats.get_stats()))

    def get_canonical_spidername(self, spidername: SpiderName) -> SpiderName:
        """
        For some applications where same site is handled by different spider classes
        but you need same canonical name to refer to all them, set the attribute "canonical_name"
        of the spider to that common name. If not set, by default it is the spider name itself.
        """
        if spidername in self.spider_loader.list():
            spidercls: Type[Spider] = self.spider_loader.load(spidername)
            return getattr(spidercls, "canonical_name", None) or spidername
        return spidername

    def get_project_running_spiders(
        self, canonical: bool = False, crawlmanagers: Tuple[str, ...] = (), only_crawlmanagers: bool = False
    ) -> Set[SpiderName]:
        """
        Get all running spiders.
        - If canonical is True, get the canonical names (see get_canonical_spidername).
        - If crawlmanagers scripts (in the format py:<basename>.py) are provided to the parameter crawlmanagers,
          include in the check the script corresponding to the given crawlmanagers (that is, if the crawlmanager
          is running, the spider is considered running even if no actual spider job is running)
        - if only_crawlmanagers is True, only check crawlmanager, not actual spider jobs.
        """
        running_spiders: Set[SpiderName] = set()
        spiders = self.spider_loader.list()
        for jdict in self.get_jobs(state=["running"], meta=["job_cmd", "spider"]):
            if jdict["spider"].startswith("py:"):
                if jdict["spider"] in crawlmanagers:
                    for fragment in jdict["job_cmd"][1:]:
                        if fragment in spiders:
                            running_spiders.add(SpiderName(fragment))
                            break
            elif not only_crawlmanagers:
                running_spiders.add(jdict["spider"])
        if canonical:
            running_spiders = set(self.get_canonical_spidername(spidername) for spidername in running_spiders)
        return running_spiders

    @dash_retry_decorator
    def get_sc_project_settings(self) -> Dict[str, Any]:
        """
        Reads real time settings from SC
        """
        shkey = self.project_settings["SH_APIKEY"] or os.environ.get("SH_APIKEY")
        if shkey is not None:
            return requests.get(
                f"https://app.zyte.com/api/settings/get.json?project={self._own_project_id}",
                auth=HTTPBasicAuth(shkey, ""),
            ).json()["project_settings"]
        return {}


class BaseLoopScriptProtocol(BaseScriptProtocol, Protocol):
    @abc.abstractmethod
    def workflow_loop(self) -> bool:
        """Implement here your loop code. Return True if want to continue looping,
        False for immediate stop of the job. For continuous looping you also need
        to set `loop_mode`
        """

    @abc.abstractmethod
    def _run_loops(self) -> Generator[bool, None, None]:
        """"""

    @abc.abstractmethod
    def base_loop_tasks(self):
        """"""

    @abc.abstractmethod
    def _on_start(self):
        """"""

    @abc.abstractmethod
    def _close(self):
        """"""


class BaseLoopScript(BaseScript, BaseLoopScriptProtocol):

    # If 0, don't loop. If positive number, repeat loop every given number of seconds
    # --loop-mode command line option overrides it
    loop_mode = 0
    max_running_time = 0
    stats_interval = 120

    def __init__(self):
        self.workflow_loop_enabled = False
        super().__init__()
        self.__start_time = time.time()

        self.__close_reason = None
        self.__last_stats_upload = None

    def set_close_reason(self, reason):
        self.__close_reason = reason

    def get_close_reason(self):
        return self.__close_reason

    def is_closed(self):
        return self.__close_reason is not None

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "--loop-mode",
            help="If provided, manager will run in loop mode, with a cycle\
                                    each given number of seconds. Default: %(default)s",
            type=int,
            metavar="SECONDS",
            default=self.loop_mode,
        )
        self.argparser.add_argument(
            "--max-running-time",
            type=int,
            default=self.max_running_time,
            metavar="SECONDS",
            help="Set max time after which the script will auto shutdown.",
        )

    def on_start(self):
        pass

    def run(self):
        self._on_start()
        for loop_result in self._run_loops():
            if loop_result and self.args.loop_mode:
                time.sleep(self.args.loop_mode)
            else:
                break
        self._close()
        logger.info("No more tasks")

    def base_loop_tasks(self):
        pass

    def is_max_time_ran_out(self) -> bool:
        return self.args.max_running_time > 0 and time.time() - self.__start_time > self.args.max_running_time

    def __base_loop_tasks(self):
        self.base_loop_tasks()
        if self.is_max_time_ran_out():
            logger.info("Time limit reached. Closing.")
            self.workflow_loop_enabled = False

    def _run_loops(self) -> Generator[bool, None, None]:
        while True:
            self.__base_loop_tasks()
            if not self.workflow_loop_enabled:
                break
            try:
                yield self.workflow_loop()
            except KeyboardInterrupt:
                logger.info("Bye")
                break
            self.maybe_upload_stats()

    def maybe_upload_stats(self):
        now = time.time()
        if self.__last_stats_upload is None or now - self.__last_stats_upload >= self.stats_interval:
            self.upload_stats()
            self.__last_stats_upload = now

    def _on_start(self):
        self.on_start()
        self.workflow_loop_enabled = True

    def on_close(self):
        pass

    def _close(self):
        self.on_close()
        self.__close_reason = self.__close_reason or "finished"
        self.upload_stats()
        self.print_stats()


class BaseLoopScriptAsyncMixin(BaseLoopScriptProtocol):
    @dash_retry_decorator
    async def _async_schedule_job(
        self, spider: str, tags=None, units=None, project_id=None, **kwargs
    ) -> Optional[JobKey]:
        project = self.get_project(project_id)
        schedule_kwargs = dict(
            spider=spider,
            add_tag=self._make_children_tags(tags),
            units=units,
            **kwargs,
        )
        logger.debug("Scheduling a job with jobargs:\n%s", schedule_kwargs)
        try:
            loop = asyncio.get_event_loop()
            future = loop.run_in_executor(None, partial(project.jobs.run, **schedule_kwargs))
            job = await future
        except DuplicateJobError as e:
            logger.error(str(e))
        except Exception:
            raise
        else:
            logger.info(f"Scheduled job {job.key}.")
            return job.key
        return None

    async def async_schedule_spider(
        self,
        spider: str,
        tags: Optional[List[str]] = None,
        units: Optional[int] = None,
        project_id: Optional[int] = None,
        **kwargs,
    ) -> Optional[JobKey]:
        return await self._async_schedule_job(spider=spider, tags=tags, units=units, project_id=project_id, **kwargs)

    async def _async_run_loops(self) -> AsyncGenerator[bool, None]:
        for result in self._run_loops():
            yield await cast(Awaitable[bool], result)

    async def run(self):
        self._on_start()
        async for loop_result in self._async_run_loops():
            if loop_result and self.args.loop_mode:
                await asyncio.sleep(self.args.loop_mode)
            else:
                break
        self._close()
        logger.info("No more tasks")
