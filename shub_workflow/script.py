"""
Implements common methods for ScrapyCloud scripts.
"""
import os
import abc
import json
import asyncio
import logging
import time
from functools import partial
import subprocess
from argparse import ArgumentParser, Namespace
from typing import List, NewType, Optional, Tuple, Generator, Dict, Union, Any, AsyncGenerator, Awaitable, cast
from typing_extensions import TypedDict, NotRequired, Protocol

from scrapinghub import ScrapinghubClient, DuplicateJobError
from scrapinghub.client.jobs import Job, JobMeta
from scrapinghub.client.projects import Project

from shub_workflow.utils import (
    resolve_project_id,
    dash_retry_decorator,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


JobKey = NewType("JobKey", str)  # ScrapyCloud job key


class JobDict(TypedDict):

    spider: str
    key: NotRequired[JobKey]
    tags: NotRequired[List[str]]
    close_reason: NotRequired[str]
    spider_args: NotRequired[Dict[str, str]]
    job_cmd: NotRequired[List[str]]
    pending_time: NotRequired[int]
    running_time: NotRequired[int]
    finished_time: NotRequired[int]


class ArgumentParserScriptProtocol(Protocol):

    args: Namespace

    @abc.abstractmethod
    def add_argparser_options(self):
        ...

    @abc.abstractmethod
    def parse_args(self) -> Namespace:
        ...

    @abc.abstractmethod
    def run(self):
        ...


class ArgumentParserScript(ArgumentParserScriptProtocol):
    def __init__(self):
        self.args: Namespace = self.parse_args()

    @property
    def description(self) -> str:
        return "You didn't set description for this script. Please set description property accordingly."

    def parse_args(self) -> Namespace:
        self.argparser = ArgumentParser(self.description)
        self.add_argparser_options()
        args = self.argparser.parse_args()
        return args


class BaseScriptProtocol(ArgumentParserScriptProtocol, Protocol):

    name: str

    @abc.abstractmethod
    def append_flow_tag(self, tag: str):
        ...

    @abc.abstractmethod
    def get_project(self, project_id: Optional[Union[int, str]] = None) -> Project:
        ...

    @abc.abstractmethod
    def get_job_metadata(self, jobkey: Optional[JobKey] = None) -> JobMeta:
        ...

    @abc.abstractmethod
    def get_job(self, jobkey: Optional[JobKey] = None) -> Job:
        ...

    @abc.abstractmethod
    def get_job_tags(self, jobkey=None) -> List[str]:
        ...

    @abc.abstractmethod
    def get_keyvalue_job_tag(self, key: str, tags: List[str]) -> Optional[str]:
        ...

    @abc.abstractmethod
    def add_job_tags(self, jobkey: Optional[JobKey] = None, tags: Optional[List[str]] = None):
        ...

    @abc.abstractmethod
    def schedule_script(self, cmd: List[str], tags=None, project_id=None, units=None, meta=None) -> Optional[JobKey]:
        ...

    @abc.abstractmethod
    def schedule_spider(
        self,
        spider: str,
        tags: Optional[List[str]] = None,
        units: Optional[int] = None,
        project_id: Optional[int] = None,
        **kwargs,
    ) -> Optional[JobKey]:
        ...

    @abc.abstractmethod
    def get_jobs(self, project_id: Optional[int] = None, **kwargs) -> Generator[JobDict, None, None]:
        ...

    @abc.abstractmethod
    def get_jobs_with_tags(self, spider, tags, project_id=None, **kwargs) -> Generator[Job, None, None]:
        ...

    @abc.abstractmethod
    def is_running(self, jobkey: JobKey) -> bool:
        ...

    @abc.abstractmethod
    def is_finished(self, jobkey: JobKey) -> Optional[str]:
        ...

    @abc.abstractmethod
    def finish(self, jobkey: Optional[JobKey] = None, close_reason: Optional[str] = None):
        ...

    @abc.abstractmethod
    def _make_children_tags(self, tags: Optional[List[str]]) -> Optional[List[str]]:
        ...


class BaseScript(ArgumentParserScript, BaseScriptProtocol):

    name = ""  # optional, may be needed for some applications
    flow_id_required = False  # if True, script can only run in the context of a flow_id
    children_tags: Optional[List[str]] = None  # extra tags added to children
    default_project_id: Optional[int] = None  # If None, autodetect (see shub_workflow.utils.resolve_project_id)

    def __init__(self):
        self.project_id: Optional[int] = None
        self.client = ScrapinghubClient(max_retries=100)
        self.close_reason: Optional[str] = None
        self.__flow_tags: List[str] = []
        super().__init__()
        self.set_flow_id_name(self.args)

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
        self.argparser.add_argument(
            "--project-id",
            help="Overrides target project id.",
            type=int,
            default=self.default_project_id,
        )
        self.argparser.add_argument("--flow-id", help="If given, use the given flow id.")
        self.argparser.add_argument(
            "--children-tag",
            help="Additional tag added to the scheduled jobs. Can be given multiple times.",
            action="append",
            default=self.children_tags or [],
        )

    def parse_project_id(self, args: Namespace) -> int:
        return args.project_id

    def parse_args(self) -> Namespace:
        args = super().parse_args()

        self.project_id = resolve_project_id(self.parse_project_id(args))
        if not self.project_id:
            self.argparser.error("Project id not provided.")

        return args

    def get_project(self, project_id: Optional[Union[int, str]] = None) -> Project:
        return self.client.get_project(project_id or self.project_id)

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

    def get_job_tags(self, jobkey=None) -> List[str]:
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
    def _get_metadata_key(metadata: JobMeta, key: str) -> Any:
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
        except Exception:
            raise
        else:
            logger.info(f"Scheduled job {job.key}")
            return job.key
        return None

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
        if self._get_metadata_key(job.metadata, "state") in ("running", "pending"):
            return True
        return False

    def is_finished(self, jobkey: JobKey) -> Optional[str]:
        """
        Checks whether a job is finished. if so, return close_reason. Otherwise return None.
        """
        metadata = self.get_job_metadata(jobkey)
        if self._get_metadata_key(metadata, "state") == "finished":
            return self._get_metadata_key(metadata, "close_reason")
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


class BaseLoopScriptProtocol(BaseScriptProtocol, Protocol):
    @abc.abstractmethod
    def workflow_loop(self) -> bool:
        """Implement here your loop code. Return True if want to continue looping,
        False for immediate stop of the job. For continuous looping you also need
        to set `loop_mode`
        """
        ...

    @abc.abstractmethod
    def _run_loops(self) -> Generator[bool, None, None]:
        ...

    def base_loop_tasks(self):
        ...


class BaseLoopScript(BaseScript, BaseLoopScriptProtocol):

    # If 0, don't loop. If positive number, repeat loop every given number of seconds
    # --loop-mode command line option overrides it
    loop_mode = 0
    max_running_time = 0

    def __init__(self):
        self.workflow_loop_enabled = False
        super().__init__()
        self.__start_time = time.time()

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
                self._close()
                logger.info("No more tasks")
                break

    def base_loop_tasks(self):
        pass

    def __base_loop_tasks(self):
        self.base_loop_tasks()
        if self.args.max_running_time and time.time() - self.__start_time > self.args.max_running_time:
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

    def _on_start(self):
        self.on_start()
        self.workflow_loop_enabled = True

    def on_close(self):
        pass

    def _close(self):
        self.on_close()


class BaseLoopScriptAsyncMixin(BaseLoopScriptProtocol):
    @dash_retry_decorator
    async def _async_schedule_job(
        self, spider: str, tags=None, units=None, project_id=None, **kwargs
    ) -> Optional[JobKey]:
        project = self.get_project(project_id)
        schedule_kwargs = dict(
            spider=spider,
            add_tag=self._make_children_tags(tags),
            debugunits=units,
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
                self._close()
                logger.info("No more tasks")
                break
