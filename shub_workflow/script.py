"""
Implements common methods for ScrapyCloud scripts.
"""

import abc
import json
import logging
import os
import subprocess
from argparse import ArgumentParser
from typing import List

from scrapinghub import ScrapinghubClient, DuplicateJobError

from shub_workflow.utils import (
    resolve_project_id,
    dash_retry_decorator,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ArgumentParserScript(abc.ABC):
    def __init__(self):
        self.args = self.parse_args()

    @property
    def description(self):
        return "You didn't set description for this script. Please set description property accordingly."

    @abc.abstractmethod
    def add_argparser_options(self):
        pass

    def parse_args(self):
        self.argparser = ArgumentParser(self.description)
        self.add_argparser_options()
        args = self.argparser.parse_args()
        return args

    @abc.abstractmethod
    def run(self):
        pass


class BaseScript(ArgumentParserScript):

    name = ""  # optional, may be needed for some applications
    flow_id_required = False  # if True, script can only run in the context of a flow_id
    children_tags = None  # extra tags added to children
    default_project_id = None  # If None, autodetect (see shub_workflow.utils.resolve_project_id)

    def __init__(self):
        self.project_id = None
        self.client = ScrapinghubClient()
        self.close_reason = None
        self.__flow_tags = []
        super().__init__()
        self.set_flow_id_name(self.args)

    def append_flow_tag(self, tag):
        """
        A flow tag is a tag that is transmited to children.
        """
        self.__flow_tags.append(tag)
        self.add_job_tags(tags=[tag])

    @staticmethod
    def generate_flow_id():
        raise NotImplementedError("generate_flow_id() must be implemented if flow_id_required is True.")

    def set_flow_id_name(self, args):
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

    def parse_project_id(self, args):
        return args.project_id

    def parse_args(self):
        args = super().parse_args()

        self.project_id = resolve_project_id(self.parse_project_id(args))
        if not self.project_id:
            self.argparser.error("Project id not provided.")

        return args

    def get_project(self, project_id=None):
        return self.client.get_project(project_id or self.project_id)

    @dash_retry_decorator
    def get_job_metadata(self, jobkey=None):
        """If jobkey is None, get own metadata"""
        jobkey = jobkey or os.getenv("SHUB_JOBKEY")
        if jobkey:
            project_id = jobkey.split("/", 1)[0]
            project = self.get_project(project_id)
            job = project.jobs.get(jobkey)
            return job.metadata
        logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    @dash_retry_decorator
    def get_job(self, jobkey=None):
        """If jobkey is None, get own metadata"""
        jobkey = jobkey or os.getenv("SHUB_JOBKEY")
        if jobkey:
            project_id = jobkey.split("/", 1)[0]
            project = self.get_project(project_id)
            return project.jobs.get(jobkey)
        logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    def get_job_tags(self, jobkey=None):
        """If jobkey is None, get own tags"""
        metadata = self.get_job_metadata(jobkey)
        if metadata:
            return dict(self._list_metadata(metadata)).get("tags", [])
        return []

    def get_keyvalue_job_tag(self, key, tags):
        for tag in tags:
            if tag.startswith(f"{key}="):
                return tag.replace(f"{key}=", "")

    @staticmethod
    @dash_retry_decorator
    def _update_metadata(metadata, data):
        metadata.update(data)

    @staticmethod
    @dash_retry_decorator
    def _list_metadata(metadata):
        return metadata.list()

    @staticmethod
    @dash_retry_decorator
    def _get_metadata_key(metadata, key):
        return metadata.get(key)

    def add_job_tags(self, jobkey=None, tags=None):
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

    def _get_flowid_name_from_tags(self, jobkey=None):
        """If jobkey is None, get flowid from own tags"""
        tags = self.get_job_tags(jobkey)
        flow_id = self.get_keyvalue_job_tag("FLOW_ID", tags)
        name = self.get_keyvalue_job_tag("NAME", tags)
        return flow_id, name

    def _make_children_tags(self, tags):
        tags = tags or []
        tags.extend(self.args.children_tag)
        if self.flow_id:
            tags.append(f"FLOW_ID={self.flow_id}")
            if self.name:
                tags.append(f"PARENT_NAME={self.name}")
        tags.extend(self.__flow_tags)
        return sorted(set(tags)) or None

    @dash_retry_decorator
    def _schedule_job(self, spider: str, tags=None, units=None, project_id=None, **kwargs):
        project = self.get_project(project_id)
        schedule_kwargs = dict(
            spider=spider,
            add_tag=self._make_children_tags(tags),
            units=units,
            **kwargs,
        )
        logger.info("Scheduling a job:\n%s", schedule_kwargs)
        try:
            job = project.jobs.run(**schedule_kwargs)
        except DuplicateJobError as e:
            logger.error(str(e))
        except Exception:
            raise
        else:
            logger.info(f"Scheduled job {job.key}")
            return job.key

    def schedule_script(self, cmd: List[str], tags=None, project_id=None, units=None, meta=None):
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

    def schedule_spider(self, spider: str, tags=None, units=None, project_id=None, **kwargs):
        return self._schedule_job(spider=spider, tags=tags, units=units, project_id=project_id, **kwargs)

    @dash_retry_decorator
    def get_jobs(self, project_id=None, **kwargs):
        kwargs = kwargs.copy()
        kwargs["start"] = 0
        max_count = kwargs.get("count") or float("inf")
        kwargs["count"] = min(1000, max_count)
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
            if count < kwargs["count"]:
                break
            else:
                shift = len(seen)
                kwargs["start"] += shift

    def get_jobs_with_tags(self, spider, tags, project_id=None, **kwargs):
        """
        Get jobs with target tags
        """
        has_tag, tags = tags[:1], tags[1:]
        for spider_job in self.get_jobs(project_id, spider=spider, has_tag=has_tag, **kwargs):
            if not set(tags).difference(spider_job["tags"]):
                yield self.get_project().jobs.get(spider_job["key"])

    @dash_retry_decorator
    def is_running(self, jobkey):
        """
        Checks whether a job is running (or pending)
        """
        project_id = jobkey.split("/", 1)[0]
        project = self.get_project(project_id)
        job = project.jobs.get(jobkey)
        if self._get_metadata_key(job.metadata, "state") in ("running", "pending"):
            return True
        return False

    def is_finished(self, jobkey):
        """
        Checks whether a job is finished. if so, return close_reason. Otherwise return None.
        """
        metadata = self.get_job_metadata(jobkey)
        if self._get_metadata_key(metadata, "state") == "finished":
            return self._get_metadata_key(metadata, "close_reason")

    @dash_retry_decorator
    def finish(self, jobkey=None, close_reason=None):
        close_reason = close_reason or "finished"
        if jobkey is None:
            self.close_reason = close_reason
            if close_reason == "finished":
                return
        jobkey = jobkey or os.getenv("SHUB_JOBKEY")
        if jobkey:
            project_id = jobkey.split("/", 1)[0]
            hsp = self.client._hsclient.get_project(project_id)
            hsj = hsp.get_job(jobkey)
            hsp.jobq.finish(hsj, close_reason=close_reason)
        else:
            logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")
