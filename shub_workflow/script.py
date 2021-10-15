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
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")


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
    children_tags = None
    default_project_id = None

    def __init__(self):
        self.project_id = None
        self.client = ScrapinghubClient()
        self.close_reason = None
        super().__init__()

    def set_flow_id(self, args, default=None):
        self._flow_id = args.flow_id or self.get_flowid_from_tags() or default
        if self.flow_id_required:
            assert not self.flow_id_required or self.flow_id, "Could not detect flow_id. Please set with --flow-id."
        if self.flow_id:
            self.add_job_tags(tags=[f"FLOW_ID={self.flow_id}"])

    @property
    def flow_id(self):
        return self._flow_id

    def add_argparser_options(self):
        self.argparser.add_argument(
            "--project-id", help="Overrides target project id.", type=int, default=self.default_project_id,
        )
        self.argparser.add_argument("--name", help="Script name.")
        self.argparser.add_argument("--flow-id", help="If given, use the given flow id.")
        self.argparser.add_argument(
            "--tag",
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

        self.set_flow_id(args)

        self.name = args.name or self.name
        return args

    def get_project(self, project_id=None):
        return self.client.get_project(project_id or self.project_id)

    @dash_retry_decorator
    def get_job_metadata(self, jobid=None):
        """If jobid is None, get own metadata
        """
        jobid = jobid or os.getenv("SHUB_JOBKEY")
        if jobid:
            project_id = jobid.split("/", 1)[0]
            project = self.get_project(project_id)
            job = project.jobs.get(jobid)
            return job.metadata
        logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    @dash_retry_decorator
    def get_job(self, jobid=None):
        """If jobid is None, get own metadata
        """
        jobid = jobid or os.getenv("SHUB_JOBKEY")
        if jobid:
            project_id = jobid.split("/", 1)[0]
            project = self.get_project(project_id)
            return project.jobs.get(jobid)
        logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")

    def get_job_tags(self, jobid=None):
        """If jobid is None, get own tags
        """
        metadata = self.get_job_metadata(jobid)
        if metadata:
            return dict(metadata.list()).get("tags", [])
        return []

    @staticmethod
    @dash_retry_decorator
    def update_metadata(metadata, data):
        metadata.update(data)

    def add_job_tags(self, jobid=None, tags=None):
        """If jobid is None, add tags to own list of tags.
        """
        if tags:
            update = False
            job_tags = self.get_job_tags(jobid)
            for tag in tags:
                if tag not in job_tags:
                    if tag.startswith("FLOW_ID="):
                        job_tags.insert(0, tag)
                    else:
                        job_tags.append(tag)
                    update = True
            if update:
                metadata = self.get_job_metadata(jobid)
                if metadata:
                    self.update_metadata(metadata, {"tags": job_tags})

    def get_flowid_from_tags(self, jobid=None):
        """If jobid is None, get flowid from own tags
        """
        for tag in self.get_job_tags(jobid):
            if tag.startswith("FLOW_ID="):
                return tag.replace("FLOW_ID=", "")

    def _make_tags(self, tags):
        tags = tags or []
        tags.extend(self.args.tag)
        if self.flow_id:
            tags.append(f"FLOW_ID={self.flow_id}")
        return list(set(tags)) or None

    @dash_retry_decorator
    def _schedule_job(self, spider: str, tags=None, units=None, project_id=None, **kwargs):
        project = self.get_project(project_id)
        schedule_kwargs = dict(spider=spider, add_tag=self._make_tags(tags), units=units, **kwargs,)
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
        return self.get_project(project_id).jobs.iter(**kwargs)

    def get_owned_jobs(self, project_id=None, **kwargs):
        assert self.flow_id, "This job doesn't have a flow id."
        assert "has_tag" not in kwargs, "Filtering by flow id requires no extra has_tag."
        assert "state" in kwargs, "'state' parameter must be provided."
        kwargs["has_tag"] = [f"FLOW_ID={self.flow_id}"]
        return self.get_jobs(project_id, **kwargs)

    @dash_retry_decorator
    def is_running(self, jobkey, project_id=None):
        """
        Checks whether a job is running (or pending)
        """
        project = self.get_project(project_id)
        job = project.jobs.get(jobkey)
        if job.metadata.get("state") in ("running", "pending"):
            return True
        return False

    @dash_retry_decorator
    def is_finished(self, jobkey, project_id=None):
        """
        Checks whether a job is running. if so, return close_reason. Otherwise return None.
        """
        project = self.get_project(project_id)
        job = project.jobs.get(jobkey)
        if job.metadata.get("state") == "finished":
            return job.metadata.get("close_reason")

    @dash_retry_decorator
    def finish(self, jobid=None, close_reason=None):
        close_reason = close_reason or "finished"
        if jobid is None:
            self.close_reason = close_reason
            if close_reason == "finished":
                return
        jobid = jobid or os.getenv("SHUB_JOBKEY")
        if jobid:
            project_id = jobid.split("/", 1)[0]
            hsp = self.client._hsclient.get_project(project_id)
            hsj = hsp.get_job(jobid)
            hsp.jobq.finish(hsj, close_reason=close_reason)
        else:
            logger.warning("SHUB_JOBKEY not set: not running on ScrapyCloud.")
