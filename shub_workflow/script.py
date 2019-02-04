"""
Implements common methods for ScrapyCloud scripts.
"""
import os
import abc
import logging

from argparse import ArgumentParser

from scrapinghub import ScrapinghubClient

from .utils import (
    resolve_project_id,
    dash_retry_decorator,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s')


class BaseScript(abc.ABC):

    name = None  # optional, may be needed for some applications
    flow_id_required = True  # if True, script can only run in the context of a flow_id

    def __init__(self):
        self.project_id = None
        self.client = ScrapinghubClient()
        self.args = self.parse_args()

    def set_flow_id(self, args, default=None):
        self._flow_id = args.flow_id or self.get_flowid_from_tags() or default
        if self.flow_id_required:
            assert not self.flow_id_required or self.flow_id, "Could not detect flow_id. Please set with --flow-id."
        if self.flow_id:
            self.add_job_tags(tags=[f'FLOW_ID={self.flow_id}'])

    @property
    def description(self):
        return "You didn't set description for this script. Please set description property accordingly."

    @property
    def flow_id(self):
        return self._flow_id

    def add_argparser_options(self):
        self.argparser.add_argument('--project-id', help='Overrides target project id.', type=int)
        self.argparser.add_argument('--name', help='Script name.')
        self.argparser.add_argument('--flow-id', help='If given, use the given flow id.')

    def parse_args(self):
        self.argparser = ArgumentParser(self.description)
        self.add_argparser_options()
        args = self.argparser.parse_args()

        self.project_id = resolve_project_id(args.project_id)
        if not self.project_id:
            self.argparser.error('Project id not provided.')

        self.set_flow_id(args)

        self.name = args.name or self.name
        return args

    def get_project(self, project_id=None):
        return self.client.get_project(project_id or self.project_id)

    @dash_retry_decorator
    def get_job_metadata(self, jobid=None, project_id=None):
        """If jobid is None, get own metadata
        """
        jobid = jobid or os.getenv('SHUB_JOBKEY')
        if jobid:
            project = self.get_project(project_id)
            job = project.jobs.get(jobid)
            return job.metadata
        else:
            logger.warning('SHUB_JOBKEY not set: not running on ScrapyCloud.')

    def get_job_tags(self, jobid=None, project_id=None):
        """If jobid is None, get own tags
        """
        metadata = self.get_job_metadata(jobid, project_id)
        if metadata:
            return dict(metadata.list()).get('tags', [])
        return []

    def add_job_tags(self, jobid=None, project_id=None, tags=None):
        """If jobid is None, add tags to own list of tags.
        """
        if tags:
            update = False
            job_tags = self.get_job_tags(jobid, project_id)
            for tag in tags:
                if tag not in job_tags:
                    if tag.startswith('FLOW_ID='):
                        job_tags.insert(0, tag)
                    else:
                        job_tags.append(tag)
                    update = True
            if update:
                metadata = self.get_job_metadata(jobid, project_id)
                if metadata:
                    metadata.update({'tags': job_tags})

    def get_flowid_from_tags(self, jobid=None, project_id=None):
        """If jobid is None, get flowid from own tags
        """
        for tag in self.get_job_tags(jobid, project_id):
            if tag.startswith('FLOW_ID='):
                return tag.replace('FLOW_ID=', '')

    @abc.abstractmethod
    def run(self):
        pass
