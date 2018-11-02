"""
Implements common methods for any workflow manager
"""
import os
import time
import logging

from argparse import ArgumentParser

from scrapinghub import ScrapinghubClient
from scrapinghub import APIError

from .utils import (
    resolve_project_id,
    schedule_script_in_dash,
    dash_retry_decorator,
)


logger = logging.getLogger(__name__)


class WorkFlowManager(object):
    project_id = None
    name = None
    default_max_jobs = float('inf')

    def __init__(self):
        self.workflow_loop_enabled = False
        self.args = self.parse_args()
        self.client = ScrapinghubClient(self.args.apikey)
        self.project_id = resolve_project_id(self.args.project_id or self.project_id)
        if not self.project_id:
            self.argparser.error('Project id not provided.')

    @property
    def max_running_jobs(self):
        return self.args.max_running_jobs

    def add_argparser_options(self):
        self.argparser.add_argument('--project-id', help='Overrides target project id.', type=int)
        self.argparser.add_argument('--name', help='Manager name.')
        self.argparser.add_argument('--apikey', help='Use specified apikey instead of autodetect.')
        self.argparser.add_argument('--loop-mode', help='If provided, manager will run in loop mode, with a cycle\
                                    each given number of seconds.', type=int, metavar='SECONDS')
        self.argparser.add_argument('--max-running-jobs', type=int, default=self.default_max_jobs,
                                    help='If given, don\'t allow more than the given jobs running at once.\
                                    Default: %(default)s')

    def parse_args(self):
        self.argparser = ArgumentParser()
        self.add_argparser_options()
        args = self.argparser.parse_args()
        self.project_id = args.project_id or self.project_id
        self.name = args.name or self.name
        return args

    def schedule_script(self, cmd, tags=None, project_id=None, **kwargs):
        """
        Schedules an external script
        """
        logger.info('Starting: {}'.format(cmd))
        project = self.get_project(project_id)
        job = schedule_script_in_dash(project, [str(x) for x in cmd], tags=tags, **kwargs)
        logger.info(f"Scheduled script job {job.key}")
        return job.key


    @dash_retry_decorator
    def schedule_spider(self, spider, tags=None, units=None, project_id=None, **spiderargs):
        schedule_kwargs = dict(spider=spider, add_tag=tags, units=units, **spiderargs)
        logger.info("Scheduling a spider:\n%s", schedule_kwargs)
        try:
            project = self.get_project(project_id)
            job = project.jobs.run(**schedule_kwargs)
            logger.info(f"Scheduled spider job {job.key}")
            return job.key
        except APIError as e:
            if 'already scheduled' in e.message:
                logger.error(e.message)
            else:
                raise e

    def get_project(self, project_id=None):
        return self.client.get_project(project_id or self.project_id)

    @dash_retry_decorator
    def is_running(self, jobkey, project_id=None):
        """
        Checks whether a job is running (or pending)
        """
        project = self.get_project(project_id)
        job = project.jobs.get(jobkey)
        if job.metadata.get('state') in ('running', 'pending'):
            return True
        return False

    @dash_retry_decorator
    def is_finished(self, jobkey, project_id=None):
        """
        Checks whether a job is running. if so, return close_reason. Otherwise return None.
        """
        project = self.get_project(project_id)
        job = project.jobs.get(jobkey)
        if job.metadata.get('state') == 'finished':
            return job.metadata.get('close_reason')
        return

    def wait_for(self, jobs_keys, interval=60, timeout=float('inf'), heartbeat=None):
        """Waits until all given jobs are not running anymore or until the
        timeout is reached, if a heartbeat is given it'll log an entry every
        heartbeat seconds (considering the interval), otherwise it'll log an
        entry every interval seconds.
        """
        if isinstance(jobs_keys, str):
            jobs_keys = [jobs_keys]
        if heartbeat is None or heartbeat < interval:
            heartbeat = interval
        still_running = dict((key, True) for key in jobs_keys)
        time_waited, next_heartbeat = 0, heartbeat
        while any(still_running.values()) and time_waited < timeout:
            time.sleep(interval)
            time_waited += interval
            for key in (k for k, v in still_running.items() if v):
                if self.is_running(key):
                    if time_waited >= next_heartbeat:
                        next_heartbeat += heartbeat
                        logger.info('{} still running'.format(key))
                    break
                else:
                    still_running[key] = False

    @dash_retry_decorator
    def get_job_metadata(self, jobid=None, project_id=None):
        jobid = jobid or os.getenv('SHUB_JOBKEY')
        project = self.get_project(project_id)
        job = project.jobs.get(jobid)
        return dict(job.metadata.list())

    def on_start(self):
        self.workflow_loop_enabled = True

    def workflow_loop(self):
        pass

    def on_close(self):
        pass

    def __close(self):
        self.on_close()

    def run(self):
        self.on_start()
        self._run_loops()

    def _run_loops(self):
        while self.workflow_loop_enabled:
            try:
                if self.workflow_loop() and self.args.loop_mode:
                    time.sleep(self.args.loop_mode)
                else:
                    self.__close()
                    logger.info("No more tasks")
                    return
            except KeyboardInterrupt:
                logger.info("Bye")
                return
