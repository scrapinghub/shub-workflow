"""
Implements common methods for any workflow manager
"""
import time
import logging
from uuid import uuid4

from scrapinghub import DuplicateJobError

from .utils import (
    schedule_script_in_dash,
    dash_retry_decorator,
)

from .script import BaseScript


logger = logging.getLogger(__name__)


class WorkFlowManager(BaseScript):

    default_max_jobs = float('inf')
    loop_mode = None

    def __init__(self):
        self.workflow_loop_enabled = False
        super().__init__()

    def set_flow_id(self, args, default=None):
        default = default or str(uuid4())
        super().set_flow_id(args, default)

    @property
    def max_running_jobs(self):
        return self.args.max_running_jobs

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument('--loop-mode', help='If provided, manager will run in loop mode, with a cycle\
                                    each given number of seconds.', type=int, metavar='SECONDS', default=self.loop_mode)
        self.argparser.add_argument('--max-running-jobs', type=int, default=self.default_max_jobs,
                                    help='If given, don\'t allow more than the given jobs running at once.\
                                    Default: %(default)s')
        self.argparser.add_argument('--tag', help='Add given tag to the scheduled jobs. Can be given multiple times.',
                                    action='append', default=[])

    def _make_tags(self, tags):
        tags = tags or []
        tags.extend(self.args.tag)
        tags.append(f'FLOW_ID={self.flow_id}')
        return list(set(tags)) or None

    def schedule_script(self, cmd, tags=None, project_id=None, **kwargs):
        """
        Schedules an external script
        """
        logger.info('Starting: {}'.format(cmd))
        project = self.get_project(project_id)
        job = schedule_script_in_dash(project, [str(x) for x in cmd], tags=self._make_tags(tags), **kwargs)
        logger.info(f"Scheduled script job {job.key}")
        return job.key

    @dash_retry_decorator
    def schedule_spider(self, spider, tags=None, units=None, project_id=None, **spiderargs):
        schedule_kwargs = dict(spider=spider, add_tag=self._make_tags(tags), units=units, **spiderargs)
        logger.info("Scheduling a spider:\n%s", schedule_kwargs)
        try:
            project = self.get_project(project_id)
            job = project.jobs.run(**schedule_kwargs)
            logger.info(f"Scheduled spider job {job.key}")
            return job.key
        except DuplicateJobError as e:
            logger.error(str(e))
        except:
            raise

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
