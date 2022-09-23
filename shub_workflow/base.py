"""
Implements common methods for any workflow manager
"""
import time
import logging
from uuid import uuid4
import abc
from collections import defaultdict

from .script import BaseScript


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CachedFinishedJobsMixin:
    def __init__(self):
        super().__init__()
        self.__finished_cache = {}
        self.__update_finished_cache_called = defaultdict(bool)

    def update_finished_cache(self, project_id):
        logger.debug("Initiating finished cache update.")
        if not self.__update_finished_cache_called[project_id]:
            for job in self.get_owned_jobs(project_id, state=["finished"], meta=["close_reason"]):
                if job["key"] in self.__finished_cache:
                    break
                self.__finished_cache[job["key"]] = job["close_reason"]
            logger.info(f"Finished jobs cache length: {len(self.__finished_cache)}")
        self.__update_finished_cache_called[project_id] = True

    def is_finished(self, jobkey):
        project_id = jobkey.split("/", 1)[0]
        self.update_finished_cache(project_id)
        return self.__finished_cache.get(jobkey)

    def _base_loop_tasks(self):
        for project_id in self.__update_finished_cache_called.keys():
            self.__update_finished_cache_called[project_id] = False


class WorkFlowManager(BaseScript, abc.ABC):

    # --max-running-job command line option overrides it
    default_max_jobs = float("inf")

    # If 0, don't loop. If positive number, repeat loop every given number of seconds
    # --loop-mode command line option overrides it
    loop_mode = 0

    flow_id_required = True

    base_failed_outcomes = (
        "failed",
        "killed by oom",
        "cancelled",
        "cancel_timeout",
        "memusage_exceeded",
        "diskusage_exceeded",
        "cancelled (stalled)",
    )

    def __init__(self):
        self.workflow_loop_enabled = False
        self.failed_outcomes = list(self.base_failed_outcomes)
        self.is_resumed = False
        super().__init__()

    def set_flow_id_name(self, args):
        super().set_flow_id_name(args)
        if not self.name:
            self.argparser.error("Manager name not set.")

    def get_owned_jobs(self, project_id=None, **kwargs):
        assert self.flow_id, "This job doesn't have a flow id."
        assert self.name, "This job doesn't have a name."
        assert "has_tag" not in kwargs, "Filtering by flow id requires no extra has_tag."
        assert "state" in kwargs, "'state' parameter must be provided."
        kwargs["has_tag"] = [f"FLOW_ID={self.flow_id}"]
        parent_tag = f"PARENT_NAME={self.name}"
        meta = kwargs.get("meta") or []
        if "tags" not in meta:
            meta.append("tags")
        kwargs["meta"] = meta
        for job in self.get_jobs(project_id, **kwargs):
            if parent_tag in job["tags"]:
                yield job

    @staticmethod
    def generate_flow_id():
        return str(uuid4())

    @property
    def max_running_jobs(self):
        return self.args.max_running_jobs

    def add_argparser_options(self):
        super().add_argparser_options()
        if not self.name:
            self.argparser.add_argument("name", help="Script name.")
        self.argparser.add_argument(
            "--loop-mode",
            help="If provided, manager will run in loop mode, with a cycle\
                                    each given number of seconds. Default: %(default)s",
            type=int,
            metavar="SECONDS",
            default=self.loop_mode,
        )
        self.argparser.add_argument(
            "--max-running-jobs",
            type=int,
            default=self.default_max_jobs,
            help="If given, don't allow more than the given jobs running at once.\
                                    Default: %(default)s",
        )

    def parse_args(self):
        args = super().parse_args()
        if not self.name:
            self.name = args.name
        return args

    def wait_for(self, jobs_keys, interval=60, timeout=float("inf"), heartbeat=None):
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
                        logger.info(f"{key} still running")
                    break
                still_running[key] = False

    def on_start(self):
        pass

    def __check_resume_workflow(self):
        resumed_job = None
        for job in self.get_jobs(state=["finished"], meta=["tags"], has_tag=[f"NAME={self.name}"]):
            if self.get_keyvalue_job_tag("FLOW_ID", job["tags"]) == self.flow_id:
                resumed_job = job
                break
        if resumed_job is not None:
            inherited_tags = []
            for tag in resumed_job["tags"]:
                if tag.split("=") == 2:
                    inherited_tags.append(tag)
            self.add_job_tags(tags=inherited_tags)
            self.is_resumed = True

    def resume_workflow(self):
        """
        Implement resume logic
        """
        pass

    def __on_start(self):
        self.__check_resume_workflow()
        if self.is_resumed:
            self.resume_workflow()
        self.on_start()
        self.workflow_loop_enabled = True

    @abc.abstractmethod
    def workflow_loop(self):
        """Implement here your loop code. Return True if want to continue looping,
        False for immediate stop of the job. For continuous looping you also need
        to set `loop_mode`
        """
        pass

    def on_close(self):
        pass

    def __close(self):
        self.on_close()

    def run(self):
        self.__on_start()
        self._run_loops()

    def _base_loop_tasks(self):
        pass

    def _run_loops(self):
        while self.workflow_loop_enabled:
            self._base_loop_tasks()
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
