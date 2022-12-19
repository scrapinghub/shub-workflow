"""
Meta manager. Defines complex workflow in terms of lower level managers

For usage example see tests

"""
import re
import logging
from time import time
from argparse import Namespace
from typing import NewType, Dict, List, Optional, Set, Tuple, DefaultDict
from typing_extensions import TypedDict, NotRequired

from collections import defaultdict, OrderedDict
from copy import copy, deepcopy

import yaml

try:
    from yaml import CLoader

    OLD_YAML = True
except ImportError:
    from yaml import Loader

    OLD_YAML = False


from shub_workflow.script import JobKey, JobDict, Outcome
from shub_workflow.base import WorkFlowManager
from shub_workflow.graph.task import (
    JobGraphDict,
    TaskId,
    BaseTask,
    Resource,
    ResourceAmmount,
    OnFinishKey,
    OnFinishTarget,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


_STARTING_JOB_RE = re.compile("--starting-job(?:=(.+))?")
JobsGraphs = NewType("JobsGraphs", Dict[TaskId, JobGraphDict])


class PendingJobDict(TypedDict):
    wait_for: Set[TaskId]
    is_retry: bool
    wait_time: Optional[int]
    origin: NotRequired[TaskId]


class GraphManager(WorkFlowManager):

    jobs_graph: JobsGraphs = JobsGraphs({})

    def __init__(self):
        # Ensure jobs are traversed in the same order as they went pending.
        self.__pending_jobs: Dict[TaskId, PendingJobDict] = OrderedDict()
        self.__running_jobs: Dict[TaskId, JobKey] = OrderedDict()
        self.__completed_jobs: Dict[TaskId, Tuple[JobKey, Outcome]] = dict()
        self._available_resources: Dict[Resource, ResourceAmmount] = {}  # map resource : ammount
        self._acquired_resources: DefaultDict[Resource, List[Tuple[TaskId, ResourceAmmount]]] = defaultdict(list)
        self.__tasks: Dict[TaskId, BaseTask] = {}
        super(GraphManager, self).__init__()
        self.__start_time: DefaultDict[TaskId, float] = defaultdict(time)
        self.__starting_jobs: List[TaskId] = self.args.starting_job
        for task in self.configure_workflow() or ():
            if self.args.root_jobs:
                self.__starting_jobs.append(task.task_id)
            self._add_task(task)

    @property
    def description(self):
        return f"Workflow manager for {self.name!r}"

    def _add_task(self, task: BaseTask):
        assert task.task_id not in self.jobs_graph, (
            "Workflow inconsistency detected: task %s referenced twice." % task.task_id
        )
        self.jobs_graph[task.task_id] = task.as_jobgraph_dict()
        self.__tasks[task.task_id] = task
        for ntask in task.get_next_tasks():
            self._add_task(ntask)

    def get_task(self, task_id: TaskId) -> BaseTask:
        return self.__tasks[task_id]

    def configure_workflow(self) -> Tuple[BaseTask, ...]:
        raise NotImplementedError("configure_workflow() method need to be implemented.")

    def on_start(self):
        if not self.jobs_graph:
            self.argparser.error("Jobs graph configuration is empty.")
        if not self.__starting_jobs:
            self.argparser.error("You must provide either --starting-job or --root-jobs.")
        self._fill_available_resources()
        self._setup_starting_jobs()
        self.workflow_loop_enabled = True
        logger.info("Starting '%s' workflow", self.name)

    def _setup_starting_jobs(self) -> None:
        for taskid in self.__starting_jobs:
            wait_for: List[TaskId] = self.get_jobdict(taskid).get("wait_for", [])
            self._add_pending_job(taskid, wait_for=tuple(wait_for))

        for taskid in list(self.__pending_jobs.keys()):
            if taskid in self.__completed_jobs:
                jobid, outcome = self.__completed_jobs[taskid]
                self.__pending_jobs.pop(taskid)
                self._check_completed_job(taskid, jobid, outcome)
            elif taskid in self.__running_jobs:
                self.__pending_jobs.pop(taskid)

    def _fill_available_resources(self):
        """
        Ensure there are enough starting resources in order every job
        can run at some point
        """
        for job in self.jobs_graph.keys():
            for required_resources in self.__tasks[job].get_required_resources():
                for resource, req_amount in required_resources.items():
                    old_amount = self._available_resources.get(resource, 0)
                    if old_amount < req_amount:
                        logger.info(
                            "Increasing available resources count for %r"
                            " from %r to %r.  Old value was not enough"
                            " for job %r to run.",
                            resource,
                            old_amount,
                            req_amount,
                            job,
                        )
                        self._available_resources[resource] = req_amount

    def get_jobdict(self, job: TaskId, pop=False) -> JobGraphDict:
        if job not in self.jobs_graph:
            self.argparser.error("Invalid job: %s. Available jobs: %s" % (job, repr(self.jobs_graph.keys())))
        if pop:
            return self.jobs_graph.pop(job)
        return self.jobs_graph[job]

    def _add_pending_job(self, job: TaskId, wait_for=(), is_retry=False):
        self._maybe_add_on_finish_default(job, is_retry)
        if job in self.args.skip_job:
            return
        if job in self.__tasks:
            task = self.__tasks[job]
            parallelization = task.get_parallel_jobs()
        else:
            task_id = self.get_jobdict(job).get("origin", job)
            task = self.__tasks[task_id]
            parallelization = 1
        if parallelization == 1:
            self.__pending_jobs[job] = {
                "wait_for": set(wait_for),
                "is_retry": is_retry,
                "wait_time": task.wait_time,
            }
        else:
            # Split parallelized task into N parallel jobs.
            basejobconf = self.get_jobdict(job, pop=True)
            for i in range(parallelization):
                subtask_id = TaskId("%s.%i" % (job, i))
                subtask_conf = deepcopy(basejobconf)
                subtask_conf["origin"] = job
                subtask_conf["index"] = i

                for _, nextjobs in subtask_conf["on_finish"].items():
                    if i != 0:  # only job 0 will conserve finish targets
                        for nextjob in copy(nextjobs):
                            if nextjob != "retry":
                                if nextjob in self.jobs_graph:
                                    self.get_jobdict(nextjob)["wait_for"].append(subtask_id)
                                    if nextjob in self.__pending_jobs:
                                        self.__pending_jobs[nextjob]["wait_for"].add(subtask_id)
                                else:
                                    for i in range(parallelization):
                                        nextjobp = TaskId("%s.%i" % (job, i))
                                        self.get_jobdict(nextjobp)["wait_for"].append(subtask_id)
                                        if nextjobp in self.__pending_jobs:
                                            self.__pending_jobs[nextjobp]["wait_for"].add(subtask_id)
                                nextjobs.remove(nextjob)
                self.jobs_graph[subtask_id] = subtask_conf
                self.__pending_jobs[subtask_id] = {
                    "wait_for": set(wait_for),
                    "is_retry": is_retry,
                    "origin": job,
                    "wait_time": task.wait_time,
                }
            for other, oconf in self.jobs_graph.items():
                if job in oconf.get("wait_for", []):
                    oconf["wait_for"].remove(job)
                    if other in self.__pending_jobs:
                        self.__pending_jobs[other]["wait_for"].discard(job)
                    for i in range(parallelization):
                        subtask_id = TaskId("%s.%i" % (job, i))
                        oconf["wait_for"].append(subtask_id)
                        if other in self.__pending_jobs:
                            self.__pending_jobs[other]["wait_for"].add(subtask_id)

    def add_argparser_options(self):
        super(GraphManager, self).add_argparser_options()
        self.argparser.add_argument("--jobs-graph", help="Define jobs graph_dict on command line", default="{}")
        self.argparser.add_argument(
            "--starting-job",
            "-s",
            action="append",
            default=[],
            help="Set starting jobs. Can be given multiple times.",
        )
        self.argparser.add_argument(
            "--only-starting-jobs",
            action="store_true",
            help="If given, only run the starting jobs (don't follow on finish next jobs)",
        )
        self.argparser.add_argument(
            "--comment",
            help="Can be used for differentiate command line and avoid scheduling "
            "fail when a graph manager job is scheduled when another one with same option "
            "signature is running. Doesn't do anything else.",
        )
        self.argparser.add_argument(
            "--skip-job",
            default=[],
            action="append",
            help="Skip given job. Can be given multiple times. Also next jobs for the skipped" "one will be skipped.",
        )
        self.argparser.add_argument("--root-jobs", action="store_true", help="Set root jobs as starting jobs.")

    def parse_args(self) -> Namespace:
        args = super(GraphManager, self).parse_args()
        if OLD_YAML:
            self.jobs_graph = yaml.load(args.jobs_graph, Loader=CLoader) or deepcopy(self.jobs_graph)
        else:
            self.jobs_graph = yaml.load(args.jobs_graph, Loader=Loader) or deepcopy(self.jobs_graph)

        if args.starting_job and args.root_jobs:
            self.argparser.error("You can't provide both --starting-job and --root-jobs.")
        return args

    def workflow_loop(self) -> bool:
        logger.debug("Pending jobs: %r", self.__pending_jobs)
        logger.debug("Running jobs: %r", self.__running_jobs)
        logger.debug("Available resources: %r", self._available_resources)
        logger.debug("Acquired resources: %r", self._acquired_resources)
        self.check_running_jobs()
        if self.__pending_jobs:
            self.run_pending_jobs()
        elif not self.__running_jobs:
            return False
        return True

    def run_job(self, job: TaskId, is_retry=False) -> Optional[JobKey]:
        task = self.__tasks.get(job)
        if task is not None:
            return task.run(self, is_retry)

        jobconf = self.get_jobdict(job)
        task = self.__tasks.get(jobconf["origin"])
        if task is not None:
            idx = jobconf["index"]
            return task.run(self, is_retry, index=idx)
        return None

    def _must_wait_time(self, job: TaskId) -> bool:
        status = self.__pending_jobs[job]
        if status["wait_time"] is not None:
            wait_time = status["wait_time"] - time() + self.__start_time[job]
            if wait_time > 0:
                logger.info("Job %s must wait %d seconds for running", job, wait_time)
                return True
        return False

    def run_pending_jobs(self):
        """Try running pending jobs.

        Normally, only jobs that have no outstanding dependencies are started.

        If all pending jobs have outstanding dependencies, try to start one job
        ignoring unknown tasks, i.e. those that are not currently pending.

        If none of the pending jobs cannot be started either way, it means
        there's a dependency cycle, in this case an error is raised.

        """

        # Normal mode: start jobs without dependencies.
        for task_id in sorted(self.__pending_jobs.keys()):
            if len(self.__running_jobs) >= self.max_running_jobs:
                break
            status = self.__pending_jobs[task_id]

            job_can_run = (
                not status["wait_for"] and not self._must_wait_time(task_id) and self._try_acquire_resources(task_id)
            )
            if job_can_run:
                try:
                    jobid = self.run_job(task_id, status["is_retry"])
                except Exception:
                    self._release_resources(task_id)
                    raise
                self.__pending_jobs.pop(task_id)
                self.__running_jobs[task_id] = jobid

        if (
            not self.__pending_jobs
            or self.__running_jobs
            or any(status["wait_time"] is not None for status in self.__pending_jobs.values())
        ):
            return

        # At this point, there are pending jobs, but none were started because
        # of dependencies, try "skip unknown deps" mode: start one job that
        # only has "unseen" dependencies to try to break the "stalemate."
        origin_job = None
        for task_id in sorted(self.__pending_jobs.keys()):
            if len(self.__running_jobs) >= self.max_running_jobs:
                break
            status = self.__pending_jobs[task_id]
            job_can_run = (
                all(w not in self.__pending_jobs for w in status["wait_for"])
                and (not origin_job or status.get("origin") == origin_job)
                and self._try_acquire_resources(task_id)
            )
            origin_job = status.get("origin")
            if job_can_run:
                try:
                    jobid = self.run_job(task_id, status["is_retry"])
                except Exception:
                    self._release_resources(task_id)
                    raise
                self.__pending_jobs.pop(task_id)
                self.__running_jobs[task_id] = jobid
            if not origin_job and self.__running_jobs:
                return

        if self.__running_jobs:
            return

        # Nothing helped, all pending jobs wait for each other somehow.
        raise RuntimeError(
            "Job dependency cycle detected: %s"
            % ", ".join(
                "%s waits for %s" % (task_id, sorted(self.__pending_jobs[task_id]["wait_for"]))
                for task_id in sorted(self.__pending_jobs.keys())
            )
        )

    def get_running_jobid(self, task_id: TaskId) -> JobKey:
        return self.__running_jobs[task_id]

    def handle_retry(self, job: TaskId, outcome: str) -> bool:
        jobconf = self.get_jobdict(job)
        retries = jobconf.get("retries", 0)
        if retries > 0:
            self._add_pending_job(job, is_retry=True)
            jobconf["retries"] -= 1
            logger.warning(
                "Will retry job %s (outcome: %s, number of retries left: %s)",
                job,
                outcome,
                jobconf["retries"],
            )
            return True
        logger.warning("No more retries for failed job %s (outcome: %s)", job, outcome)
        return False

    def check_running_jobs(self) -> None:
        for task_id, jobid in list(self.__running_jobs.items()):
            outcome = self.is_finished(jobid)
            if outcome is not None:
                self._check_completed_job(task_id, jobid, outcome)
                self.__running_jobs.pop(task_id)
            else:
                logger.info("Job %s (%s) still running", task_id, jobid)

    def _check_completed_job(self, task_id: TaskId, jobid: JobKey, outcome: Outcome):
        will_retry = False
        logger.info('Job "%s/%s" (%s) finished', self.name, task_id, jobid)
        for nextjob in self._get_next_jobs(task_id, outcome):
            if nextjob == "retry":
                will_retry = self.handle_retry(task_id, outcome)
                if not will_retry:
                    self.bad_outcome_hook(task_id, jobid, outcome)
            elif nextjob in self.__pending_jobs:
                logger.error("Job %s already pending", nextjob)
            else:
                wait_for = self.get_jobdict(nextjob).get("wait_for", [])
                self._add_pending_job(nextjob, wait_for)
        self._release_resources(task_id)
        if not will_retry:
            self.__completed_jobs[task_id] = jobid, outcome
            for st in self.__pending_jobs.values():
                st["wait_for"].discard(task_id)
            for conf in self.jobs_graph.values():
                if task_id in conf.get("wait_for", []):
                    conf["wait_for"].remove(task_id)

    def bad_outcome_hook(self, task_id: TaskId, jobid: JobKey, outcome: Outcome):
        pass

    def _try_acquire_resources(self, job: TaskId) -> bool:
        result = True
        task_id = self.get_jobdict(job).get("origin", job)
        for required_resources in self.__tasks[task_id].get_required_resources(partial=True):
            for resource, req_amount in required_resources.items():
                if self._available_resources[resource] < req_amount:
                    result = False
                    break
            else:
                for resource, req_amount in required_resources.items():
                    self._available_resources[resource] -= req_amount
                    self._acquired_resources[resource].append((job, req_amount))
                return True
        return result

    def _release_resources(self, job: TaskId):
        for res, acquired in self._acquired_resources.items():
            for rjob, res_amount in acquired:
                if rjob == job:
                    self._available_resources[res] += res_amount
                    self._acquired_resources[res].remove((rjob, res_amount))

    def _maybe_add_on_finish_default(self, job: TaskId, is_retry: bool) -> Dict[OnFinishKey, OnFinishTarget]:
        on_finish = self.get_jobdict(job)["on_finish"]
        task = self.__tasks.get(job)
        if task is not None and not task.is_locked:
            task.get_start_callback()(self, is_retry)
            task.set_is_locked()
            for t in task.get_next_tasks():
                on_finish["default"].append(t.task_id)
                if t.task_id not in self.jobs_graph:
                    self._add_task(t)

        return on_finish

    def _get_next_jobs(self, job: TaskId, outcome: Outcome) -> OnFinishTarget:
        if self.args.only_starting_jobs:
            return []
        on_finish = self.get_jobdict(job)["on_finish"]
        if outcome in on_finish:
            nextjobs = on_finish[outcome]
        elif outcome in self.failed_outcomes:
            nextjobs = on_finish.get("failed", [])
        else:
            nextjobs = on_finish["default"]

        return nextjobs

    @property
    def pending_jobs(self) -> Dict[TaskId, PendingJobDict]:
        return self.__pending_jobs

    def get_job_taskid(self, job: JobDict) -> Optional[TaskId]:
        task_id = self.get_keyvalue_job_tag("TASK_ID", job["tags"])
        if task_id is not None:
            return TaskId(task_id)
        return None

    def resume_running_job_hook(self, job: JobDict):
        task_id = self.get_job_taskid(job)
        if task_id is not None:
            self.__running_jobs[task_id] = job["key"]
            origin_task_id = TaskId(task_id.rsplit(".", 1)[0])
            self.__tasks[origin_task_id].append_jobid(job["key"])

    def resume_finished_job_hook(self, job: JobDict):
        task_id = self.get_job_taskid(job)
        if task_id is not None:
            self.__completed_jobs[task_id] = job["key"], Outcome(job["close_reason"])
            origin_task_id = TaskId(task_id.rsplit(".", 1)[0])
            self.__tasks[origin_task_id].append_jobid(job["key"])
