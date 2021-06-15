"""
Meta manager. Defines complex workflow in terms of lower level managers

For usage example see tests

"""
import re
from time import time
import logging

from collections import defaultdict, OrderedDict as odict
from copy import copy, deepcopy

import yaml

from shub_workflow.base import WorkFlowManager


logger = logging.getLogger(__name__)


_STARTING_JOB_RE = re.compile("--starting-job(?:=(.+))?")


class GraphManager(WorkFlowManager):

    jobs_graph = {}

    def __init__(self):
        self.__failed_outcomes = list(self.base_failed_outcomes)
        # Ensure jobs are traversed in the same order as they went pending.
        self.__pending_jobs = odict()
        self.__running_jobs = odict()
        self._available_resources = {}  # map resource : ammount
        self._acquired_resources = defaultdict(list)  # map resource : list of (job, ammount)
        self.__tasks = {}
        super(GraphManager, self).__init__()
        self.__start_time = defaultdict(time)
        self.__starting_jobs = self.args.starting_job
        for task in self.configure_workflow() or ():
            if self.args.root_jobs:
                self.__starting_jobs.append(task.task_id)
            self._add_task(task)

    @property
    def description(self):
        return f"Workflow manager for {self.name!r}"

    def _add_task(self, task):
        assert task.task_id not in self.jobs_graph, (
            "Workflow inconsistency detected: task %s referenced twice." % task.task_id
        )
        self.jobs_graph[task.task_id] = task.as_jobgraph_dict()
        self.__tasks[task.task_id] = task
        for ntask in task.get_next_tasks():
            self._add_task(ntask)

    def configure_workflow(self):
        raise NotImplementedError("configure_workflow() method need to be implemented.")

    def resume_workflow(self):
        # TODO: implement
        raise NotImplementedError()

    def on_start(self):
        if not self.jobs_graph:
            self.argparser.error("Jobs graph configuration is empty.")
        self.__starting_jobs = self.args.starting_job or self.__starting_jobs
        if not self.__starting_jobs:
            self.argparser.error("You must provide either --starting-job or --root-jobs.")
        self._fill_available_resources()
        self._setup_starting_jobs([])
        self.workflow_loop_enabled = True
        logger.info("Starting '%s' workflow", self.name)

    def _setup_starting_jobs(self, ran_tasks, candidates=None):
        candidates = candidates or self.__starting_jobs
        for taskid in candidates:
            if taskid in ran_tasks:
                logger.info(
                    "Task %s already done %s.", taskid, tuple(self.__tasks[taskid].get_scheduled_jobs()),
                )
                next_tasks = [t.task_id for t in self.__tasks[taskid].get_next_tasks()]
                if next_tasks:
                    self._setup_starting_jobs(ran_tasks, next_tasks)
            else:
                self._add_initial_pending_job(taskid)
                logger.info("Resuming at task %s", taskid)

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

    def get_job(self, job, pop=False):
        if job not in self.jobs_graph:
            self.argparser.error("Invalid job: %s. Available jobs: %s" % (job, repr(self.jobs_graph.keys())))
        if pop:
            return self.jobs_graph.pop(job)
        return self.jobs_graph[job]

    def _add_initial_pending_job(self, job):
        wait_for = self.get_job(job).get("wait_for", [])
        self._add_pending_job(job, wait_for=tuple(wait_for))

    def _add_pending_job(self, job, wait_for=(), is_retry=False):
        if job in self.args.skip_job:
            return
        if job in self.__tasks:
            task = self.__tasks[job]
            parallelization = task.get_parallel_jobs()
        else:
            task_id = self.get_job(job).get("origin", job)
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
            basejobconf = self.get_job(job, pop=True)
            for i in range(parallelization):
                job_unit = "%s_%i" % (job, i)
                job_unit_conf = deepcopy(basejobconf)
                job_unit_conf["origin"] = job
                job_unit_conf["index"] = i

                for _, nextjobs in job_unit_conf.get("on_finish", {}).items():
                    if i != 0:  # only job 0 will conserve finish targets
                        for nextjob in copy(nextjobs):
                            if nextjob != "retry":
                                if nextjob in self.jobs_graph:
                                    self.get_job(nextjob).setdefault("wait_for", []).append(job_unit)
                                    if nextjob in self.__pending_jobs:
                                        self.__pending_jobs[nextjob]["wait_for"].add(job_unit)
                                else:
                                    for i in range(parallelization):
                                        nextjobp = "%s_%i" % (job, i)
                                        self.get_job(nextjobp).get("wait_for", []).append(job_unit)
                                        if nextjobp in self.__pending_jobs:
                                            self.__pending_jobs[nextjobp]["wait_for"].add(job_unit)
                                nextjobs.remove(nextjob)
                self.jobs_graph[job_unit] = job_unit_conf
                self.__pending_jobs[job_unit] = {
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
                        job_unit = "%s_%i" % (job, i)
                        oconf["wait_for"].append(job_unit)
                        if other in self.__pending_jobs:
                            self.__pending_jobs[other]["wait_for"].add(job_unit)

    def add_argparser_options(self):
        super(GraphManager, self).add_argparser_options()
        self.argparser.add_argument("--jobs-graph", help="Define jobs graph_dict on command line", default="{}")
        self.argparser.add_argument(
            "--starting-job", "-s", action="append", default=[], help="Set starting jobs. Can be given multiple times.",
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

    def parse_args(self):
        args = super(GraphManager, self).parse_args()
        self.jobs_graph = yaml.load(args.jobs_graph) or deepcopy(self.jobs_graph)

        if not self.name:
            self.argparser.error("Manager name not set.")
        if args.starting_job and args.root_jobs:
            self.argparser.error("You can't provide both --starting-job and --root-jobs.")
        return args

    def workflow_loop(self):
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

    def run_job(self, job, is_retry=False):
        task = self.__tasks.get(job)
        if task is not None:
            return task.run(self, is_retry)

        jobconf = self.get_job(job)
        task = self.__tasks.get(jobconf["origin"])
        if task is not None:
            idx = jobconf["index"]
            return task.run(self, is_retry, index=idx)

    def _must_wait_time(self, job):
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
        for job in sorted(self.__pending_jobs.keys()):
            if len(self.__running_jobs) >= self.max_running_jobs:
                break
            status = self.__pending_jobs[job]

            job_can_run = not status["wait_for"] and not self._must_wait_time(job) and self._try_acquire_resources(job)
            if job_can_run:
                try:
                    jobid = self.run_job(job, status["is_retry"])
                except:
                    self._release_resources(job)
                    raise
                self.__pending_jobs.pop(job)
                self.__running_jobs[job] = jobid

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
        for job in sorted(self.__pending_jobs.keys()):
            if len(self.__running_jobs) >= self.max_running_jobs:
                break
            status = self.__pending_jobs[job]
            job_can_run = (
                all(w not in self.__pending_jobs for w in status["wait_for"])
                and (not origin_job or status.get("origin") == origin_job)
                and self._try_acquire_resources(job)
            )
            origin_job = status.get("origin")
            if job_can_run:
                try:
                    jobid = self.run_job(job, status["is_retry"])
                except:
                    self._release_resources(job)
                    raise
                self.__pending_jobs.pop(job)
                self.__running_jobs[job] = jobid
            if not origin_job and self.__running_jobs:
                return

        if self.__running_jobs:
            return

        # Nothing helped, all pending jobs wait for each other somehow.
        raise RuntimeError(
            "Job dependency cycle detected: %s"
            % ", ".join(
                "%s waits for %s" % (job, sorted(self.__pending_jobs[job]["wait_for"]))
                for job in sorted(self.__pending_jobs.keys())
            )
        )

    def check_running_jobs(self):
        for job, jobid in list(self.__running_jobs.items()):
            outcome = self.is_finished(jobid)
            if outcome is not None:
                logger.info('Job "%s/%s" (%s) finished', self.name, job, jobid)
                for st in self.__pending_jobs.values():
                    st["wait_for"].discard(job)
                for conf in self.jobs_graph.values():
                    if job in conf.get("wait_for", []):
                        conf["wait_for"].remove(job)
                for nextjob in self._get_next_jobs(job, outcome):
                    if nextjob == "retry":
                        jobconf = self.get_job(job)
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
                    elif nextjob in self.__pending_jobs:
                        logger.error("Job %s already pending", nextjob)
                    else:
                        wait_for = self.get_job(nextjob).get("wait_for", [])
                        self._add_pending_job(nextjob, wait_for)
                self._release_resources(job)
                self.__running_jobs.pop(job)
            else:
                logger.info("Job %s (%s) still running", job, jobid)

    def _try_acquire_resources(self, job):
        result = True
        task_id = self.get_job(job).get("origin", job)
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

    def _release_resources(self, job):
        for res, acquired in self._acquired_resources.items():
            for rjob, res_amount in acquired:
                if rjob == job:
                    self._available_resources[res] += res_amount
                    self._acquired_resources[res].remove((rjob, res_amount))

    def _get_next_jobs(self, job, outcome):
        if self.args.only_starting_jobs:
            return []
        on_finish = self.get_job(job).get("on_finish", {})
        if outcome in on_finish:
            nextjobs = on_finish[outcome]
        elif outcome in self.__failed_outcomes:
            nextjobs = on_finish.get("failed", [])
        else:
            nextjobs = on_finish.get("default", [])
        return nextjobs

    @property
    def pending_jobs(self):
        return self.__pending_jobs
