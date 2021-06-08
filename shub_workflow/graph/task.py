import logging
import shlex
import abc
from collections import namedtuple
from fractions import Fraction

from jinja2 import Template

from .utils import get_scheduled_jobs_specs


logger = logging.getLogger(__name__)


Resource = namedtuple("Resource", ["name"])


class BaseTask(abc.ABC):
    def __init__(self, task_id, tags=None, units=None, retries=1, project_id=None, wait_time=None, on_finish=None):
        assert task_id != "retry", "Reserved word 'retry' can't be used as task id"
        self.task_id = task_id
        self.tags = tags
        self.units = units
        self.retries = retries
        self.project_id = project_id
        self.wait_time = wait_time
        self.on_finish = on_finish or {}

        self.__next_tasks = []
        self.__wait_for = []
        self.__required_resources = []

        self.__job_ids = []

    def append_jobid(self, jobid):
        self.__job_ids.append(jobid)

    def get_scheduled_jobs(self):
        """
        - Returns the task job ids
        """
        return self.__job_ids

    def add_next_task(self, task):
        self.__next_tasks.append(task)

    def add_wait_for(self, task):
        self.__wait_for.append(task)

    def add_required_resources(self, resources_dict):
        self.__required_resources.append(resources_dict)

    def get_next_tasks(self):
        return self.__next_tasks

    def get_required_resources(self, partial=False):
        """
        If partial is True, return required resources for each splitted job.
        Otherwise return the resouces required for the full task.
        """
        if not partial:
            return self.__required_resources
        required_resources = []
        parallelization = self.get_parallel_jobs()
        for reqset in self.__required_resources:
            reqres = {}
            for resource, req_amount in reqset.items():
                # Split required resource into N parts.  There are two
                # ideas behind this:
                #
                # - if the job in whole requires some resources, each of
                #   its N parts should be using 1/N of that resource
                #
                # - in most common scenario when 1 unit of something is
                #   required, allocating 1/N of it means that when we start
                #   one unit job, we can start another unit job to allocate
                #   2/N, but not a completely different job (as it would
                #   consume (1 + 1/N) of the resource.
                #
                # Use fraction to avoid any floating point quirks.
                reqres[resource] = Fraction(req_amount, parallelization)
            required_resources.append(reqres)
        return required_resources

    def get_wait_for(self):
        return self.__wait_for

    def as_jobgraph_dict(self):
        jdict = {
            "tags": self.tags,
            "units": self.units,
            "on_finish": self.on_finish,
            "wait_for": [t.task_id for t in self.get_wait_for()],
        }
        next_tasks = self.get_next_tasks()
        if next_tasks:
            jdict["on_finish"]["default"] = [t.task_id for t in next_tasks]
        if self.retries > 0:
            jdict["retries"] = self.retries
            jdict["on_finish"]["failed"] = ["retry"]
        if self.project_id:
            jdict["project_id"] = self.project_id
        if self.wait_time:
            jdict["wait_time"] = self.wait_time

        return jdict

    def start_callback(self, manager, is_retry):
        pass

    @abc.abstractmethod
    def run(self, manager, is_retry=False):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_parallel_jobs(self):
        """
        Returns total number of parallel jobs that this task will consist on.
        """
        pass


class Task(BaseTask):
    def __init__(
        self,
        task_id,
        command,
        init_args=None,
        retry_args=None,
        tags=None,
        units=None,
        retries=1,
        project_id=None,
        wait_time=None,
        on_finish=None,
    ):
        """
        id - String. identifies the task.
        command - String. script name or jinja2 template string.
        init_args - List of strings. Arguments and options to add to the command.
        retry_args - List of strings. If given and job is retries, use this list of arguments instead the ones
                     specified in init_args.
        tags - List of strings. tags to add to the scheduled job.
        units - Int. units to use for this scheduled job.
        retries - Int. Max number of retries in case job failed.
        project_id - Int. Run task in given project. If not given, just run in the actual project.
        wait_time - Int. Don't run the task before the given number of seconds after job goes to pending status.
        """
        super(Task, self).__init__(task_id, tags, units, retries, project_id, wait_time, on_finish)
        self.command = command
        self.init_args = init_args or []
        self.retry_args = retry_args or []
        self.__template = Template(self.command)

    def as_jobgraph_dict(self):
        jdict = super(Task, self).as_jobgraph_dict()
        jdict.update(
            {"command": self.get_commands(), "init_args": self.init_args, "retry_args": self.retry_args}
        )
        return jdict

    def get_commands(self):
        return self.__template.render().splitlines()

    def get_command(self, index=None):
        index = index or 0
        return shlex.split(self.get_commands()[index])

    def get_parallel_jobs(self):
        """
        Returns total number of parallel jobs that this task will consist on.
        """
        return len(self.get_commands())

    def get_scheduled_jobs(self, manager=None, level=0):
        """
        - if level == 0, just return the task job ids (this is a second way to access self.job_ids)
        - if level == 1, return the job ids of the jobs scheduled by this task jobs (typically, a crawl
          manager, so returned job ids are the ids of the spider jobs scheduled by it). In this case, a
          second parameter, the manager instance that schedules this task, must be provided.
        """
        job_ids = super(Task, self).get_scheduled_jobs()
        if level == 0:
            return job_ids
        assert level == 1, "Invalid level"
        return [j[2] for j in get_scheduled_jobs_specs(manager, job_ids)]

    def run(self, manager, is_retry=False, index=None):
        command = self.get_command(index)
        self.start_callback(manager, is_retry)
        if index is None:
            jobname = f"{manager.name}/{self.task_id}"
        else:
            jobname = f"{manager.name}/{self.task_id}_{index}"
        if is_retry:
            logger.info('Will retry task "%s"', jobname)
        else:
            logger.info('Will start task "%s"', jobname)
        if is_retry:
            retry_args = self.retry_args or self.init_args
            cmd = command + retry_args
        else:
            cmd = command + self.init_args
        jobid = manager.schedule_script(cmd, tags=self.tags, units=self.units, project_id=self.project_id)
        if jobid:
            logger.info('Scheduled task "%s" (%s)', jobname, jobid)
            self.append_jobid(jobid)
            return jobid


class SpiderTask(BaseTask):
    """
    A simple spider task.
    """

    def __init__(
        self,
        task_id,
        spider,
        tags=None,
        units=None,
        retries=1,
        wait_time=None,
        on_finish=None,
        job_settings=None,
        **spider_args,
    ):
        super(SpiderTask, self).__init__(task_id, tags, units, retries, None, wait_time, on_finish)
        self.spider = spider
        self.__spider_args = spider_args
        self.__job_settings = job_settings

    def get_spider_args(self):
        spider_args = self.__spider_args
        if self.__job_settings is not None:
            spider_args.update({"job_settings": self.__job_settings})
        return spider_args

    def as_jobgraph_dict(self):
        jdict = super(SpiderTask, self).as_jobgraph_dict()
        jdict.update(
            {"spider": self.spider, "spider_args": self.get_spider_args()}
        )
        return jdict

    def get_parallel_jobs(self):
        return 1

    def run(self, manager, is_retry=False):
        self.start_callback(manager, is_retry)
        jobname = "{}/{}".format(manager.name, self.task_id)
        if is_retry:
            logger.info('Will retry spider "%s"', jobname)
        else:
            logger.info('Will start spider "%s"', jobname)
        jobid = manager.schedule_spider(
            self.spider, tags=self.tags, units=self.units, project_id=self.project_id, **self.get_spider_args()
        )
        if jobid:
            logger.info('Scheduled spider "%s" (%s)', jobname, jobid)
            self.append_jobid(jobid)
            return jobid
