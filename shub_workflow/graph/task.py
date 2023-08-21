import logging
import shlex
import abc
from fractions import Fraction
from typing import NewType, List, Dict, Optional, Union, Literal, Callable, Protocol
from typing_extensions import TypedDict, NotRequired

from jinja2 import Template

from shub_workflow.script import JobKey
from shub_workflow.base import WorkFlowManagerProtocol, Outcome


logger = logging.getLogger(__name__)

Resource = NewType("Resource", str)
ResourceAmmount = Union[int, Fraction]
ResourcesDict = NewType("ResourcesDict", Dict[Resource, ResourceAmmount])
TaskId = NewType("TaskId", str)
OnFinishKey = Union[Outcome, Literal["default", "failed"]]
OnFinishTarget = List[Union[Literal["retry"], TaskId]]


class GraphManagerProtocol(WorkFlowManagerProtocol, Protocol):
    @abc.abstractmethod
    def get_task(self, task_id: TaskId) -> "BaseTask":
        ...


class JobGraphDict(TypedDict):
    tags: Optional[List[str]]
    units: Optional[int]
    on_finish: Dict[OnFinishKey, OnFinishTarget]
    wait_for: List[TaskId]
    retries: int
    wait_time: Optional[int]
    project_id: Optional[int]

    command: NotRequired[List[str]]
    init_args: NotRequired[List[str]]
    retry_args: NotRequired[List[str]]

    origin: NotRequired[TaskId]
    index: NotRequired[int]

    spider: NotRequired[str]
    spider_args: NotRequired[Dict[str, str]]


class BaseTask(abc.ABC):
    def __init__(
        self,
        task_id: TaskId,
        tags: Optional[List[str]] = None,
        units: Optional[int] = None,
        retries: int = 1,
        project_id: Optional[int] = None,
        wait_time: Optional[int] = None,
        on_finish: Optional[Dict[OnFinishKey, OnFinishTarget]] = None,
    ):
        assert task_id != "retry", "Reserved word 'retry' can't be used as task id"
        self.task_id = task_id
        self.tags = tags
        self.units = units
        self.retries = retries
        self.project_id = project_id
        self.wait_time = wait_time
        self.on_finish: Dict[OnFinishKey, OnFinishTarget] = on_finish or {}

        self.__is_locked: bool = False
        self.__next_tasks: List[BaseTask] = []
        self.__wait_for: List[BaseTask] = []
        self.__required_resources: List[ResourcesDict] = []
        self.__start_callback: Callable[[GraphManagerProtocol, bool], None]

        self.__job_ids: List[JobKey] = []

        self.set_start_callback(self._default_start_callback)

    def set_is_locked(self):
        self.__is_locked = True

    @property
    def is_locked(self) -> bool:
        return self.__is_locked

    def append_jobid(self, jobid: JobKey):
        self.__job_ids.append(jobid)

    def get_scheduled_jobs(self) -> List[JobKey]:
        """
        - Returns the task job ids
        """
        return self.__job_ids

    def add_next_task(self, task: "BaseTask"):
        assert not self.__is_locked, "You can't alter a locked job."
        self.__next_tasks.append(task)

    def add_wait_for(self, task: "BaseTask"):
        assert not self.__is_locked, "You can't alter a locked job."
        self.__wait_for.append(task)

    def add_required_resources(self, resources_dict: ResourcesDict):
        assert not self.__is_locked, "You can't alter a locked job."
        self.__required_resources.append(resources_dict)

    def get_next_tasks(self) -> List["BaseTask"]:
        return self.__next_tasks

    def get_required_resources(self, partial: bool = False) -> List[ResourcesDict]:
        """
        If partial is True, return required resources for each splitted job.
        Otherwise return the resouces required for the full task.
        """
        if not partial:
            return self.__required_resources
        required_resources = []
        parallelization = self.get_parallel_jobs()
        for reqset in self.__required_resources:
            reqres: ResourcesDict = ResourcesDict({})
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

    def get_wait_for(self) -> List["BaseTask"]:
        return self.__wait_for

    def as_jobgraph_dict(self) -> JobGraphDict:
        jdict: JobGraphDict = {
            "tags": self.tags,
            "units": self.units,
            "on_finish": self.on_finish,
            "wait_for": [t.task_id for t in self.get_wait_for()],
            "retries": self.retries,
            "project_id": self.project_id,
            "wait_time": self.wait_time,
        }
        self.on_finish["default"] = []
        if self.retries > 0:
            self.on_finish["failed"] = ["retry"]

        return jdict

    def set_start_callback(self, func: Callable[[GraphManagerProtocol, bool], None]):
        self.__start_callback = func

    def get_start_callback(self) -> Callable[[GraphManagerProtocol, bool], None]:
        assert self.__start_callback is not None, "Start callback not initialized."
        return self.__start_callback

    def _default_start_callback(self, manager: GraphManagerProtocol, is_retry: bool):
        pass

    @abc.abstractmethod
    def run(self, manager: GraphManagerProtocol, is_retry=False, index: Optional[int] = None) -> Optional[JobKey]:
        ...

    @abc.abstractmethod
    def get_parallel_jobs(self):
        """
        Returns total number of parallel jobs that this task will consist on.
        """


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
        super().__init__(task_id, tags, units, retries, project_id, wait_time, on_finish)
        assert "." not in self.task_id, ". is not allowed in task name."
        self.command = command
        self.init_args = init_args or []
        self.retry_args = retry_args or []
        self.__template = Template(self.command)

    def as_jobgraph_dict(self) -> JobGraphDict:
        jdict = super().as_jobgraph_dict()
        jdict.update({"command": self.get_commands(), "init_args": self.init_args, "retry_args": self.retry_args})
        return jdict

    def get_commands(self) -> List[str]:
        return self.__template.render().splitlines()

    def get_command(self, index: int = 0) -> List[str]:
        return shlex.split(self.get_commands()[index])

    def get_parallel_jobs(self) -> int:
        """
        Returns total number of parallel jobs that this task will consist on.
        """
        return len(self.get_commands())

    def run(
        self, manager: GraphManagerProtocol, is_retry: bool = False, index: Optional[int] = None
    ) -> Optional[JobKey]:
        command = self.get_command(index or 0)
        tags = []
        if self.tags is not None:
            tags.extend(self.tags)
        if index is None:
            jobname = f"{manager.name}/{self.task_id}"
            tags.append(f"TASK_ID={self.task_id}")
        else:
            jobname = f"{manager.name}/{self.task_id}.{index}"
            tags.append(f"TASK_ID={self.task_id}.{index}")
        if is_retry:
            logger.info('Will retry task "%s"', jobname)
        else:
            logger.info('Will start task "%s"', jobname)
        if is_retry:
            retry_args = self.retry_args or self.init_args
            cmd = command + retry_args
        else:
            cmd = command + self.init_args

        jobid = manager.schedule_script(cmd, tags=tags, units=self.units, project_id=self.project_id)
        if jobid is not None:
            logger.info('Scheduled task "%s" (%s)', jobname, jobid)
            self.append_jobid(jobid)
            return jobid
        return None


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
        super().__init__(task_id, tags, units, retries, None, wait_time, on_finish)
        self.spider = spider
        self.__spider_args = spider_args
        self.__job_settings = job_settings

    def get_spider_args(self):
        spider_args = self.__spider_args
        if self.__job_settings is not None:
            spider_args.update({"job_settings": self.__job_settings})
        return spider_args

    def as_jobgraph_dict(self) -> JobGraphDict:
        jdict = super().as_jobgraph_dict()
        jdict.update({"spider": self.spider, "spider_args": self.get_spider_args()})
        return jdict

    def get_parallel_jobs(self):
        return 1

    def run(self, manager: GraphManagerProtocol, is_retry=False, index: Optional[int] = None) -> Optional[JobKey]:
        assert index is None, "Spider Task don't support parallelization."
        jobname = f"{manager.name}/{self.task_id}"
        if is_retry:
            logger.info('Will retry spider "%s"', jobname)
        else:
            logger.info('Will start spider "%s"', jobname)
        tags = []
        if self.tags is not None:
            tags.extend(self.tags)
        tags.append(f"TASK_ID={self.task_id}")
        jobid = manager.schedule_spider(
            self.spider, tags=tags, units=self.units, project_id=self.project_id, **self.get_spider_args()
        )
        if jobid is not None:
            logger.info('Scheduled spider "%s" (%s)', jobname, jobid)
            self.append_jobid(jobid)
            return jobid
        return None
