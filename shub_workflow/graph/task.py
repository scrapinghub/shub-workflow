import logging
from collections import namedtuple

from .utils import get_scheduled_jobs_specs


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


Resource = namedtuple('Resource', ['name'])


class BaseTask(object):
    def __init__(self, task_id, tags=None, units=None, retries=1, project_id=None, wait_time=None):
        assert task_id != 'retry', "Reserved word 'retry' can't be used as task id"
        self.task_id = task_id
        self.tags = tags
        self.units = units
        self.retries = retries
        self.project_id = project_id
        self.wait_time = wait_time

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

    def add_required_resources(self, *rargs):
        self.__required_resources.append(rargs)

    def get_next_tasks(self):
        return self.__next_tasks

    def get_required_resources(self):
        return self.__required_resources

    def get_wait_for(self):
        return self.__wait_for

    def as_jobgraph_dict(self):
        jdict = {
            'tags': self.tags,
            'units': self.units,
            'on_finish': {},
            'required_resources': [],
            'wait_for': [t.task_id for t in self.get_wait_for()],
        }
        next_tasks = self.get_next_tasks()
        if next_tasks:
            jdict['on_finish']['default'] = [t.task_id for t in next_tasks]
        if self.retries > 0:
            jdict['retries'] = self.retries
            jdict['on_finish']['failed'] = ['retry']
        for _rscs in self.get_required_resources():
            as_dict = {}
            while _rscs:
                as_dict[_rscs[0].name] = _rscs[1]
                _rscs = _rscs[2:]
            jdict['required_resources'].append(as_dict)
        if self.project_id:
            jdict['project_id'] = self.project_id
        if self.wait_time:
            jdict['wait_time'] = self.wait_time

        return jdict

    def start_callback(self, manager, retries):
        pass

    def run(self, manager, retries=False):
        raise NotImplementedError()


class Task(BaseTask):
    def __init__(self, task_id, command, init_args=None, parallel_arg=None,
                 retry_args=None, tags=None, units=None, retries=1,
                 project_id=None, wait_time=None):
        """
        id - String. identifies the task.
        command - String. script name.
        init_args - List of strings. Arguments and options to add to the command.
        parallel_args - String. Specify an option/argument template that depends on parallel pipeline.
                        %d will be replaced by the specific pipeline index.
        retry_args - List of strings. If given and job is retries, use this list of arguments instead the ones
                     specified in init_args.
        tags - List of strings. tags to add to the scheduled job.
        units - Int. units to use for this scheduled job.
        retries - Int. Number of retries in case job failed.
        project_id - Int. Run task in given project. If not given, just run in the actual project.
        wait_time - Int. Don't run the task before the given number of seconds after workflow started.
        """
        super(Task, self).__init__(task_id, tags, units, retries, project_id, wait_time)
        self.command = command
        self.init_args = init_args or []
        self.parallel_arg = parallel_arg
        self.retry_args = retry_args or []

    def as_jobgraph_dict(self):
        jdict = super(Task, self).as_jobgraph_dict()
        jdict.update({
            'command': self.command,
            'init_args': self.init_args,
            'retry_args': self.retry_args,
            'parallel_arg': self.parallel_arg,
        })
        return jdict

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

    def run(self, manager, retries=False):
        self.start_callback(manager, retries)
        jobname = "{}/{}".format(manager.name, self.task_id)
        if retries:
            logger.info('Will retry task "%s"', jobname)
        else:
            logger.info('Will start task "%s"', jobname)
        if retries:
            cmd = [self.command] + self.retry_args
        else:
            cmd = [self.command] + self.init_args
        jobid = manager.schedule_script(cmd, tags=self.tags, units=self.units, project_id=self.project_id)
        if jobid:
            logger.info('Scheduled task "%s" (%s)', jobname, jobid)
            self.append_jobid(jobid)
            return jobid


class SpiderTask(BaseTask):
    """
    A simple spider task.
    """
    def __init__(self, task_id, spider, tags=None, units=None, retries=1, wait_time=None,
                 **spider_args):
        super(SpiderTask, self).__init__(task_id, tags, units, retries, None, wait_time)
        self.spider = spider
        self.spider_args = spider_args

    def as_jobgraph_dict(self):
        jdict = super(SpiderTask, self).as_jobgraph_dict()
        jdict.update({
            'spider': self.spider,
            'spider_args': self.spider_args,
        })
        return jdict

    def run(self, manager, retries=False):
        self.start_callback(manager, retries)
        jobname = "{}/{}".format(manager.name, self.task_id)
        if retries:
            logger.info('Will retry spider "%s"', jobname)
        else:
            logger.info('Will start spider "%s"', jobname)
        jobid = manager.schedule_spider(self.spider, self.tags, self.units, **self.spider_args)
        if jobid:
            logger.info('Scheduled spider "%s" (%s)', jobname, jobid)
            self.append_jobid(jobid)
            return jobid
