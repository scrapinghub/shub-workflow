import re
from typing import List, Tuple, Optional, cast, Literal

from shub_workflow.base import WorkFlowManager
from shub_workflow.script import JobKey

_SCHEDULED_RE = re.compile(r"Scheduled (?:(task|spider) \"(.+?)\" \()?.*?(\d+/\d+/\d+)\)?", re.I)


def _search_scheduled_line(txt: str) -> Optional[Tuple[str, str, str]]:
    """
    graph manager scheduled task
    >>> _search_scheduled_line('Scheduled task "totalwine/productsJob" (168012/20/62)')
    ('task', 'totalwine/productsJob', '168012/20/62')

    graph manager scheduled spider
    >>> _search_scheduled_line('Scheduled spider "totalwine/storesJob" (168012/27/2)')
    ('spider', 'totalwine/storesJob', '168012/27/2')
    """
    m = _SCHEDULED_RE.search(txt)
    if m is not None:
        return cast(Tuple[Literal["task", "spider"], str, JobKey], m.groups())
    return None


def get_scheduled_jobs_specs(manager: WorkFlowManager, job_ids: List[JobKey]) -> List[Tuple[str, str, str]]:
    """
    Return the jobs specs of the jobs scheduled by the jobs identified
    by given job_ids

    Each job spec is a 3-element tuple which contains, in order:
        - the kind of task job (spider/task)
        - the complete id name of the task job
        - the job id of the of the task job
    """
    scheduled_jobs = []
    for jobid in job_ids:
        project_id = jobid.split("/")[0]
        job = manager.get_project(project_id).jobs.get(jobid)
        for logline in job.logs.iter():
            if "message" not in logline:
                continue
            m = _search_scheduled_line(logline["message"])
            if m is not None:
                scheduled_jobs.append(m)
    return scheduled_jobs
