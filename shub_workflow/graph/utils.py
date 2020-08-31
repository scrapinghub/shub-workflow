import re


_SCHEDULED_RE = re.compile(r"Scheduled (?:(task|spider) \"(.+?)\" \()?.*?(\d+/\d+/\d+)\)?", re.I)


def _search_scheduled_line(txt):
    """
    graph manager scheduled task
    >>> _search_scheduled_line('Scheduled task "totalwine/productsJob" (168012/20/62)')
    ('task', 'totalwine/productsJob', '168012/20/62')

    graph manager scheduled spider
    >>> _search_scheduled_line('Scheduled spider "totalwine/storesJob" (168012/27/2)')
    ('spider', 'totalwine/storesJob', '168012/27/2')
    """
    m = _SCHEDULED_RE.search(txt)
    if m:
        return m.groups()


def get_scheduled_jobs_specs(manager, job_ids):
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
        project_id = jobid.split('/')[0]
        job = manager.get_project(project_id).jobs.get(jobid)
        for l in job.logs.iter():
            if 'message' not in l:
                continue
            m = _search_scheduled_line(l['message'])
            if m:
                scheduled_jobs.append(m)
    return scheduled_jobs
