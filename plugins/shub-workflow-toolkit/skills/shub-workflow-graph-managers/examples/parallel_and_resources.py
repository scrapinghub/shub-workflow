"""
Advanced graph features: parallel tasks, resources, outcome-based routing, and bad_outcome_hook.

- Parallelization: a Task whose command template renders N lines splits into N parallel subtasks
  (task_id.0 ... task_id.N-1); successors wait for all of them.
- Resources: named semaphores that gate how many tasks of a class run at once, independent of
  --max-running-jobs.
- on_finish: route to different next tasks per close-reason outcome.
- bad_outcome_hook: react when a task failed and has no retries left.
"""
import logging

from shub_workflow.graph import GraphManager
from shub_workflow.graph.task import Task, Resource, ResourcesDict

LOG = logging.getLogger(__name__)

DB = Resource("db")


class MyGraphManager(GraphManager):

    loop_mode = 60

    def configure_workflow(self):
        # 4 parallel subtasks: crawl.0 .. crawl.3 (multi-line rendered command)
        crawl = Task("crawl", "{% for i in range(4) %}py:crawlmanager.py --part={{ i }}\n{% endfor %}")

        # route by outcome: on a clean finish -> deliver; on a custom "no_items" close reason -> alert
        deliver = Task("deliver", "py:deliver.py", init_args=["--output-file=s3://bucket/out.jl"])
        alert = Task("alert", "py:alert.py", retries=0)
        crawl.on_finish = {"finished": ["deliver"], "no_items": ["alert"]}
        # (without on_finish, add_next_task() targets are the "default" route)

        # only one DB-using task may run at a time, regardless of max_running_jobs
        deliver.add_required_resources(ResourcesDict({DB: 1}))

        # register tasks reachable only via on_finish by linking them so they enter the graph
        crawl.add_next_task(deliver)
        crawl.add_next_task(alert)

        return (crawl,)

    def bad_outcome_hook(self, task_id, jobid, outcome):
        # called when a task failed and exhausted its retries
        LOG.error("Task %s (%s) failed for good: %s", task_id, jobid, outcome)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MyGraphManager().run()
