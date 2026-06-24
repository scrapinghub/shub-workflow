"""
A graph manager: declare a DAG of tasks in configure_workflow(), return the root task(s).

Tasks are scripts (Task) or spiders (SpiderTask); link them with add_next_task / add_wait_for.
The manager schedules each task once its dependencies are satisfied, until nothing is pending or
running. Invoke with a name and a start directive, e.g.:  python flowmanager.py mygraph --root-jobs
"""
import time
import logging

from shub_workflow.graph import GraphManager
from shub_workflow.graph.task import Task, SpiderTask

LOG = logging.getLogger(__name__)


class MyGraphManager(GraphManager):

    loop_mode = 120        # REQUIRED — it's a loop manager
    # name: from the `name` class attribute or the first positional CLI arg (WorkFlowManager)

    def configure_workflow(self):
        # a spider task (scheduled via schedule_spider; **kwargs become spider args)
        discover = SpiderTask("discover", "example_spider", category="news")

        # a script task (command = script name or a Jinja template; init_args appended)
        crawl = Task("crawl", "py:crawlmanager.py", init_args=["example.com"], retries=2)

        deliver = Task(
            "deliver",
            "py:deliver.py",
            init_args=["example.com", f"--output-file=s3://bucket/{int(time.time())}.jl"],
        )

        # edges: discover -> crawl -> deliver
        discover.add_next_task(crawl)
        crawl.add_next_task(deliver)
        # a pure dependency (run after, without being a "next" of): deliver.add_wait_for(other)

        return (discover,)            # root task(s)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MyGraphManager().run()
