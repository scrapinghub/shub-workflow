# hack the base library path into syspath
import logging
import os
import sys
import yaml
from collections import defaultdict

# NOTICE: Remove this try/except block if you have shub_workflow
# package installed in your environment
try:
    import shub_workflow
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from shub_workflow.graph import GraphManager
from shub_workflow.graph.task import SpiderTask


logger = logging.getLogger(__name__)


class TestManager(GraphManager):
    name = "test"

    # map spider_name: number_of_finished_jobs
    spider_run_count = defaultdict(int)

    def configure_workflow(self):
        """ How the following lines work and what we are doing:
        - First we are generating and storing what we call a `root_task` this
          is the first task of a sequence of X tasks, the X is determined by
          the frequency the task needs to run.
        - Then we are buliding a chain of task, so for a frequency=3 we should
          have the following chain: task_0 > task_1 > task_2.
        - We then set the `root_tasks` as the starting_jobs for this manager
          remember that the `root_tasks` is the 1st task in a chain
        """
        import pdb

        pdb.set_trace()
        config = self.load_config("examples/config.yml")
        config = self.process_config(config)
        root_task_ids = []
        for spider_config in config:
            task_id = f'{spider_config["spider"]}_0'
            root_task = self.create_spider_task(task_id, spider_config)
            root_task_ids.append(task_id)
            self.build_chain(root_task, spider_config)
            yield root_task
        self.args.starting_job = root_task_ids

    def build_chain(self, root_task, spider_config):
        chain_node = root_task
        for i in range(1, spider_config.get("frequency", 1)):
            task_id = f'{spider_config["spider"]}_{i}'
            next_task = self.create_spider_task(task_id, spider_config)
            chain_node.add_next_task(next_task)
            chain_node = next_task

    def load_config(self, filename):
        try:
            stream = open(filename)
            return list(yaml.load_all(stream, Loader=yaml.SafeLoader))
        except Exception:
            logger.exception("Could not parse config YAML file.")
            return

    def create_spider_task(self, task_id, spider_config):
        return SpiderTask(
            task_id=task_id,
            spider=spider_config["spider"],
            input_file=spider_config["input_file"],
            job_settings={"CLOSESPIDER_TIMEOUT": 60},
        )

    def process_config(self, config: dict) -> dict:
        """Receives a config dictionary and "expand" it, generating a single
        entry for each spider scheduled, handle the following special cases:
        - when `spider` contains more than one spider comma separated
        - when `spider=*` generates an entry for every available spider
        """
        new_config = []
        for c in config:
            spiders = list(map(str.strip, c["spider"].split(",")))
            if len(spiders) > 1:
                for s in spiders:
                    copy = c.copy()
                    copy["spider"] = s
                    new_config.append(copy)
            elif c["spider"].upper() == "ALL":
                exclude = list(map(str.strip, c.pop("exclude", "").split(",")))
                spiders = filter(
                    lambda s: s["id"] not in exclude, self.get_project().spiders.list()
                )
                for spider in spiders:
                    copy = c.copy()
                    copy["spider"] = spider["id"]
                    new_config.append(copy)
            else:
                new_config.append(c)
        return new_config


if __name__ == "__main__":
    TestManager().run()
