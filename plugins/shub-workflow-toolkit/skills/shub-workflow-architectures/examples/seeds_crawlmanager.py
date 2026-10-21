"""
Crawl manager feeding one spider with seeds read from its frontier slot. One instance per spider:

    py:crawlmanager.py <spider>

started (and restarted) by the keep-alive scheduler. When the slot is empty the generator ends, the manager waits
for its jobs and closes; the scheduler starts it again later.
"""
import asyncio
import json
import logging
from typing import Dict, Optional

from hcf_backend.manager import HCFManager
from shub_workflow.crawl import AsyncSchedulerCrawlManagerMixin, GeneratorCrawlManager

from myproject.common import get_slotname

LOGGER = logging.getLogger(__name__)

# One entry per enabled spider, with all its parameters. RUNNING_JOBS None = read the ScrapyCloud project setting
# <SPIDER>_RUNNING_JOBS on every loop (changeable without deploy; 0 pauses the source).
ENABLED_SPIDERS: Dict[str, Dict[str, Optional[int]]] = {
    "mysite.com_tags": {"SEEDS_PER_JOB": 10, "RUNNING_JOBS": None, "UNITS": 1},
    "mysite.com_authors": {"SEEDS_PER_JOB": 100, "RUNNING_JOBS": 4, "UNITS": 1},
}

FATAL_OUTCOMES = ("account_suspended",)  # TODO: custom close reasons of the spiders that must stop the crawl


class CrawlManager(AsyncSchedulerCrawlManagerMixin, GeneratorCrawlManager):

    name = "crawlmanager"
    loop_mode = 60
    # scheduled automatically: adopt the jobs left running by the previous instance
    acquire_all_jobs = True
    dont_acquire_finished_jobs = True
    # retry failed jobs with the same arguments, seeds included (0 = seeds of failed jobs are lost)
    MAX_RETRIES = 1

    def __init__(self):
        super().__init__()
        assert self.spider in ENABLED_SPIDERS, f"{self.spider} is not enabled"
        self.config = ENABLED_SPIDERS[self.spider]
        self.hcf = HCFManager(self.project_settings["FRONTIER_NAME"], project_id=self.project_id)

    def get_owned_jobs(self, project_id=None, **kwargs):
        # own every job of the target spider, whoever scheduled it, and only of that spider (all the instances
        # share the same name, so name-based acquisition alone would adopt the jobs of the other instances)
        yield from self.get_jobs(project_id, spider=self.spider, **kwargs)

    @property
    def max_running_jobs(self) -> int:
        running_jobs = self.config["RUNNING_JOBS"]
        if running_jobs is None:
            settings = self.get_sc_project_settings()
            running_jobs = int(settings.get(f"{self.spider.upper()}_RUNNING_JOBS", 1))
        # TODO (back-pressure): return 0 while the downstream queue of this source is too long
        return running_jobs

    def set_parameters_gen(self):
        spidercls = self.spider_loader.load(self.spider)
        slot = get_slotname(self.get_canonical_spidername(self.spider), spidercls.seeds_source)
        seeds_per_job = self.config["SEEDS_PER_JOB"] or 1
        while True:
            seeds = []
            for batch in self.hcf.read(slot, seeds_per_job * max(self.max_running_jobs, 1)):
                seeds.extend(fp for fp, _ in batch["requests"])
                self.hcf.delete(slot, [batch["id"]])  # delete on read: see MAX_RETRIES
            if not seeds:
                LOGGER.info(f"No more seeds in slot {slot}.")
                # TODO (recrawl): if the downstream needs more data, delete the slot and schedule a seed job
                break
            for i in range(0, len(seeds), seeds_per_job):
                yield {"seeds": json.dumps(seeds[i: i + seeds_per_job]), "units": self.config["UNITS"]}

    def bad_outcome_hook(self, spider, outcome, job_args_override, jobkey):
        if outcome in FATAL_OUTCOMES:
            LOGGER.error(f"Job {jobkey} finished with {outcome}. Stopping the crawl.")
            self.workflow_loop_enabled = False
        else:
            super().bad_outcome_hook(spider, outcome, job_args_override, jobkey)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    asyncio.run(CrawlManager().run())
