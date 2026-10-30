"""
Crawl manager feeding one spider with seeds read from its frontier slot, built on shub-workflow's ready-made
HCFSeedsCrawlManager (it reads and deletes the seed batches, schedules one job per chunk of seeds, owns the jobs of
its target spider, and ends when the slot is empty). One instance per spider:

    py:crawlmanager.py <spider>

started (and restarted) by the keep-alive scheduler. When the slot is empty the generator ends, the manager waits
for its jobs and closes; the scheduler starts it again later.
"""
import asyncio
import logging
from typing import Dict

from shub_workflow.crawl import AsyncSchedulerCrawlManagerMixin
from shub_workflow.script import SpiderName
from shub_workflow.utils.hcf import HCFSeedsCrawlManager, SeedsSpiderConfig

LOGGER = logging.getLogger(__name__)

# One entry per enabled spider, with all its parameters. RUNNING_JOBS None (or absent) = read the ScrapyCloud project
# setting <SPIDER>_RUNNING_JOBS on every loop (changeable without deploy; 0 pauses the source). A module constant, as
# the keep-alive scheduler imports it too.
ENABLED_SPIDERS: Dict[SpiderName, SeedsSpiderConfig] = {
    SpiderName("mysite.com_tags"): {"SEEDS_PER_JOB": 10, "RUNNING_JOBS": None, "SC_UNITS": 1},
    SpiderName("mysite.com_authors"): {"SEEDS_PER_JOB": 100, "RUNNING_JOBS": 4, "SC_UNITS": 1},
}


class CrawlManager(AsyncSchedulerCrawlManagerMixin, HCFSeedsCrawlManager):  # type: ignore[misc]  # async mixin FIRST

    name = "crawlmanager"
    loop_mode = 60
    ENABLED_SPIDERS = ENABLED_SPIDERS
    # The slot read is <canonical name>_<seeds_source>: the consumer must write with the same naming
    # (myproject.common.get_slotname). The frontier is the FRONTIER_NAME project setting.

    # TODO: custom close reasons of the spiders that must stop the crawl
    fatal_outcomes = ("account_suspended",)
    # retry failed jobs with the same arguments, seeds included (0 = seeds of failed jobs are lost)
    MAX_RETRIES = 1

    @property
    def max_running_jobs(self) -> int:
        # TODO (back-pressure): return 0 while the downstream queue of this source is too long
        return super().max_running_jobs

    def on_empty_frontier(self):
        super().on_empty_frontier()
        # TODO (recrawl): if the downstream needs more data, delete the slot and schedule a seed job


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    asyncio.run(CrawlManager().run())
