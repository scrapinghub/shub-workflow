"""
PeriodicCrawlManager: reschedules the SAME spider job (same args) after each one finishes, forever.

Use when you want a single spider kept continuously running. Requires loop_mode. A failed outcome
does NOT stop it (its bad_outcome_hook is a no-op); override the hooks for custom reactions.
"""
import logging

from shub_workflow.crawl import PeriodicCrawlManager

LOG = logging.getLogger(__name__)


class CrawlManager(PeriodicCrawlManager):

    name = "crawl"
    loop_mode = 180            # check the running job every 3 min; reschedule when it finishes
    spider = "example_spider"

    @property
    def description(self):
        return "Keep one example_spider job running continuously."


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    CrawlManager().run()
