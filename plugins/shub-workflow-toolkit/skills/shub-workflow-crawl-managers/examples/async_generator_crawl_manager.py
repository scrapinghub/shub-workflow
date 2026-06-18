"""
Async generator crawl manager: schedules each cycle's jobs concurrently.

Mix AsyncSchedulerCrawlManagerMixin IN FRONT OF GeneratorCrawlManager. set_parameters_gen() stays an
ordinary (synchronous) generator — only the scheduling becomes concurrent. Because run() is now a
coroutine, launch it with asyncio.run(...).
"""
import asyncio
import logging

from shub_workflow.crawl import GeneratorCrawlManager, AsyncSchedulerCrawlManagerMixin

LOG = logging.getLogger(__name__)


class CrawlManager(AsyncSchedulerCrawlManagerMixin, GeneratorCrawlManager):  # mixin FIRST

    name = "crawl"
    loop_mode = 30
    default_max_jobs = 20
    spider = "example_spider"

    @property
    def description(self):
        return "Schedule many example_spider jobs concurrently per cycle."

    def set_parameters_gen(self):       # NOT async
        for page in range(1000):
            yield {"page": page}


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    asyncio.run(CrawlManager().run())   # async run() => asyncio.run
