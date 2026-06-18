"""
The most common crawl manager: a GeneratorCrawlManager.

You implement set_parameters_gen() — a generator yielding one dict per spider job to schedule. The
manager schedules them respecting max_running_jobs, and finishes when the generator is exhausted and
all jobs are done.
"""
import logging

from shub_workflow.crawl import GeneratorCrawlManager

LOG = logging.getLogger(__name__)


class CrawlManager(GeneratorCrawlManager):

    name = "crawl"              # REQUIRED (WorkFlowManager). Set here, or pass as first positional arg.
    loop_mode = 60              # seconds between cycles; REQUIRED for a generator manager (0 = run once)
    default_max_jobs = 4        # at most N spider jobs running at once (overrides via --max-running-jobs)
    spider = "example_spider"   # default target; a yielded "spider" key overrides it per job

    @property
    def description(self):
        return "Schedule example_spider for each input."

    def set_parameters_gen(self):
        # Each yielded dict is the spider arguments for one job. Recognized Scrapy Cloud keys are
        # pulled out and handled specially: spider, units, tags, job_settings, project_id.
        # Everything else becomes the job's spider args.
        for page in range(10):
            yield {"page": page}                       # -> spider arg page=<n>
        # yield {"spider": "other_spider", "page": 1, "units": 2}   # per-job overrides also work


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    CrawlManager().run()
