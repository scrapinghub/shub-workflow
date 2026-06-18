"""
Retries, outcome hooks, and delayed scheduling on a GeneratorCrawlManager.

Shows the four common customization points: built-in retries (MAX_RETRIES + get_retry_override),
a custom bad_outcome_hook, finished_ok_hook, and can_schedule_job_with_params for throttling.
"""
import logging

from shub_workflow.crawl import GeneratorCrawlManager, StopRetry

LOG = logging.getLogger(__name__)


class CrawlManager(GeneratorCrawlManager):

    name = "crawl"
    loop_mode = 60
    default_max_jobs = 4
    spider = "example_spider"
    MAX_RETRIES = 2            # enable built-in retries (0 = disabled). 'cancelled' is never retried.

    @property
    def description(self):
        return "Generator crawl manager with retries and hooks."

    def __init__(self):
        super().__init__()
        # failed_outcomes is a mutable copy of base_failed_outcomes — extend it with custom ones:
        self.failed_outcomes.append("custom_dead_end")

    def set_parameters_gen(self):
        for page in range(100):
            yield {"page": page, "units": 1}

    # --- retries: tweak each retry, or abort it -------------------------------------------------
    def get_retry_override(self, spider, outcome, job_args_override, jobkey):
        # Called by the default retry logic. Return extra params merged into the retried job.
        if outcome == "memusage_exceeded":
            return {"units": 6}                        # retry the OOM job with more units
        if outcome == "custom_dead_end":
            raise StopRetry("not worth retrying")      # abort retrying this job
        return {}

    # --- or take full control of failed outcomes ------------------------------------------------
    # def bad_outcome_hook(self, spider, outcome, job_args_override, jobkey):
    #     if outcome == "memusage_exceeded" and job_args_override.get("units", 1) < 6:
    #         job_args_override["units"] = 6
    #         self.add_job(spider, job_args_override)   # add_job() is GeneratorCrawlManager-only

    def finished_ok_hook(self, spider, outcome, job_args_override, jobkey):
        LOG.info("Job %s finished ok (%s)", jobkey, outcome)

    # --- throttling: defer a job to a later cycle -----------------------------------------------
    def can_schedule_job_with_params(self, params) -> bool:
        # Returning False moves the job to a delayed queue, retried next cycle. E.g. cap one
        # running job per target spider:
        return self.spider_running_count(params["spider"]) < 1


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    CrawlManager().run()
