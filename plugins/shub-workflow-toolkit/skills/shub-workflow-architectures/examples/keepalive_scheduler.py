"""
Keep-alive scheduler: run it as a periodic job (e.g. hourly). It holds no state and knows nothing about the work: it
starts every continuous component that should be running and isn't. Any component can therefore be restarted (e.g. to
deploy a new version) by just stopping it. Run-to-completion components (deliveries, monitors) are periodic jobs.
"""
import logging
import time
from datetime import datetime

from shub_workflow.issuer import hash_mod
from shub_workflow.script import BaseScript

from myproject.common import NUM_SLOTS
from myproject.scripts.crawlmanager import ENABLED_SPIDERS

LOGGER = logging.getLogger(__name__)

# spiders that sweep a whole source periodically, outside the frontier
SWEEP_SPIDERS = ("othersite.com_sweep",)
SWEEP_MIN_AGE_DAYS = 75  # a bit shorter than the period (a quarter), so a late run doesn't shift the next ones


class Scheduler(BaseScript):

    @property
    def description(self):
        return "Keep the workflow components alive."

    def running_commands(self, script: str):
        jobs = self.get_jobs(spider=script, state=["pending", "running"], meta=["job_cmd"])
        return {tuple(jdict["job_cmd"][1:]) for jdict in jobs}

    def keep_alive(self, script: str, args_list, units: int = 1):
        running = self.running_commands(script)
        for args in args_list:
            if tuple(args) not in running:
                LOGGER.info(f"Scheduling {script} {' '.join(args)}")
                self.schedule_script([script, *args], units=units)

    def sweep_is_due(self, spider: str) -> bool:
        if hash_mod(spider, 24) != datetime.utcnow().hour:  # spread the sweeps over the day
            return False
        for jdict in self.get_jobs(spider=spider, state=["finished"], count=1, meta=["finished_time"]):
            return time.time() - jdict["finished_time"] / 1000 > SWEEP_MIN_AGE_DAYS * 86400
        return True

    def run(self):
        self.keep_alive("py:crawlmanager.py", [[spider] for spider in ENABLED_SPIDERS])
        self.keep_alive("py:consumer.py", [["class:SeededSpider"]], units=2)
        self.keep_alive("py:deduplicator.py", [[str(slot)] for slot in range(NUM_SLOTS)], units=6)
        self.keep_alive("py:downloader_crawlmanager.py", [[]])
        for spider in SWEEP_SPIDERS:
            if self.sweep_is_due(spider):
                self.schedule_spider(spider)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Scheduler().run()
