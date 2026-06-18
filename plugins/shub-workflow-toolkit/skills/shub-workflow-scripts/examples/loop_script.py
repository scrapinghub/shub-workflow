"""
A BaseLoopScript: repeats work on an interval / runs continuously until told to stop.

You implement workflow_loop() (one cycle); run() is provided and drives the loop. Use for
schedulers, consumers, long-running managers.
"""
import logging

from shub_workflow.script import BaseLoopScript

LOG = logging.getLogger(__name__)


class MyLoop(BaseLoopScript):

    loop_mode = 60        # default seconds between cycles (0 = run body once). --loop-mode overrides.
    max_running_time = 0  # if >0, auto-stop after this many seconds (also --max-running-time)
    stats_interval = 120  # seconds between periodic stats uploads during the loop

    @property
    def description(self) -> str:
        return "Keep example_spider topped up to N running jobs."

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("--target-running", type=int, default=3)

    def on_start(self):
        # one-time setup before the loop begins
        self.spider = "example_spider"

    def workflow_loop(self) -> bool:
        running = sum(1 for _ in self.get_jobs(spider=self.spider, state=["running", "pending"]))
        for _ in range(max(0, self.args.target_running - running)):
            self.schedule_spider(self.spider)
        self.stats.set_value("running", running)
        return True  # True = keep looping; return False (or set self.workflow_loop_enabled=False) to stop

    def on_close(self):
        # one-time teardown when the loop ends
        LOG.info("Closing with reason %s", self.get_close_reason())


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    script = MyLoop()
    script.run()
