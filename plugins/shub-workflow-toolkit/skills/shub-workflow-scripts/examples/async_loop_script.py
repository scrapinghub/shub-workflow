"""
An asyncio-based loop script: mix BaseLoopScriptAsyncMixin IN FRONT OF BaseLoopScript and make
workflow_loop a coroutine. Use when a cycle schedules/awaits many things concurrently.

Note the entry point: run() is now a coroutine, so launch it with asyncio.run(...).
"""
import asyncio
import logging

from shub_workflow.script import BaseLoopScript, BaseLoopScriptAsyncMixin

LOG = logging.getLogger(__name__)


class MyAsyncLoop(BaseLoopScriptAsyncMixin, BaseLoopScript):  # mixin first, BaseLoopScript last

    loop_mode = 30

    @property
    def description(self) -> str:
        return "Schedule a batch of jobs concurrently each cycle."

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("--batch", type=int, default=5)

    async def workflow_loop(self) -> bool:
        # async scheduling helper provided by the mixin
        jobkeys = await asyncio.gather(
            *(self.async_schedule_spider("example_spider") for _ in range(self.args.batch))
        )
        LOG.info("Scheduled %s jobs", len([k for k in jobkeys if k]))
        # synchronous BaseScript helpers (get_jobs, stats, tags, ...) are still usable here
        self.stats.inc_value("cycles")
        return True


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    script = MyAsyncLoop()
    asyncio.run(script.run())  # async run() => asyncio.run
