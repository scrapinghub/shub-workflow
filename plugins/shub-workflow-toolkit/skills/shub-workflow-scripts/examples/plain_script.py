"""
A one-shot BaseScript: parse args, do the work in run(), exit.

The default choice for a Scrapy Cloud script — including scripts that only *operate on* SC
(scan/query jobs, schedule spiders) and may run locally against a project.
"""
import logging

from shub_workflow.script import BaseScript

LOG = logging.getLogger(__name__)


class MyScript(BaseScript):

    # Optional class attributes (all have sane defaults):
    # name = "my_script"                 # adds a NAME= tag, propagated to scheduled children
    # project_required = False           # set False for a script that doesn't touch SC
    # default_project_id = None          # default for --project-id (None => autodetect)
    # flow_id_required = False           # True => must run within a flow id
    # children_tags = ["MYTAG"]          # extra tags added to every job this script schedules

    @property
    def description(self) -> str:
        return "Schedule example_spider and report recent finished jobs."

    def add_argparser_options(self):
        super().add_argparser_options()  # keep --project-id/--flow-id/-t/-g/-v
        self.argparser.add_argument("--units", type=int, default=1)
        self.argparser.add_argument("--spider", default="example_spider")

    def run(self):
        # schedule a spider in the target project (self.project_id, from --project-id)
        jobkey = self.schedule_spider(self.args.spider, units=self.args.units, job_settings={})
        LOG.info("Scheduled %s", jobkey)
        self.stats.inc_value("scheduled")

        # query jobs (paginated, de-duplicated generator of JobDicts)
        for jdict in self.get_jobs(spider=self.args.spider, state=["finished"], count=20):
            LOG.info("%s -> %s", jdict["key"], jdict.get("close_reason"))


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    script = MyScript()
    script.run()
