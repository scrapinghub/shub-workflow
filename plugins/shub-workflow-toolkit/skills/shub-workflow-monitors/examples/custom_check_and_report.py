"""
Custom check method, stats_postprocessing, and a report table.

- Any method named check_<x>(self, start_limit, end_limit) is auto-discovered and run during run() —
  use it for things outside spider/script stats (filesystem counts, API quotas, queue depth, ...).
- stats_postprocessing(start, end) derives stats and (here) builds report_table.
- generate_report() renders report_table; run with --generate-report (and --slack-report to send it).
"""
import logging

from shub_workflow.utils.monitor import BaseMonitor

LOG = logging.getLogger(__name__)


class Monitor(BaseMonitor):

    default_subject = "MyProject report"

    def check_queue_depth(self, start_limit, end_limit):
        # custom check — auto-run by run(); aggregate anything into self.stats
        depth = self._count_pending_files()          # your own logic
        self.stats.set_value("queue/pending", depth)

    def stats_postprocessing(self, start_limit, end_limit):
        # derive stats and assemble a report table (header row, then data rows)
        pending = self.stats.get_value("queue/pending", 0)
        scraped = self.stats.get_value("item_scraped_count/total", 0)
        self.report_table = (
            ("metric", "value"),
            ("scraped items", str(scraped)),
            ("pending queue", str(pending)),
        )

    def _count_pending_files(self) -> int:
        return 0


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    # run with: python monitor.py --project-id=<id> --generate-report [--report-format=pretty_with_tabs] [--slack-report]
    Monitor().run()
