"""
A typical monitor: declarative cross-job aggregation + ratios + threshold alerts.

BaseMonitor scans the spider/script jobs in a time window, aggregates stats (and log-derived stats),
computes ratios, and raises alerts via a stat hook. Mix an alert backend (SlackMixin / SentryMixin)
IN FRONT of BaseMonitor so queued messages are actually delivered. It runs once and exits — schedule
it periodically.
"""
import logging

from shub_workflow.utils.monitor import BaseMonitor
from shub_workflow.contrib.slack import SlackMixin

# import the spider base class(es) you want to monitor
from myproject.spiders import MyBaseSpider

LOG = logging.getLogger(__name__)

# per-spider thresholds with a default (a common pattern)
EXCEPTION_RATE_THRESHOLDS = {"default": 0.01, "flaky_source": 0.05}


class Monitor(SlackMixin, BaseMonitor):          # alert mixin FIRST, BaseMonitor LAST

    default_subject = "MyProject monitor"

    # which spiders to scan, and the stats prefix for each group
    target_spider_classes = {MyBaseSpider: "discovery"}

    # extra stat-key regex prefixes to aggregate (on top of the always-on defaults)
    target_spider_stats = ("dropped_items/",)

    # aggregate a script's stats: matches -> "<prefix>/<group>" (+ "<prefix>/total")
    target_script_stats = {"py:deliver.py": ((r"delivered_count/(.+)", "delivered"),)}

    # aggregate numbers parsed from a long-running script's LOG lines
    target_script_logs = {"py:consumer.py": ((r"new urls for (.+): (\d+)", "new_urls"),)}

    # ratio stats: (numerator_regex, denominator_regex, target_stat)
    stats_ratios = (
        ("discovery/spider_exceptions/.+/(.+)", "discovery/item_scraped_count/(.+)", "discovery/exceptions_rate"),
    )

    # (stat_regex, method_name); the method gets (start, end, value, *regex_groups)
    stats_hooks = (
        (r"^discovery/item_scraped_count/total$", "no_items_hook"),
        (r"^discovery/exceptions_rate/(.+)$", "exceptions_hook"),
    )

    def no_items_hook(self, start_limit, end_limit, value, *groups):
        if value == 0:
            self.append_message("ALERT: zero items scraped in the window.")

    def exceptions_hook(self, start_limit, end_limit, value, source):
        threshold = EXCEPTION_RATE_THRESHOLDS.get(source, EXCEPTION_RATE_THRESHOLDS["default"])
        if value > threshold:
            self.append_message(f"ALERT: {source} exception rate {value:.3f} > {threshold}")


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Monitor().run()
