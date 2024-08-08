import re
import time
import logging
import inspect
from typing import Dict, Type, Tuple, Any, Callable
from datetime import timedelta, datetime
from collections import defaultdict

import dateparser
from scrapy import Spider

from shub_workflow.script import BaseScript, SpiderName, JobDict

LOG = logging.getLogger(__name__)


class BaseMonitor(BaseScript):

    # a map from spiders classes to check, to a stats prefix to identify the aggregated stats.
    target_spider_classes: Dict[Type[Spider], str] = {Spider: ""}
    # stats aggregated from spiders
    target_spider_stats: Tuple[str, ...] = ()

    # - a map from script name into a tuple of 2-elem tuples (aggregating stat regex, aggregated stat prefix)
    # - the aggregating stat regex is used to match stat on target script
    # - the aggregated stat prefix is used to generate the monitor stat. The original stat name is appended to
    #   the prefix.
    target_script_stats: Dict[str, Tuple[Tuple[str, str], ...]] = {}

    # - a map from script name into a tuple of 2-elem tuples (aggregating log regex, aggregated stat name)
    # - the aggregating log regex must match log lines in the target script job, with a first group to extract a number
    #   from it. If not a group number is extracted, the match alone aggregates 1.
    # - the aggregated stat name is the name where to aggregate the number extracted by the regex, plus a second group,
    #   if exists.
    target_script_logs: Dict[str, Tuple[Tuple[str, str], ...]] = {}

    # A dict from a stat name to a callable that receives only the value of that stat.
    # Useful for adding monitor alerts or any kind of reaction according to stat value.
    stats_hooks: Dict[str, Callable[[Any], None]] = {}

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "--period", "-p", type=int, default=86400, help="Target period in seconds. Default: %(default)s"
        )
        self.argparser.add_argument(
            "--end-time",
            "-e",
            type=str,
            help="""End side of the target period. By default it is just now. Format: any string
that can be recognized by dateparser.""",
        )
        self.argparser.add_argument(
            "--start-time",
            "-s",
            type=str,
            help="""
            Starting side of the target period. By default it is the end side minus the period. Format:
            any string that can be recognized by dateparser.
        """,
        )

    def stats_postprocessing(self):
        """
        Apply here any additional code for post processing stats, generate derivated stats, etc.
        """

    def run(self):
        end_limit = time.time()
        if self.args.end_time is not None and (dt := dateparser.parse(self.args.end_time)) is not None:
            end_limit = dt.timestamp()
        start_limit = end_limit - self.args.period
        if self.args.start_time and (dt := dateparser.parse(self.args.start_time)) is not None:
            start_limit = dt.timestamp()
        else:
            LOG.info(f"Period set: {timedelta(seconds=self.args.period)}")
        LOG.info(f"Start time: {str(datetime.fromtimestamp(start_limit))}")
        LOG.info(f"End time: {str(datetime.fromtimestamp(end_limit))}")

        for attr in dir(self):
            if attr.startswith("check_"):
                method = getattr(self, attr)
                if inspect.ismethod(method):
                    check_name = attr.replace("check_", "")
                    LOG.info(f"Checking {check_name}...")
                    method(start_limit, end_limit)

        self.stats_postprocessing()

        self.run_stats_hooks()
        self.upload_stats()
        self.print_stats()

    def run_stats_hooks(self):
        for stat, hook in self.stats_hooks.items():
            hook(self.stats.get_value(stat))

    def _get_stats_prefix_from_spider_class(self, spiderclass: Type[Spider]) -> str:
        for cls, prefix in self.target_spider_classes.items():
            if issubclass(spiderclass, cls):
                return prefix
        return ""

    def spider_job_hook(self, jobdict: JobDict):
        """
        This is called for every spider job retrieved, from the spiders declared on target_spider_stats,
        so additional per job customization can be added.
        """

    def check_spiders(self, start_limit, end_limit):

        spiders: Dict[SpiderName, Type[SpiderName]] = {}
        for s in self.spider_loader.list():
            subclass = self.spider_loader.load(s)
            if issubclass(subclass, tuple(self.target_spider_classes.keys())):
                spiders[SpiderName(s)] = subclass

        items: Dict[SpiderName, int] = defaultdict(int)
        for jobcount, jobdict in enumerate(
            self.get_jobs(
                state=["finished"],
                meta=["spider", "finished_time", "scrapystats", "spider_args"],
                has_tag=[f"FLOW_ID={self.flow_id}"] if self.flow_id is not None else None,
                endts=int(end_limit * 1000),
            ),
            start=1,
        ):
            if jobdict["finished_time"] / 1000 < start_limit:
                break
            if jobdict["finished_time"] / 1000 > end_limit:
                continue
            self.spider_job_hook(jobdict)
            if jobdict["spider"] in spiders:
                canonical = self.get_canonical_spidername(jobdict["spider"])
                stats_added_prefix = self._get_stats_prefix_from_spider_class(spiders[jobdict["spider"]])
                if "scrapystats" in jobdict:
                    items[jobdict["spider"]] += jobdict["scrapystats"].get("item_scraped_count", 0)
                    self.stats.inc_value(f"{stats_added_prefix}/jobs/{canonical}".strip("/"))
                    for statkey in jobdict["scrapystats"]:
                        if statkey.startswith("downloader/response_count"):
                            self.stats.inc_value(
                                f"{stats_added_prefix}/{statkey}/{canonical}".strip("/"),
                                jobdict["scrapystats"][statkey],
                            )
                        else:
                            for statnameprefix in self.target_spider_stats:
                                if statkey.startswith(statnameprefix):
                                    value = jobdict["scrapystats"][statkey]
                                    if stats_added_prefix != canonical:
                                        self.stats.inc_value(
                                            f"{stats_added_prefix}/{statkey}/{canonical}".strip("/"), value
                                        )
                                        self.stats.inc_value(f"{stats_added_prefix}/{statkey}/total".strip("/"), value)
                                    else:
                                        self.stats.inc_value(f"{stats_added_prefix}/{statkey}".strip("/"), value)
                else:
                    LOG.error(f"Job {jobdict['key']} does not have scrapystats.")
            if jobcount % 1000 == 0:
                LOG.info(f"Read {jobcount} jobs")

        total_crawled: Dict[str, int] = defaultdict(int)
        for spidername, itemcount in items.items():
            stats_added_prefix = self._get_stats_prefix_from_spider_class(spiders[spidername])
            total_crawled[stats_added_prefix] += itemcount
            self.stats.inc_value(f"{stats_added_prefix}/scraped_items/total".strip("/"), itemcount)

        for spidername, itemcount in items.items():
            canonical = self.get_canonical_spidername(spidername)
            stats_added_prefix = self._get_stats_prefix_from_spider_class(spiders[spidername])
            self.stats.set_value(f"{stats_added_prefix}/scraped_items/{canonical}".strip("/"), itemcount)
            if total_crawled[stats_added_prefix] > 0:
                self.stats.set_value(
                    f"{stats_added_prefix}/scraped_items_ratio/{canonical}".strip("/"),
                    round(100 * itemcount / total_crawled[stats_added_prefix], 2),
                )

        return total_crawled

    def script_job_hook(self, jobdict: JobDict):
        """
        This is called for every script job retrieved, from the scripts declared on target_script_stats,
        so additional per job customization can be added
        """

    def check_scripts_stats(self, start_limit, end_limit):
        for script, regexes in self.target_script_stats.items():
            plural = script.replace("py:", "").replace(".py", "") + "s"
            LOG.info(f"Checking {plural} stats ...")
            for jobdict in self.get_jobs(
                spider=script,
                meta=["finished_time", "scrapystats"],
                endts=int(end_limit * 1000),
                has_tag=[f"FLOW_ID={self.flow_id}"] if self.flow_id is not None else None,
            ):
                if jobdict["finished_time"] / 1000 < start_limit:
                    break
                if jobdict["finished_time"] / 1000 > end_limit:
                    continue
                self.script_job_hook(jobdict)
                for key, val in jobdict.get("scrapystats", {}).items():
                    for regex, prefix in regexes:
                        if (m := re.search(regex, key)) is not None:
                            aggregated_stat_name = m.groups()[0] if m.groups() else m.group()
                            self.stats.inc_value(f"{prefix}/{aggregated_stat_name}".strip("/"), val)
                            if prefix:
                                self.stats.inc_value(f"{prefix}/total".strip("/"), val)

    def check_script_logs(self, start_limit, end_limit):
        for script, regexes in self.target_script_logs.items():
            plural = script.replace("py:", "").replace(".py", "") + "s"
            LOG.info(f"Checking {plural} logs ...")
            for jobdict in self.get_jobs(
                spider=script,
                state=["running", "finished"],
                meta=["state", "finished_time"],
                has_tag=[f"FLOW_ID={self.flow_id}"] if self.flow_id is not None else None,
            ):
                if jobdict["state"] == "running" or jobdict["finished_time"] / 1000 > start_limit:
                    job = self.get_job(jobdict["key"])
                    for logline in job.logs.iter():
                        if logline["time"] / 1000 < start_limit:
                            continue
                        if logline["time"] / 1000 > end_limit:
                            break
                        for regex, stat in regexes:
                            if (m := re.search(regex, logline["message"])) is not None:
                                if gr := m.groups():
                                    try:
                                        val = int(gr[0])
                                        stat_suffix = gr[1] if len(gr) > 0 else ""
                                    except ValueError:
                                        val = int(gr[1])
                                        stat_suffix = gr[0]
                                    self.stats.inc_value(stat, val)
                                    if stat_suffix:
                                        self.stats.inc_value(stat + f"/{stat_suffix}", val)
                                else:
                                    self.stats.inc_value(stat)
