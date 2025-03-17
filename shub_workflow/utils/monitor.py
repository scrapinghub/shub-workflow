import re
import abc
import time
import logging
import inspect
import csv
from io import StringIO
from typing import Dict, Type, Tuple, Optional, Protocol, Union
from datetime import timedelta, datetime
from collections import Counter

import dateparser
from scrapy import Spider
from prettytable import PrettyTable

from shub_workflow.script import BaseScript, BaseScriptProtocol, SpiderName, JobDict
from shub_workflow.utils.alert_sender import AlertSenderMixin
from shub_workflow.contrib.slack import SlackSender

LOG = logging.getLogger(__name__)


def _get_number(txt: str) -> Optional[int]:
    try:
        return int(txt)
    except ValueError:
        return None


class BaseMonitorProtocol(BaseScriptProtocol, Protocol):

    @abc.abstractmethod
    def close(self):
        pass


BASE_TARGET_SPIDER_STATS = (
    "downloader/response_status_count/",
    "downloader/response_count",
    "item_scraped_count",
    "spider_exceptions/",
    "scrapy-zyte-api/429",
    "zyte_api_proxy/response/status/429",
)


RESPONSE_STATUS_COUNT_RE = re.compile(r"downloader/response_status_count/(\d+)/(.+)")


class SpiderStatsAggregatorMixin(BaseScriptProtocol):
    # stats aggregated from spiders. A tuple of stats regex prefixes.
    target_spider_stats: Tuple[str, ...] = ()
    stats_only_total = True

    def aggregate_spider_stats(self, jobdict: JobDict, stats_added_prefix: str = ""):
        canonical = self.get_canonical_spidername(jobdict["spider"])
        for statkey in jobdict.get("scrapystats") or {}:
            for statnameprefix in self.target_spider_stats + BASE_TARGET_SPIDER_STATS:
                if re.match(statnameprefix, statkey) is not None:
                    value = jobdict["scrapystats"][statkey]
                    if stats_added_prefix != canonical:
                        if not self.stats_only_total:
                            self.stats.inc_value(f"{stats_added_prefix}/{statkey}/{canonical}".strip("/"), value)
                        self.stats.inc_value(f"{stats_added_prefix}/{statkey}/total".strip("/"), value)
                    else:
                        self.stats.inc_value(f"{stats_added_prefix}/{statkey}".strip("/"), value)


class BaseMonitor(AlertSenderMixin, SpiderStatsAggregatorMixin, BaseScript, BaseMonitorProtocol):

    # a map from spiders classes to check, to a stats prefix to identify the aggregated stats.
    target_spider_classes: Dict[Type[Spider], str] = {Spider: ""}

    # - a map from script name into a tuple of 2-elem tuples (aggregating stat regex, aggregated stat prefix)
    # - the aggregating stat regex is used to match stat on target script
    # - the aggregated stat prefix is used to generate the monitor stat. The original stat name is appended to
    #   the prefix.
    # - if a group is present in the regex, its value is used as suffix of the generate stat, instead of
    #   the complete original stat name.
    target_script_stats: Dict[str, Tuple[Tuple[str, str], ...]] = {}

    # - a map from script name into a tuple of 2-elem tuples (aggregating log regex, aggregated stat name)
    # - the aggregating log regex must match log lines in the target script job, with a first group to extract a number
    #   from it. If not a group number is extracted, the match alone aggregates 1.
    # - the aggregated stat name is the name where to aggregate the number extracted by the regex, plus a second group,
    #   if exists.

    # - a map from script name into a tuple of 2-elem tuples (aggregating log regex, aggregated stat name)
    # - the aggregating log regex must match log lines in the target script job, with a group to extract a number
    #   from it. If not a group number is extracted, the match alone aggregates 1.
    # - the final aggregated stat name is the specified aggregated stat name, plus a second non numeric group in the
    #   match, if exists.
    target_script_logs: Dict[str, Tuple[Tuple[str, str], ...]] = {}

    # Define here ratios computations. Each 3-tuple contains:
    # - a string or regex that matches the numerator stat. You can use a regex group here for computing multiple ratios
    # - a string or regex that matches the denominator stat. You can use a regex group for matching numerator group and
    #   have a different denominator for each one. If no regex group in denominator, there will be a single
    #   denominator.
    # - the target stat. If there are numerator groups, this will be the target stat prefix.
    # Ratios computing are performed after the stats_postprocessing() method is called. It is a stat postprocessing
    #   itself.
    stats_ratios: Tuple[Tuple[str, str, str], ...] = ()

    # A tuple of 2-elem tuples each one with a stat regex and the name of the monitor instance method that will receive:
    # - start and end limit of the window (epoch seconds)
    # - the value of the stat
    # - one extra argument per regex group, if any.
    # Useful for adding monitor alerts or any kind of reaction according to stat value.
    stats_hooks: Tuple[Tuple[str, str], ...] = ()

    # if True, only generate the totals and not per crawler stats. This has effect on default stats only, not the
    # custom ones added by the developer.
    stats_only_total = False

    # A tuple of string tuples for generating a report table.
    # The first line is the header. Following ones are the rows. Ensure that all tuples has the same length.
    report_table: Tuple[Tuple[str, ...], ...] = ()

    # additional projects in multiproject projects
    additional_projects: Tuple[int, ...] = ()

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "--period", "-p", type=int, default=86400, help="Time window period in seconds. Default: %(default)s"
        )
        self.argparser.add_argument(
            "--end-time",
            "-e",
            type=str,
            help="""End side of the time window. By default it is just now. Format: any string
that can be recognized by dateparser.""",
        )
        self.argparser.add_argument(
            "--start-time",
            "-s",
            type=str,
            help="""
            Starting side of the time window. By default it is the end side minus the period. Format:
            any string that can be recognized by dateparser.
        """,
        )
        self.argparser.add_argument(
            "--generate-report",
            action="store_true",
            help=("Generate report table. See report_table attribute and generate_report() method."),
        )
        self.argparser.add_argument(
            "--report-format",
            choices=("csv", "pretty", "pretty_with_borders", "pretty_with_tabs"),
            default="pretty",
            help="'pretty_with_tabs' is suitable for easy copy and paste over a spreadsheet.",
        )
        self.argparser.add_argument(
            "--slack-report", action="store_true", help="Send report to slack. By default it just prints it in the log."
        )

    def stats_postprocessing(self, start_limit, end_limit):
        """
        Apply here any additional code for post processing stats, generate derivated stats, reports, etc.
        """

    def generate_report(self):
        """
        Facilitates the generation of printed/slack reports.
        """
        header: Tuple[str, ...] = self.report_table[0]
        rows: Tuple[Tuple[str, ...], ...] = self.report_table[1:]

        table = PrettyTable(
            field_names=header,
            border=self.args.report_format == "pretty_with_borders",
            preserve_internal_border=self.args.report_format == "pretty_with_tabs",
        )
        for row in rows:
            table.add_row(list(row))

        if self.args.report_format == "csv":
            fp = StringIO()
            w = csv.writer(fp)
            w.writerow(table.field_names)
            w.writerows(table.rows)
            fp.seek(0)
            table_text = fp.read()
            fp.close()
        elif self.args.report_format == "pretty_with_tabs":
            table_text = str(table).replace(" | ", " \t ")
        else:
            table_text = str(table)

        if self.args.slack_report:
            table_text = "```" + table_text + "```"
            sender = SlackSender(self.project_settings)
            sender.send_slack_messages(
                [table_text],
                self.args.subject or "Stats Report",
            )
            # avoid slack rate limit error when sending the rates alert to slack
            time.sleep(30)
        else:
            print(table_text)

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

        self.stats_postprocessing(start_limit, end_limit)
        self.run_stats_ratios()
        if self.args.generate_report:
            self.generate_report()
        self.run_stats_hooks(start_limit, end_limit)
        self.upload_stats()
        self.print_stats()
        self.close()

    def run_stats_ratios(self):
        """
        Generate new ratio stats, based on definitions in stats_ratios attribute.
        """

        stats_ratios = self.stats_ratios

        # append response status ratios
        for prefix in self.target_spider_classes.values():
            if prefix:
                prefix = prefix + "/"
            for stat, value in list(self.stats.get_stats().items()):
                if (m := RESPONSE_STATUS_COUNT_RE.search(stat)) is not None and stat.startswith(prefix):
                    status_code, spider = m.groups()
                    stats_ratios += ((
                        f"{prefix}downloader/response_status_count/{status_code}/{spider}",
                        f"{prefix}downloader/response_count/{spider}",
                        f"{prefix}downloader/response_count/rate/{status_code}/{spider}"
                    ),)

        for num_regex, den_regex, target_prefix in stats_ratios:
            numerators: Dict[str, int] = Counter()
            denominators: Dict[str, int] = Counter()
            numerator_has_groups = False
            denominator_has_groups = False
            for stat, value in list(self.stats.get_stats().items()):
                if m := re.search(num_regex, stat):
                    if m.groups():
                        source = m.groups()[0]
                        numerators[source] += value
                        numerator_has_groups = True
                    else:
                        numerators[stat] += value
                if m := re.search(den_regex, stat):
                    if m.groups():
                        source = m.groups()[0]
                        denominators[source] += value
                        denominator_has_groups = True
                    else:
                        denominators[stat] += value
            for source, numer in numerators.items():
                denominator = 0
                if denominator_has_groups:
                    denominator = denominators.pop(source, 0)
                elif denominators:
                    denominator = list(denominators.values())[0]
                    denominators = {}
                if denominator > 0:
                    target_stat = target_prefix
                    if numerator_has_groups:
                        target_stat += "/" + source
                    self.stats.set_value(target_stat, round(numer / denominator, 4))
            for source, denom in denominators.items():
                target_stat = target_prefix
                if numerator_has_groups:
                    target_stat += "/" + source
                self.stats.set_value(target_stat, round(0, 4))

    def run_stats_hooks(self, start_limit, end_limit):
        for stat, val in self.stats.get_stats().items():
            for stat_re, hook_name in self.stats_hooks:
                if (m := re.search(stat_re, stat)) is not None:
                    hook = getattr(self, hook_name)
                    hook(start_limit, end_limit, self.stats.get_value(stat), *m.groups())

    def _get_stats_prefix_from_spider_class(self, spiderclass: Type[Spider]) -> str:
        for cls, prefix in self.target_spider_classes.items():
            if issubclass(spiderclass, cls):
                return prefix
        return ""

    def spider_job_hook(self, jobdict: JobDict):
        """
        This is called for every spider job retrieved, from the spiders declared on target_spider_classes,
        so additional per job customization can be added.
        """

    def get_jobs_in_window(self, start_limit: int, end_limit: Union[int, float, None], **kwargs):
        end_limit = end_limit or float("inf")
        for project_id in (self.project_id,) + self.additional_projects:
            for jobdict in self.get_jobs(project_id=project_id, **kwargs):
                if "finished_time" in jobdict and jobdict["finished_time"] / 1000 < start_limit:
                    break
                if "finished_time" in jobdict and jobdict["finished_time"] / 1000 > end_limit:
                    continue
                yield jobdict

    def check_spiders(self, start_limit, end_limit):

        spiders: Dict[SpiderName, Type[Spider]] = {}
        for s in self.spider_loader.list():
            subclass = self.spider_loader.load(s)
            if issubclass(subclass, tuple(self.target_spider_classes.keys())):
                spiders[SpiderName(s)] = subclass

        for jobcount, jobdict in enumerate(
            self.get_jobs_in_window(
                start_limit,
                end_limit,
                state=["finished"],
                meta=["spider", "finished_time", "scrapystats", "spider_args", "close_reason", "tags"],
                has_tag=[f"FLOW_ID={self.flow_id}"] if self.flow_id is not None else None,
                endts=int(end_limit * 1000),
            ),
            start=1,
        ):
            if jobdict["spider"] in spiders:
                self.spider_job_hook(jobdict)
                stats_added_prefix = self._get_stats_prefix_from_spider_class(spiders[jobdict["spider"]])
                self.aggregate_spider_stats(jobdict, stats_added_prefix)
            if jobcount % 1000 == 0:
                LOG.info(f"Read {jobcount} jobs")

    def script_job_hook(self, jobdict: JobDict):
        """
        This is called for every script job retrieved, from the scripts declared on target_script_stats,
        so additional per job customization can be added
        """

    def check_scripts_stats(self, start_limit, end_limit):
        for script, regexes in self.target_script_stats.items():
            plural = script.replace("py:", "").replace(".py", "") + "s"
            LOG.info(f"Checking {plural} stats ...")
            for jobdict in self.get_jobs_in_window(
                start_limit,
                end_limit,
                spider=script,
                state=["finished"],
                meta=["finished_time", "scrapystats", "close_reason", "job_cmd", "tags"],
                endts=int(end_limit * 1000),
                has_tag=[f"FLOW_ID={self.flow_id}"] if self.flow_id is not None else None,
            ):
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
            for jobdict in self.get_jobs_in_window(
                start_limit,
                None,
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
                                stat_suffix = ""
                                if gr := m.groups():
                                    val = _get_number(gr[0])
                                    if val is None:
                                        if len(gr) > 1:
                                            val = _get_number(gr[1])
                                            stat_suffix = gr[0]
                                        if val is None:
                                            val = 1
                                    else:
                                        stat_suffix = gr[1] if len(gr) > 0 else ""
                                else:
                                    val = 1
                                self.stats.inc_value(stat, val)
                                if stat_suffix:
                                    self.stats.inc_value(stat + f"/{stat_suffix}", val)

    def close(self):
        self.send_messages()
