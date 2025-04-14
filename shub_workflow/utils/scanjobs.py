"""
Utility for scan, find and prints logs, spider arguments and items  on target spiders/scripts using regex patterns.

It can generate regex pattern groups, post process them via simple post-script like language,
and save in order to generate data tables.

The filter logic is as follows:

    (spider arg pattern 1 OR spider arg pattern 2 OR ...) AND
    (log pattern 1 OR log pattern 2 OR ... OR item field pattern 1 OR item field pattern 2 OR ...)

but the script can also be used to just find jobs based on spider arguments, without need to scan logs or items.
In this case the filter logic is just:

    (spider arg pattern 1 OR spider arg pattern 2 OR ...)

In addition, you can search for log and/or item patterns with no specific job argument constraint:

    (log pattern 1 OR log pattern 2 OR ... OR item field pattern 1 OR item field pattern 2 OR ...)

The only required constraint is the target spider/script name (so it is the command line required argument)

By default, the scan period is the las 1 day. See --limit-secs option.

By default, each time a new match is found, it is printed in the console and the search pauses waiting for
pressing Enter. This mode is useful for visual inspection. This behavior can be modified via the --write
option, which is useful for generating big amount of data for further analysis or generating data tables (in
combination with regex groups and stat values). With this option, data is written into a json list file, each line
being the data extracted from a single match.

As usual in shub-workflows when you run them in your console, you need to include the --project-id in order
to set the correct target project where to find the jobs.

Examples
========

1. Searches for log pattern 'youtube.+?always_retriable_rate' in jobs for script "py:deliver.py":

       > python scanjobs.py --project-id=production py:deliver.py -l 'youtube.+?always_retriable_rate'

2. Searches for log pattern 'youtube.+?always_retriable_rate": (\\d+\\.\\d+)' in jobs for script "py:deliver.py",
   and additionally prints the data extracted from regex groups defined in the pattern.

       > python scanjobs.py --project-id=production py:deliver.py -l 'youtube.+?always_retriable_rate": (\\d+\\.\\d+)'

3. Searches for the stats 'ipType'. 'records_read', 'unable_to_get_url/retries' in jobs of the spider "downloader"
   for which the spider argument "source" matches the pattern "douyin". The data extracted will be the regex group
   in 'ipType/(.+)', plus the value of the matching stats.

       > python scanjobs.py --project-id=production downloader -a source:douyin -s 'ipType/(.+)' -s records_read \\
       -s unable_to_get_url/retries

   Lets suppose that the data extracted on each match is like:

       ('datacenter', '11558', '2500', '9059')

   the first element corresponds to the matchin group of the 'ipType/(.+)' applied on the stat name. The second one is
   the value of that stat, and the third and fourth one are the value of the stats "records_read" and
   "unable_to_get_url/retries" respectively.

4. The same as example 3, but with post processing instructions:

       > python scanjobs.py --project-id=798547 downloader -a source:douyin -s 'ipType/(.+)' -s records_read \\
       -s unable_to_get_url/retries -p "3 -1 roll pop exch div"

   "3 -1 roll pop" discards the second element.
   "exch div" divides the last number over the second-last, consume boths and appends the result.

   The final effect of the instructions "3 -1 roll pop exch div" is to discard the second element, and divide
   the last by the second last. So a data line like this one:

       ('datacenter', '11558', '2500', '9059')

   will be converted into:

       ('datacenter', 3.6236)

5. Another more complex example:

       > python scanjobs.py --project-id=798547 downloader -a source:douyin -s 'ipType/(.+)' -s unable_to_get_url \\
        -s records_read -p "4 -1 roll pop dup 4 -1 roll exch div 3 1 roll div 1 add"

   Lets suppose that it matches these stats:

       {'ipType/datacenter': 5943, 'unable_to_get_url': 278, 'unable_to_get_url/retries': 5063, 'records_read': 880}

   So, the initial data generated is:

       ('datacenter', '5943', '278', '5063', '880')

   "4 -1 roll pop" discards the second element:

       ('datacenter', '278', '5063', '880')

   "dup" duplicates the last one:

       ('datacenter', '278', '5063', '880', '880')

   "4 -1 roll" rotates the last 4 elements 1 place left:

       ('datacenter', '5063', '880', '880', '278')

   "exch div" dives the last over the second last:

      ('datacenter', '5063', '880', 0.3159090909090909)

   "3 1 roll" rotates right the three last elements:

      ('datacenter', 0.3159090909090909, '5063', '880')

   And f"div 1 add" divides 5063 over 880 and adds 1, thus yielding the final result:

      ('datacenter', 0.3159090909090909, 6.7534090909090905))

postscript instructions supported:
----------------------------------

1. operations:

add, sub, mul, div

2. stack manipulation and counting:

dup, pop, roll, exch count

3. flow manipulation:

repeat

4. conversion:

cvi

======================================================================
"""

import re
import time
import json
import argparse
import datetime
from typing import Iterator, Tuple, TypedDict, List, Iterable, Dict, Union
from itertools import chain

import dateparser
from typing_extensions import NotRequired
from scrapinghub.client.jobs import Job
from shub_workflow.script import BaseScript, JobDict


def post_process(instructions: Iterable[Union[str, int, float]]) -> List[Union[str, int, float]]:
    """
    >>> post_process(["stringA", 3, 4, "dup"])
    ['stringA', 3, 4, 4]
    >>> post_process(["stringA", 3, 4, "div"])
    ['stringA', 0.75]
    >>> post_process(["stringA", 3, 4, "pop", "pop"])
    ['stringA']
    >>> post_process(["stringA", 3, 4, "add"])
    ['stringA', 7.0]
    >>> post_process(["stringA", 3, 4, 3, 1, "roll"])
    [4, 'stringA', 3]
    >>> post_process(["stringA", 3, 4, 5, 3, 2, "roll"])
    ['stringA', 4, 5, 3]
    >>> post_process(["stringA", 3, 4, 5, 3, -2, "roll"])
    ['stringA', 5, 3, 4]
    >>> post_process(["stringA", 3, 4, 5, "exch"])
    ['stringA', 3, 5, 4]
    >>> post_process([4, 3, "sub"])
    [1.0]
    >>> post_process([4, 3, "mul"])
    [12.0]
    >>> post_process(["2025-04-08", "residential", "100", "30", "189", "3", "-1", "roll",
    ... "dup", "3", "1", "roll", "div", "3", "1", "roll", "div", "2", "1", "roll"])
    ['2025-04-08', 'residential', 0.3, 1.89]

    >>> post_process(["123", "cvi"])
    [123]

    >>> post_process(["3", "4", "5", "2", "{", "add", "}", "repeat"])
    [12.0]

    Lets suppose we have the following series: ['431', '2138', '412', '216', '829', '195']
    lets divide 3 by sum of 0 and 3, 4 by sum of 1 and 4, 5 by sum of 2 and 5:
    >>> [216 / (431 + 216), 829 / (2138 + 829), 195 / (412 + 195)]
    [0.33384853168469864, 0.2794068082237951, 0.3212520593080725]

    How to achieve same result with postprocess commands?
    >>> post_process(['431', '2138', '412', '216', '829', '195', 3, 1, "roll", 4, 1, "roll",
    ... 5, 1, "roll", "dup", 3, 1, "roll", "add", "div", "count", 1, "roll",
    ... "dup", 3, 1, "roll", "add", "div", "count", 1, "roll",
    ... "dup", 3, 1, "roll", "add", "div", "count", 1, "roll"])
    [0.33384853168469864, 0.2794068082237951, 0.3212520593080725]

    Notice the 3 times repetition of ["dup", 3, 1, "roll", "add", "div", "count", 1, "roll"]
    The above can be simplified as:
    >>> post_process(['431', '2138', '412', '216', '829', '195', 3, 1, "roll", 4, 1, "roll",
    ... 5, 1, "roll", 3, "{", "dup", 3, 1, "roll", "add", "div", "count", 1, "roll", "}", "repeat"])
    [0.33384853168469864, 0.2794068082237951, 0.3212520593080725]
    """

    stack: List[Union[str, int, float]] = []
    repeat_level = 0

    for ins in instructions:
        if ins == "repeat":
            assert stack.pop() == "}", "invalid syntax for repeat"
            repeat_list: List[Union[str, int, float]] = []
            try:
                while (e := stack.pop()) != "{":
                    repeat_list.insert(0, e)
            except IndexError:
                raise SyntaxError("Unclosed }")
            num_repeats = int(stack.pop())
            for _ in range(num_repeats):
                stack = post_process(stack + repeat_list)
            continue
        if ins == "{":
            repeat_level += 1
        elif ins == "}":
            repeat_level -= 1
        if repeat_level > 0:
            stack.append(ins)
        elif ins == "dup":
            stack.append(stack[-1])
        elif ins == "pop":
            stack.pop()
        elif ins == "add":
            stack.append(float(stack.pop()) + float(stack.pop()))
        elif ins == "mul":
            stack.append(float(stack.pop()) * float(stack.pop()))
        elif ins == "div":
            denom = float(stack.pop())
            num = float(stack.pop())
            stack.append(num / denom)
        elif ins == "roll":
            places = int(stack.pop())
            length = int(stack.pop())
            head, tail = stack[:-length], stack[-length:]
            stack = head + tail[-places:] + tail[:-places]
        elif ins == "exch":
            a = stack.pop()
            b = stack.pop()
            stack.extend([a, b])
        elif ins == "sub":
            a = float(stack.pop())
            b = float(stack.pop())
            stack.append(b - a)
        elif ins == "count":
            stack.append(len(stack))
        elif ins == "cvi":
            stack.append(int(stack.pop()))
        else:
            stack.append(ins)
    return stack


class FilterResult(TypedDict):
    tstamp: str
    message: NotRequired[str]
    groups: Tuple[Union[str, int, float], ...]
    field: NotRequired[str]
    itemno: NotRequired[int]
    stats: NotRequired[Dict[str, Union[str, int, float]]]
    value: NotRequired[str]


class Check(BaseScript):

    description = __doc__

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("spider")
        self.argparser.add_argument(
            "--log-pattern", "-l", help="Log pattern. Can be multiple.", action="append", default=[]
        )
        self.argparser.add_argument(
            "--spider-argument-pattern",
            "-a",
            help="argument pattern (in format arg:value). Can be multiple.",
            action="append",
            default=[],
        )
        self.argparser.add_argument(
            "--item-field-pattern",
            "-f",
            help="Item field pattern (in format field:value). Can be multiple.",
            action="append",
            default=[],
        )
        self.argparser.add_argument(
            "--stat-pattern",
            "-s",
            help="Stat key pattern. Can be multiple.",
            action="append",
            default=[],
        )
        self.argparser.add_argument("--max-timestamp", help="In any format that dateparser can recognize.")
        self.argparser.add_argument(
            "--limit-secs", type=int, default=86400, help="dont't go further than given seconds in past"
        )
        self.argparser.add_argument(
            "--first-match-only",
            help="Print only first match and continue with next job.",
            action="store_true",
        )
        self.argparser.add_argument(
            "--max-items-per-job", type=int, help="Don't scan more than the given number of items or logs per job."
        )
        self.argparser.add_argument(
            "--print-progress-each",
            type=int,
            default=100,
            help="Print scan progress each given number of jobs. Default: %(default)s",
        )
        self.argparser.add_argument(
            "--write",
            "-w",
            type=argparse.FileType("w"),
            help="If given, write the captured patterns into the provided json list file, along with dates.",
        )
        self.argparser.add_argument("--post-process", "-p", help="postscript like instructions to process groups.")

    def parse_args(self):
        args = super().parse_args()
        if not any([args.log_pattern, args.spider_argument_pattern, args.item_field_pattern, args.stat_pattern]):
            self.argparser.error("You must provide at least one pattern. (use either -l, -a , -f or -s)")
        return args

    def filter_log_pattern(self, jdict: JobDict, job: Job, tstamp: datetime.datetime) -> Iterator[FilterResult]:
        if not self.args.log_pattern:
            return
        has_match = False
        for idx, logline in enumerate(job.logs.iter()):
            if self.args.max_items_per_job and idx == self.args.max_items_per_job:
                break

            msg = logline["message"]
            for pattern in self.args.log_pattern:
                if (m := re.search(pattern, msg, flags=re.S)) is not None:
                    yield {"tstamp": str(tstamp), "message": msg, "groups": m.groups()}
                    has_match = True

                    if self.args.first_match_only and has_match:
                        break

    def filter_item_field_pattern(self, jdict: JobDict, job: Job, tstamp: datetime.datetime) -> Iterator[FilterResult]:
        if not self.args.item_field_pattern:
            return
        has_match = False
        for idx, item in enumerate(job.items.iter()):
            if self.args.max_items_per_job and idx == self.args.max_items_per_job:
                break

            for item_field_pattern in self.args.item_field_pattern:
                key, pattern = item_field_pattern.split(":", 1)
                value = item.get(key, "")
                if (m := re.search(pattern, value)) is not None:
                    yield {"tstamp": str(tstamp), "itemno": idx, "field": key, "value": value, "groups": m.groups()}
                    has_match = True

            if self.args.first_match_only and has_match:
                break

    def filter_stats_pattern(self, jdict: JobDict, job: Job, tstamp: datetime.datetime) -> Iterator[FilterResult]:
        if not self.args.stat_pattern:
            return
        groups: List[str] = []
        stats: Dict[str, Union[str, int, float]] = {}
        for stat_pattern in self.args.stat_pattern:
            for key, val in jdict["scrapystats"].items():
                if m := re.search(stat_pattern, key):
                    groups.extend(m.groups() + (str(val),))
                    stats[key] = val
        if groups:
            yield {
                "tstamp": str(tstamp),
                "stats": stats,
                "value": val,
                "groups": tuple(groups),
            }

    def filter_spider_argument(self, jdict: JobDict, tstamp: datetime.datetime, jobcount: int) -> bool:
        for spider_arg_pattern in self.args.spider_argument_pattern:
            key, pattern = spider_arg_pattern.split(":", 1)
            if re.search(pattern, jdict.get("spider_args", {}).get(key, "")):
                print(f"Jobs scanned: {jobcount}")
                print(f"Timestamp reached: {tstamp}")
                print(f"https://app.zyte.com/p/{jdict['key']}/stats")
                print(jdict["spider_args"])
                return True
        return False

    def run(self):

        end_limit = time.time()
        if self.args.max_timestamp is not None and (dt := dateparser.parse(self.args.max_timestamp)) is not None:
            end_limit = dt.timestamp()

        limit = (end_limit - self.args.limit_secs) * 1000
        jobcount = 0
        for jdict in self.get_jobs(
            spider=self.args.spider, meta=["spider_args", "finished_time", "scrapystats"], state=["finished"]
        ):
            if "finished_time" in jdict and jdict["finished_time"] / 1000 > end_limit:
                continue

            jobcount += 1
            keyprinted = False
            job = self.get_job(jdict["key"])
            tstamp = datetime.datetime.fromtimestamp(jdict["finished_time"] / 1000)
            has_match = False

            if self.filter_spider_argument(jdict, tstamp, jobcount):
                has_match = True
                keyprinted = True
                if not self.args.write:
                    input("Press Enter to continue...\n")
            elif self.args.spider_argument_pattern:
                continue

            for result in chain(
                self.filter_log_pattern(jdict, job, tstamp),
                self.filter_item_field_pattern(jdict, job, tstamp),
                self.filter_stats_pattern(jdict, job, tstamp),
            ):
                if not keyprinted:
                    print(f"Jobs scanned: {jobcount}")
                    print(f"Timestamp reached: {result['tstamp']}")
                    print(f"https://app.zyte.com/p/{jdict['key']}/stats")
                    keyprinted = True
                if "message" in result:
                    print(result["message"])
                    has_match = True
                if "itemno" in result:
                    print(f"Item #{result['itemno']}. {result['field']}:{result['value']}")
                    has_match = True
                if "stats" in result:
                    print("Matching stats:", result["stats"])
                    has_match = True
                if result["groups"]:
                    if self.args.post_process:
                        print("Data points extracted:", result["groups"])
                        result["groups"] = tuple(post_process(result["groups"] + tuple(self.args.post_process.split())))
                    print("Data points generated:", result["groups"])
                if self.args.write:
                    if result["groups"]:
                        groups = (result["tstamp"],) + result["groups"]
                        print(json.dumps(groups), file=self.args.write)
                    else:
                        print(json.dumps(result), file=self.args.write)
                else:
                    input("Press Enter to continue...\n")

                if self.args.first_match_only and has_match:
                    break

            if jobcount % self.args.print_progress_each == 0:
                print(f"Jobs scanned: {jobcount}")
                tstamp = datetime.datetime.fromtimestamp(jdict["finished_time"] / 1000)
                print(f"Timestamp reached: {tstamp}")
            if jdict["finished_time"] < limit:
                print(f"Reached limit of {self.args.limit_secs} seconds.")
                print("Total jobs scanned:", jobcount)
                break


if __name__ == "__main__":
    Check().run()
