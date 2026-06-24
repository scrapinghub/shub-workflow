# Monitor API cheat-sheet

Full reference in the wiki:
[Appendix F: Monitor Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-F:-Monitor-Classes).
Source:
[`shub_workflow/utils/monitor.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/utils/monitor.py),
[`alert_sender.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/utils/alert_sender.py),
[`contrib/slack.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/contrib/slack.py),
[`contrib/sentry.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/contrib/sentry.py).

## What it is

`BaseMonitor` is a **`BaseScript`** (not a loop manager). `run()` computes a time window, runs every
`check_*` method, then `stats_postprocessing` → ratios → optional report → stat hooks → upload/print
stats → `close()` (sends queued alerts). It runs **once** and exits — schedule it periodically.

## Configuration attributes

| Attribute | Meaning |
| --- | --- |
| `target_spider_classes` | `{SpiderClass: stats_prefix}` — which spiders to scan + per-group prefix. Default `{Spider: ""}`. |
| `target_spider_stats` | extra stat-key regex prefixes to aggregate (added to `BASE_TARGET_SPIDER_STATS`). |
| `stats_only_total` | `True` ⇒ only `…/total`, no per-spider. Default `False`. |
| `target_script_stats` | `{script: ((regex, prefix), …)}` — aggregate a script's stats → `<prefix>/<group-or-key>` (+ `/total`). |
| `target_script_logs` | `{script: ((regex, stat), …)}` — aggregate numbers parsed from a script's log lines. |
| `stats_ratios` | `((num_regex, den_regex, target), …)` — ratio stats (grouped by first regex group). |
| `stats_hooks` | `((stat_regex, method_name), …)` — call a method per matching stat. |
| `report_table` | header row + data rows for `generate_report()`. |
| `additional_projects` | extra project ids to scan. |
| `default_subject` | default alert subject (`--subject` overrides). |

Always-on defaults (`BASE_TARGET_SPIDER_STATS`): `downloader/response_status_count/`,
`downloader/response_count`, `item_scraped_count`, `spider_exceptions/`, `scrapy-zyte-api/429`,
`zyte_api_proxy/response/status/429` — emitted as `<prefix>/<statkey>/<spider>` and `/total`.

## Methods you override

| Method | Purpose |
| --- | --- |
| `check_<name>(self, start, end)` | a custom check — **auto-discovered and run** by `run()`. |
| `spider_job_hook(jobdict)` / `script_job_hook(jobdict)` | per-job custom aggregation. |
| `stats_postprocessing(self, start_limit, end_limit)` | derive stats / build `report_table` after checks. |
| a `stats_hooks` method `(self, start_limit, end_limit, value, *regex_groups)` | react to a stat (e.g. threshold → `append_message`). |
| `aggregate_spider_stats(jobdict, prefix="")` / `generate_report()` | override to customize aggregation / report. |

## Alerts

`self.append_message(text)` queues an alert; `close()` flushes through registered senders. Register a
sender by mixing a backend **in front of** `BaseMonitor`:
- `class Monitor(SlackMixin, BaseMonitor)` — needs `SPIDERMON_SLACK_*` settings + `spidermon[slack-sdk]`.
- `class Monitor(SentryMixin, BaseMonitor)` — needs `SPIDERMON_SENTRY_*` settings + `spidermon[sentry-sdk]`.
Both honour `SPIDERMON_*_FAKE` (log instead of send). Subject = `--subject` or `default_subject`.

## CLI arguments

`--period/-p` (seconds, default `86400`), `--start-time/-s`, `--end-time/-e` (dateparser strings; `-s`
overrides `-p`), `--generate-report`, `--report-format` (`pretty`|`pretty_with_borders`|
`pretty_with_tabs`|`csv`), `--slack-report`, `--subject`; plus `BaseScript` options (`--project-id`,
`--flow-id`, `--children-tag`, `-g`/`-v`). `--flow-id` (or being scheduled by a graph manager) scopes
the checks to one workflow; otherwise all jobs in the window are considered.

## Entry point

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel
    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Monitor().run()
```
