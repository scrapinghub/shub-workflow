---
name: shub-workflow-monitors
description: >-
  Use when building, updating, fixing, or understanding a shub-workflow monitor — a script that gives
  a broad cross-job view of a workflow: scanning the spider/script jobs in a time window, aggregating
  their stats (and log-derived stats), computing ratios, emitting reports, and raising threshold
  alerts (Slack / Sentry). Built on shub_workflow.utils.monitor.BaseMonitor (a BaseScript, not a loop
  manager) plus the AlertSenderMixin / SlackMixin / SentryMixin alert layer. Use for any monitor.py
  that subclasses BaseMonitor (directly or via a project base mixin).
---

# shub-workflow monitors

A **monitor** aggregates stats *across* the spider and script jobs that ran in a time window, derives
ratios, optionally emits a report, and raises alerts when thresholds are breached. `BaseMonitor`
([`shub_workflow/utils/monitor.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/utils/monitor.py))
is a **`BaseScript`** (see the `shub-workflow-scripts` skill), **not** a loop/workflow manager. Full
reference: [Appendix F: Monitor Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-F:-Monitor-Classes);
intro: the [Monitors](https://github.com/scrapinghub/shub-workflow/wiki/Monitors) tutorial.

Use a monitor for the broad view (project-wide error rates, totals, drops) where Spidermon — which
watches a single spider job — can't help.

## Core procedure

1. Subclass `BaseMonitor`. To deliver alerts, mix an alert backend **in front**:
   `class Monitor(SlackMixin, BaseMonitor)` (or `SentryMixin`).
2. Configure declaratively (templates: [examples/monitor.py](examples/monitor.py)):
   `target_spider_classes`, `target_spider_stats`, `target_script_stats`, `target_script_logs`,
   `stats_ratios`, `stats_hooks`, `default_subject`.
3. Add reactions: a `stats_hooks` method per alert condition (signature
   `(self, start_limit, end_limit, value, *regex_groups)` → `self.append_message(...)`); and/or
   override `stats_postprocessing(start, end)` for derived stats.
4. For checks outside spider/script stats, add a `check_<name>(self, start, end)` method — it's
   auto-discovered and run. For reports, set `report_table` and run with `--generate-report`
   ([examples/custom_check_and_report.py](examples/custom_check_and_report.py)).
5. Add the `__main__` boilerplate (`Monitor().run()`).
6. Register in `setup.py`; deploy via the **`scrapy-cloud-deployment`** skill. **Schedule it
   periodically** (e.g. a daily and a monthly periodic job with distinct tags, or a small
   `monitor_scheduler.py`) — it runs once per invocation.

API tables: [references/api-cheatsheet.md](references/api-cheatsheet.md).

## Critical rules & footguns

- **It's NOT a loop manager.** `BaseMonitor` is a `BaseScript`: it runs its checks once and exits.
  There is no `loop_mode` / `workflow_loop`; recurring monitoring = scheduling it periodically.
- **`check_*` methods are auto-run.** Any method whose name starts with `check_` is discovered and
  called with `(start_limit, end_limit)`. Name unrelated helpers differently, or they'll run as checks.
- **Alert mixin order + delivery.** Mix `SlackMixin`/`SentryMixin` **before** `BaseMonitor`. Without a
  backend mixin, `append_message()` just queues text that's never sent. Backends need their
  `SPIDERMON_SLACK_*` / `SPIDERMON_SENTRY_*` settings (and the matching `spidermon[...]` extra); both
  honour a `*_FAKE` setting that logs instead of sending.
- **`stats_hooks` method signature** is `(self, start_limit, end_limit, value, *regex_groups)` — one
  extra positional per capturing group in the stat regex. Mismatched arity raises at runtime.
- **`target_spider_classes` is first-match by `issubclass`, order matters.** Each spider job's prefix
  is the first entry whose class it subclasses. To give a subclass its own group, list it **before**
  its parent — listed after, the parent wins and it folds into the parent's prefix. Conversely, adding
  a base class that's already a parent of a targeted class changes nothing (it's already matched).
- **`additional_projects` covers script scans too.** It feeds `get_jobs_in_window` (`(project_id,) +
  additional_projects`), which backs the spider scan *and* `target_script_stats`/`target_script_logs`.
  So a script's regex set runs against that script's jobs in every scanned project — entries just
  don't match where a stat is absent, letting you rename a per-project stat variant to a common name.
- **`--flow-id` scoping.** With no flow id the monitor aggregates *all* jobs in the window; with
  `--flow-id` (or when scheduled by a graph manager, inheriting its flow id) it scopes to that
  workflow. Pick deliberately.
- **Know the default aggregated stats** (always on, per spider + total): `item_scraped_count`,
  `downloader/response_count`, `downloader/response_status_count/*`, `spider_exceptions/*`,
  `scrapy-zyte-api/429`, `zyte_api_proxy/response/status/429`. Set `stats_only_total = True` to drop
  per-spider entries. (Note: per-spider *job counts* are **not** aggregated by default.)
- **Persisting stats history:** `--collection-name <name>` saves the whole stats dict to a Scrapy
  Cloud collection (one record per window, `_key` = the date, or `<start> to <end>`) — the built-in
  way to keep monitor history. It only saves when the window is **midnight-aligned** (both ends at
  `00:00`), else it's skipped with an error; so schedule day-aligned runs (e.g. `-e "today at 0:00"`).
- **Visualizing stat trends:** to see how a monitor's aggregated stats evolve over time, scan the
  monitor's own jobs with the **scanjobs** tool (filter by its run tag, `-s` the stat, `--plot`) —
  see the `scanjobs-programs` skill. A lightweight complement to `--collection-name`, often
  predefined as scanjobs `programs`.
- **Always `super().add_argparser_options()`** before adding CLI args.

## Entry point boilerplate

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    Monitor().run()
```
