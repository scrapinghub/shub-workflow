---
name: shub-workflow-crawl-managers
description: >-
  Use when building, updating, fixing, or understanding a shub-workflow crawl manager — a script
  that schedules spider jobs on Scrapy Cloud and reacts to their outcomes, built on
  shub_workflow.crawl (CrawlManager / PeriodicCrawlManager / GeneratorCrawlManager /
  AsyncSchedulerCrawlManagerMixin) and WorkFlowManager. Covers choosing the base class, the
  set_parameters_gen() generator pattern, the outcome/retry/throttling hooks, async concurrent
  scheduling, and the name/flow-id/loop-mode rules. Use for any crawl-manager script in any project
  that subclasses these classes (directly or via a project base mixin).
---

# shub-workflow crawl managers

A **crawl manager** is a workflow-manager script that schedules spider jobs on Scrapy Cloud and
reacts to how they finish. The classes live in
[`shub_workflow/crawl.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/crawl.py)
on top of `WorkFlowManager` → `BaseLoopScript`. Full reference:
[Appendix C: Crawl Manager Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-C:-Crawl-Manager-Classes);
gentler intro: the [Crawl Managers](https://github.com/scrapinghub/shub-workflow/wiki/Crawl-Managers)
tutorial. This skill builds on the `shub-workflow-scripts` skill (the base-class layer).

## Choose the base class

| Base class | Use when | Template |
| --- | --- | --- |
| `GeneratorCrawlManager` | schedule a **stream** of jobs from a generator of params (the usual case) | [examples/generator_crawl_manager.py](examples/generator_crawl_manager.py) |
| `AsyncSchedulerCrawlManagerMixin` (+ `GeneratorCrawlManager`) | the cycle schedules **many** jobs and you want them dispatched concurrently | [examples/async_generator_crawl_manager.py](examples/async_generator_crawl_manager.py) |
| `PeriodicCrawlManager` | keep **one** spider job running, rescheduling it after each finish | [examples/periodic_crawl_manager.py](examples/periodic_crawl_manager.py) |
| `CrawlManager` | schedule a **single** spider job once (rare on its own; it's the base of the others) | — |

Retries / outcome hooks / throttling: [examples/retries_and_hooks.py](examples/retries_and_hooks.py).
API tables: [references/api-cheatsheet.md](references/api-cheatsheet.md).

## Core procedure

1. Pick the base class. The default answer is `GeneratorCrawlManager`.
2. Set `name` and `loop_mode` (both effectively required — see footguns), plus `spider` and
   `default_max_jobs`, and a `description`.
3. Implement `set_parameters_gen()` — yield one dict per job. Flat keys become spider args; the keys
   `spider`, `units`, `tags`, `job_settings`, `project_id` are handled specially.
4. Add reactions only if needed: `MAX_RETRIES` + `get_retry_override` (or a custom `bad_outcome_hook`),
   `finished_ok_hook`, `can_schedule_job_with_params` for throttling.
5. Add the `__main__` boilerplate — `CrawlManager().run()`, or `asyncio.run(CrawlManager().run())`
   for the async mixin.
6. Register the file in the project's `setup.py`; deploy via the **`scrapy-cloud-deployment`** skill.

If the project already has a **base crawl-manager mixin** (shared options/helpers), subclass it:
`class CrawlManager(ProjectMixin, GeneratorCrawlManager)` — mixin first (find existing ones with
`grep -rl GeneratorCrawlManager <project>/`).

## Critical rules & footguns

- **A name is required.** `WorkFlowManager` mandates `name` (and `flow_id_required = True`). Set the
  `name` class attribute, or pass it as the **first positional** CLI arg. No name → the script errors
  at startup. Distinct managers in one workflow must have distinct names.
- **Set `loop_mode`** (class attr or `--loop-mode`) for periodic/generator managers. With
  `loop_mode = 0` the manager runs one cycle and exits — a generator manager would schedule only the
  first batch and quit.
- **`spider` attribute vs positional.** If `spider` is unset, a positional `spider` CLI arg is
  required; if set, that arg disappears. A yielded `"spider"` key overrides the target per job (set
  `spider = ""` to *force* every yielded dict to name its spider).
- **De-duplication is built in.** A job whose `(spider, spider_args)` was already scheduled is
  **skipped** (bloom filter). Don't expect re-yielding identical params to reschedule; vary the args,
  or override `create_dupe_filter()` to change capacity/error rate.
- **Retries:** set `MAX_RETRIES` (`0` = off; `cancelled` is never retried). Tune per retry with
  `get_retry_override` (raise `StopRetry` to abort), or override `bad_outcome_hook` entirely.
  `add_job()` is **GeneratorCrawlManager-only** — don't call it on `CrawlManager`/`PeriodicCrawlManager`.
- **Async entry point differs.** With `AsyncSchedulerCrawlManagerMixin`: mixin **first** in the
  bases, and launch with `asyncio.run(script.run())`. `set_parameters_gen()` stays a **synchronous**
  generator — only scheduling is async.
- **Always `super().add_argparser_options()`** before adding args, or you lose `name`/`spider`/
  `--spider-args`/etc.
- **`default_max_jobs` defaults to 1000** (not infinite); override the attr or pass `--max-running-jobs`.

## Entry point boilerplate

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    CrawlManager().run()          # ... or  asyncio.run(CrawlManager().run())  for the async mixin
```
