---
name: shub-workflow-scripts
description: >-
  Use when writing, fixing, or updating a Python script built on the shub_workflow.script base
  classes (BaseScript / BaseLoopScript / BaseLoopScriptAsyncMixin / ArgumentParserScript) — i.e. any
  script that operates on Scrapy Cloud: scheduling spiders or scripts, scanning/querying SC jobs,
  aggregating stats, or running as a crawl manager, monitor, scheduler, consumer/deliverer, or an
  ad-hoc CLI that talks to SC — whether it runs ON Scrapy Cloud or locally against a project. When
  asked to create a new "script", first confirm it is a Scrapy Cloud script (see below), since these
  base classes are the right tool precisely when the script deploys to or operates on Scrapy Cloud.
---

# Writing shub-workflow scripts

shub-workflow scripts subclass a base class in
[`shub_workflow/script.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/script.py)
and get, for free: argument parsing (+ reusable `-g`/`-v` "programs"), project-id resolution, the
ScrapinghubClient, job scheduling with flow/name tagging, paginated+retrying job queries, a Scrapy
stats collector and an `FSHelper`. The deep reference is the wiki:
[Appendix B: Script Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-B:-Script-Classes).

## First: is this a Scrapy Cloud script?

A brand-new file isn't yet registered in the project's `setup.py`, so you can't tell from the repo.
**When asked to "create a script", confirm with the user whether it will deploy to or operate on
Scrapy Cloud** (run as an SC job, schedule spiders/scripts, scan/query SC jobs, aggregate stats,
etc.). If yes → use these base classes. If it's just a local utility with no SC interaction, a plain
script is fine and this skill doesn't apply. When editing an existing file that already imports
`shub_workflow.script`, this skill applies directly.

## Choose the base class

| Base class | Use when | Template |
| --- | --- | --- |
| `BaseScript` | one-shot: parse args, do work in `run()`, exit (the default) | [examples/plain_script.py](examples/plain_script.py) |
| `BaseLoopScript` | must repeat work on an interval / run continuously until stopped | [examples/loop_script.py](examples/loop_script.py) |
| `BaseLoopScriptAsyncMixin` (+ `BaseLoopScript`) | the loop cycle is asyncio-based (schedules/awaits many things at once) | [examples/async_loop_script.py](examples/async_loop_script.py) |
| `ArgumentParserScript` | only argparse + `PROGRAMS`, **no** SC access (rare; the base the others build on) | — |

Projects usually add a shared **base mixin** (common CLI options/helpers) that every concrete script
inherits — see [examples/project_base_mixin.py](examples/project_base_mixin.py). Check whether the
project already has one and build on it rather than re-adding shared options.

## Core procedure

1. Confirm it's an SC script (above); pick the base class; reuse the project's base mixin if present.
2. Set `description` (property) and add your arguments in `add_argparser_options()` —
   **always call `super()` first** so `--project-id`/`-g`/`-v`/etc. survive.
3. Implement the entry method: `run()` for `BaseScript`; `workflow_loop()` (returns `bool`; `async
   def` for the async mixin) plus optional `on_start()`/`on_close()` for loop scripts.
4. Use the base API for SC work — scheduling, job queries, tags, stats — see
   [references/api-cheatsheet.md](references/api-cheatsheet.md).
5. Add the `__main__` boilerplate (below).
6. To make it runnable, register it in the project's `setup.py`. Deployment itself is handled by the
   **`scrapy-cloud-deployment`** skill, not here.

## Critical rules & footguns

- **Always `super().add_argparser_options()`** before adding arguments, or you lose the framework
  flags (`--project-id`, `--flow-id`, `-g/-v`, loop flags).
- **Mixin order is `class X(ProjectMixin, BaseScript)`** — mixin first, concrete base last. The mixin
  must inherit the typing-only `BaseScriptProtocol` (never `BaseScript`), so the implementation
  isn't duplicated in the MRO.
- **Async entry point differs:** a `BaseLoopScriptAsyncMixin` script's `run()` is a coroutine —
  launch it with `asyncio.run(script.run())`, and make `workflow_loop` an `async def`.
- **Two project ids:** `self.project_id` is the *target* (where you schedule/query, from
  `--project-id`); the script's own running project can differ. Don't hardcode ids; pass
  `--project-id` or set `default_project_id`.
- **`workflow_loop()` returns `bool`:** `True` keeps looping (with `--loop-mode`/`loop_mode`);
  `False` stops immediately. A loop with `loop_mode = 0` runs its body once.
- **Set `project_required = False`** only for scripts that genuinely never touch SC.
- **File I/O? Use the built-in `self.fshelper`** (an `FSHelper`) — don't construct your own
  `FSHelper`/`S3Helper`. To configure its credentials/role/ACLs, **override the `init_fshelper()`
  hook** (default is a bare `FSHelper()`); `self.project_settings` is available there. See the
  api-cheatsheet's *File operations* and the **`shub-workflow-fshelper`** skill.
- For the `-g`/`-v` `PROGRAMS` mechanism (predefined command-line shortcuts), see the
  **`scanjobs-programs`** skill — `scanjobs.py` is the canonical example of a heavily
  `PROGRAMS`-driven script.

## Entry point boilerplate

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    script = MyScript()
    script.run()          # ... or  asyncio.run(script.run())  for a BaseLoopScriptAsyncMixin script
```
