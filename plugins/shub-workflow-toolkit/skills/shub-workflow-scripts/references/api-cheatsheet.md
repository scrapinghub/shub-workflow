# BaseScript API cheat-sheet

The most-used class attributes and methods. Full reference (lifecycle, the two project ids, flow/name
tagging, the loop model, async variant) is in the wiki:
[Appendix B: Script Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-B:-Script-Classes).
Source of truth:
[`shub_workflow/script.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/script.py).

## Class attributes (set on your subclass)

| Attribute | Default | Meaning |
| --- | --- | --- |
| `name` | `""` | logical name → `NAME=` tag, propagated to children as `PARENT_NAME=` |
| `project_required` | `True` | `False` for scripts that don't touch SC (won't error without a project id) |
| `default_project_id` | `None` | default for `--project-id`; `None` ⇒ autodetect |
| `flow_id_required` | `False` | `True` ⇒ must run within a flow id (auto-generated if absent) |
| `children_tags` | `None` | extra tags added to every scheduled child job |
| `PROGRAMS` | `{}` | command-line shortcut definitions (see the `scanjobs-programs` skill) |
| `loop_mode` * | `0` | (loop scripts) default seconds between cycles; `0` = once. `--loop-mode` overrides |
| `max_running_time` * | `0` | (loop scripts) auto-stop after N seconds if `>0` |
| `stats_interval` * | `120` | (loop scripts) seconds between periodic stats uploads |

\* `BaseLoopScript` only.

## CLI arguments you get for free

`--project-id` (numeric or `scrapinghub.yml` keyword — the **target** project), `--flow-id`,
`--children-tag/-t` (repeatable), `--load-sc-settings`, `--program/-g`, `--program-variables/-v`;
plus (loop) `--loop-mode`, `--max-running-time`.

## Methods you override

| Method | Notes |
| --- | --- |
| `description` (property) | the script's help/title |
| `add_argparser_options(self)` | add arguments; **always call `super()`** first |
| `run(self)` | entry point for `BaseScript` (you call it from `__main__`) |
| `workflow_loop(self) -> bool` | one cycle for `BaseLoopScript`; `True` = keep looping. `async def` for the async mixin |
| `on_start()` / `on_close()` | loop hooks (one-time setup/teardown) |
| `base_loop_tasks()` | optional work at the start of every cycle |

## Methods you call

Scheduling:
- `schedule_spider(spider, tags=None, units=None, project_id=None, **kwargs) -> JobKey | None`
- `schedule_script(cmd: list, tags=None, project_id=None, units=None, meta=None) -> JobKey | None`
  (script name normalized to `py:<name>.py`)
- async: `await async_schedule_spider(...)` (on `BaseLoopScriptAsyncMixin`)
- override `handle_schedule_duplicate_error()` / `handle_schedule_error()` to customize failure behavior

Job querying:
- `get_jobs(project_id=None, **kwargs) -> Generator[JobDict]` — paginated + de-duplicated; supports
  `spider=`, `state=[...]`, `has_tag=`, `lacks_tag=`, `count=`, `meta=[...]`, `startts=`, …
- `get_jobs_with_tags(spider, tags, project_id=None, **kwargs) -> Generator[Job]`
- `is_running(jobkey) -> bool`, `is_finished(jobkey) -> close_reason | None`,
  `finish(jobkey=None, close_reason=None)` (own job if `jobkey` is None)

Tags & flow:
- `append_flow_tag(tag)` (tag transmitted to children), `add_job_tags(jobkey=None, tags=[])`,
  `remove_job_tags(tags, jobkey)`, `get_job_tags(jobkey=None)`, `get_keyvalue_job_tag(key, tags)`

Stats & settings:
- `self.stats` (Scrapy `StatsCollector`: `inc_value`/`set_value`/`get_value`), `upload_stats()`,
  `print_stats()`
- `get_sc_project_settings()` — live project settings from the dashboard API
- `self.fshelper` — `FSHelper` for s3/gcs/local file ops

Spiders:
- `get_canonical_spidername(spidername)`, `get_project_running_spiders(canonical=False,
  crawlmanagers=(), only_crawlmanagers=False)`

## Entry point boilerplate

```python
if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    script = MyScript()
    script.run()          # ... or  asyncio.run(script.run())  for a BaseLoopScriptAsyncMixin script
```
