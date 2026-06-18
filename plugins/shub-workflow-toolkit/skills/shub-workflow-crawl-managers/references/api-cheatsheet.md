# Crawl manager API cheat-sheet

Full reference in the wiki:
[Appendix D: Crawl Manager Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-D:-Crawl-Manager-Classes)
(crawl manager classes) and
[Appendix C: Workflow Manager](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-C:-Workflow-Manager)
(the inherited `WorkFlowManager` layer).
Source of truth:
[`shub_workflow/crawl.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/crawl.py)
and [`shub_workflow/base.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/base.py).

## Class attributes

| Attribute | On | Default | Meaning |
| --- | --- | --- | --- |
| `name` | WorkFlowManager | `""` | **required** — set here or pass as the first positional arg. Distinct managers in a workflow need distinct names. |
| `default_max_jobs` | WorkFlowManager | `1000` | cap on simultaneously-running children (`--max-running-jobs` overrides). |
| `flow_id_required` | WorkFlowManager | `True` | a flow id is mandatory (auto-generated if absent). |
| `acquire_all_jobs` / `dont_acquire_finished_jobs` | WorkFlowManager | `False` | resume tuning (acquire owned jobs regardless of flow id / skip finished). |
| `base_failed_outcomes` | WorkFlowManager | tuple | outcomes routed to `bad_outcome_hook`; copied to mutable `self.failed_outcomes`. |
| `loop_mode` | BaseLoopScript | `0` | seconds between cycles; **required** for periodic/generator managers (`0` = run once). |
| `spider` | CrawlManager | `None` | default target spider; if unset a positional `spider` arg is required. |
| `running_jobs_reverse_check` | CrawlManager | `False` | check running jobs newest-first when `True`. |
| `MAX_RETRIES` | GeneratorCrawlManager | `0` | built-in retry count for failed outcomes (`0` = no retries; `cancelled` never retried). |

## CLI arguments (added across the chain)

Positional `name` (unless `name` attr set), positional `spider` (unless `spider` attr set);
`--max-running-jobs`, `--spider-args` (JSON), `--job-settings` (JSON), `--units`; plus `--loop-mode`,
`--max-running-time`, `--project-id`, `--flow-id`, `--children-tag`, `-g`/`-v`.

## Methods you override

| Method | On | Purpose |
| --- | --- | --- |
| `set_parameters_gen(self)` | GeneratorCrawlManager | **abstract** — yield one dict of args per job to schedule. The core of a generator manager. |
| `bad_outcome_hook(self, spider, outcome, job_args_override, jobkey)` | CrawlManager | react to a failed outcome (Generator's default does retries). |
| `finished_ok_hook(self, spider, outcome, job_args_override, jobkey)` | CrawlManager | react to a successful finish (mutually exclusive with bad_outcome_hook per job). |
| `get_retry_override(self, spider, outcome, job_args_override, jobkey)` | GeneratorCrawlManager | per-retry param changes; raise `StopRetry` to abort retrying. |
| `can_schedule_job_with_params(self, params) -> bool` | GeneratorCrawlManager | return `False` to defer a job to a later cycle (throttling). |
| `running_job_hook(self, jobkey)` / `finished_metadata_hook(self, jobkey, metadata)` | CrawlManager | react to running / any-finished jobs. |
| `resume_running_job_hook(job)` / `resume_finished_job_hook(job)` | WorkFlowManager | rebuild state when resuming. |
| `description` (property) / `add_argparser_options(self)` | (base) | help text / extra CLI args (always `super()` first). |

## Methods you call

- `add_job(spider, job_args_override)` — **GeneratorCrawlManager only** — queue an extra job ahead of the generator (used in retry/hook logic).
- `schedule_spider_with_jobargs(job_args_override=None, spider=None)` — schedule one job (async on the async mixin).
- `spider_running_count(spider)` / `get_running_spiders()` / `spider_delayed_count(spider)` — introspect current scheduling state.
- `get_job_settings(override=None)` — `--job-settings` merged with an override.
- `create_dupe_filter()` — classmethod; override to change the de-dup bloom filter capacity/error rate.
- WorkFlowManager: `get_owned_jobs(state=[...], **kw)`, `get_finished_owned_jobs(...)`, `wait_for(keys, ...)`, `max_running_jobs` (property).
- Plus the whole `BaseScript` API (scheduling, job queries, tags, stats) — see the `shub-workflow-scripts` skill.

## set_parameters_gen() dicts

Each yielded dict = flat spider args. These keys are pulled out and handled specially:
`spider` (per-job target override), `units`, `tags`, `job_settings`, `project_id` (cross-project).
Everything else becomes the job's spider args. **De-dup:** identical `(spider, spider_args)` is
scheduled once — re-yielding the same params won't reschedule.

## Entry point

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel
    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    CrawlManager().run()          # ... or  asyncio.run(CrawlManager().run())  with the async mixin
```
