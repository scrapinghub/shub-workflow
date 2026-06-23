# Graph manager API cheat-sheet

Full reference in the wiki:
[Appendix E: Graph Manager Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-E:-Graph-Manager-Classes)
(and [Appendix C: Workflow Manager](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-C:-Workflow-Manager)
for the inherited layer). Source:
[`graph/__init__.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/graph/__init__.py)
and [`graph/task.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/graph/task.py).

## GraphManager

| Member | Notes |
| --- | --- |
| `configure_workflow(self) -> Tuple[BaseTask, ...]` | **abstract, you implement** — build the graph, return root task(s). |
| `loop_mode` | seconds between cycles; **required** (it's a loop manager). |
| `name` | required (WorkFlowManager) — class attribute or first positional CLI arg. |
| `bad_outcome_hook(self, task_id, jobid, outcome)` | *override* — task failed with no retries left. |
| `get_task(task_id)`, `pending_jobs`, `get_running_jobid(task_id)` | inspect graph state. |

CLI: `--root-jobs` | `--starting-job/-s <id>` (repeatable, mutually exclusive with `--root-jobs`),
`--only-starting-jobs`, `--skip-job <id>`, `--jobs-graph <yaml>`, `--comment`; plus WorkFlowManager's
positional `name`, `--max-running-jobs`, `--loop-mode`, `--flow-id`, `--children-tag`, `-g`/`-v`.

## Tasks (`shub_workflow.graph.task`)

Shared `BaseTask` params: `task_id` (unique; not `"retry"`), `tags`, `units`, `retries` (default
`1`), `project_id`, `wait_time`, `on_finish`.

| Class | Schedules | Signature highlights |
| --- | --- | --- |
| `Task` | a **script** | `Task(task_id, command, init_args=None, retry_args=None, tags=, units=, retries=1, project_id=, wait_time=, on_finish=)`. `command` = script name (`py:foo.py`) or Jinja2 template; multi-line render ⇒ parallel subtasks. No `.` in `task_id`. |
| `SpiderTask` | a **spider** | `SpiderTask(task_id, spider, tags=, units=, retries=1, wait_time=, on_finish=, job_settings=, **spider_args)`. No parallelization. |

### Linking (call in `configure_workflow`, before the graph locks)

| Method | Meaning |
| --- | --- |
| `add_next_task(task)` | successor — runs after this completes (the `"default"` `on_finish` edge). |
| `add_wait_for(task)` | dependency edge (must wait, without being scheduled-after). |
| `add_required_resources(ResourcesDict({Resource("x"): n}))` | needs `n` of resource `x` to start. |
| `set_start_callback(fn)` | advanced — runs when the task goes pending (add successors lazily). |
| `get_scheduled_jobs()` | the job ids this task scheduled (after the fact). |

## on_finish routing

A task's `on_finish` maps an outcome → list of next task ids (or `"retry"`). Resolution order on
finish: exact outcome key → `"failed"` (if the outcome is a failed outcome) → `"default"` (the
`add_next_task` targets). With `retries > 0`, `"failed"` defaults to `["retry"]`, so a failed task
retries up to `retries` times, then falls through to `bad_outcome_hook`.

## Start directive (required)

Pass `--root-jobs` (start from `configure_workflow()`'s returned tasks) **or** one/more
`--starting-job <task_id>`. Example: `python flowmanager.py mygraph --root-jobs`.

## Entry point

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel
    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MyGraphManager().run()
```
