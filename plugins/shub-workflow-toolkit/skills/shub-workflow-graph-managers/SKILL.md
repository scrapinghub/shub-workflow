---
name: shub-workflow-graph-managers
description: >-
  Use when building, updating, fixing, or understanding a shub-workflow graph manager — a script
  that runs an arbitrary DAG of tasks (spiders and scripts, including crawl managers and deliver
  scripts) on Scrapy Cloud with dependencies, built on shub_workflow.graph (GraphManager + Task /
  SpiderTask) and WorkFlowManager. Covers configure_workflow(), Task vs SpiderTask, dependency
  linking (add_next_task / add_wait_for), the on_finish/retry routing, resources, parallelization,
  and the start directives. Use for any graph-manager / flow-manager script that subclasses
  GraphManager (directly or via a project base mixin).
---

# shub-workflow graph managers

A **graph manager** orchestrates a directed graph of **tasks** — spiders and scripts (often crawl
managers + a deliver script) — on Scrapy Cloud, scheduling each as its dependencies are satisfied.
`GraphManager` is a `WorkFlowManager` (inherits name / flow-id / owned-jobs / resume); tasks are
`Task` (a script) or `SpiderTask` (a spider). Source:
[`shub_workflow/graph/`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/graph/__init__.py).
Full reference:
[Appendix E: Graph Manager Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-E:-Graph-Manager-Classes)
(+ [Appendix C: Workflow Manager](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-C:-Workflow-Manager)
for the inherited layer); intro:
[Graph Managers](https://github.com/scrapinghub/shub-workflow/wiki/Graph-Managers). Builds on the
`shub-workflow-scripts` and `shub-workflow-crawl-managers` skills.

## When to use a graph manager

When you need **more than one job with dependencies** — e.g. "run a crawl manager, then a deliver
script once it finishes", fan-out/fan-in, or branching by outcome. For scheduling a single spider or
a stream of one spider's jobs, a crawl manager is simpler (see `shub-workflow-crawl-managers`).

## Core procedure

1. Subclass `GraphManager`; set `loop_mode` (and `name`, or pass it positionally).
2. Implement **`configure_workflow()`**: create tasks, link them, and **return the root task(s)**.
   - `Task(task_id, command, init_args=[...])` for a script (`command` = `py:foo.py` or a Jinja
     template); `SpiderTask(task_id, spider, **spider_args)` for a spider.
   - `a.add_next_task(b)` → b runs after a; `a.add_wait_for(b)` → a waits for b (pure dependency).
3. Optional: per-task `retries`, `on_finish` routing by outcome, `add_required_resources(...)`,
   parallelization (multi-line `Task` command), and a `bad_outcome_hook` override.
4. Add the `__main__` boilerplate (`MyGraphManager().run()`).
5. Register the file in the project's `setup.py`; deploy via the **`scrapy-cloud-deployment`** skill.
   The scripts/spiders the graph schedules are separate deployables.

Templates: [examples/graph_manager.py](examples/graph_manager.py) (basic),
[examples/parallel_and_resources.py](examples/parallel_and_resources.py) (parallel + resources +
on_finish + bad_outcome_hook). API tables: [references/api-cheatsheet.md](references/api-cheatsheet.md).

## Critical rules & footguns

- **Override `configure_workflow()` and return the ROOT tasks** (a tuple). Returning the wrong tasks
  (or forgetting to link successors) silently changes the graph. Only root tasks should be returned;
  successors are reached via the edges.
- **A start directive is mandatory at runtime:** invoke with `--root-jobs` (start from the returned
  roots) **or** `--starting-job <id>` (repeatable). Neither → the manager errors on start. They're
  mutually exclusive.
- **Set `loop_mode`** (it's a loop manager) and a **`name`** (WorkFlowManager — class attr or first
  positional arg). No name → error at startup.
- **`retries` defaults to `1`**, and `on_finish["failed"]` defaults to `["retry"]` — so a failed
  task is retried once by default, then routed to `bad_outcome_hook`. Set `retries=0` to disable.
- **`Task` parallelization** comes from a **multi-line rendered command** (Jinja `{% for %}`): N
  lines → N parallel subtasks `task_id.0..N-1`; successors wait for all. `SpiderTask` never
  parallelizes.
- **`task_id` rules:** must be unique, can't be `"retry"`, and a `Task` id can't contain `.` (used
  for parallel subtask ids).
- **`on_finish` routing order:** exact outcome → `"failed"` (for failed outcomes) → `"default"` (the
  `add_next_task` targets). Use a custom `on_finish={...}` to branch on specific close reasons; still
  `add_next_task` any task referenced there so it's registered in the graph.
- **Resources** gate a *class* of tasks independently of `--max-running-jobs` (named semaphores via
  `add_required_resources`). Don't conflate the two limits.
- **Always `super().add_argparser_options()`** if you add CLI args.

## Entry point boilerplate

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MyGraphManager().run()
```
