---
name: shub-workflow-issuers
description: >-
  Use when building, updating, fixing, or understanding a shub-workflow issuer — a fundamental
  Scrapy Cloud data-pipeline component that reads a massive input (finished spider jobs or batch
  files), processes/dedups/transforms the items, and writes a massive batch output, chaining into the
  post-crawl processing pipeline up to delivery. Built on shub_workflow.issuer (IssuerScript /
  IssuerScriptWithFileSystemInput / IssuerScriptWithSCJobInput) over BaseLoopScript. Use for
  consumers, deduplicators, filters, balancers, reducers, and issuer-based delivery scripts — and
  when migrating an old delivery (BaseDeliverScript) to an issuer.
---

# shub-workflow issuers

An **issuer** reads a massive input, processes the items, and writes a massive batch output —
chaining issuers builds the whole post-crawl data pipeline (consumer → filter → deduplicator →
balancer → **delivery**). `IssuerScript`
([`shub_workflow/issuer/__init__.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/issuer/__init__.py))
is a `BaseLoopScript` (see the `shub-workflow-scripts` skill), generic over an `IssuerItem` subtype.
Full reference:
[Appendix G: Issuer Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-G:-Issuer-Classes).

Issuers are the modern backbone of the data chain; some older projects don't use them yet but are
expected to migrate, and delivery especially should move from the deprecated `BaseDeliverScript` to
an issuer.

## Choose the input subclass

| Class | Input | Use for |
| --- | --- | --- |
| `IssuerScriptWithFileSystemInput[T]` | batch files in a folder | mid-pipeline stages: consumer-of-files, filter, deduplicator, balancer, reducer ([examples/file_input_issuer.py](examples/file_input_issuer.py)) |
| `IssuerScriptWithSCJobInput[T]` | finished SC spider jobs (CLI `target`) | first stage reading raw crawl output, and delivery ([examples/sc_job_input_issuer.py](examples/sc_job_input_issuer.py)) |

Delivery-as-an-issuer (replacing `BaseDeliverScript`): [examples/delivery_issuer.py](examples/delivery_issuer.py).
API tables: [references/api-cheatsheet.md](references/api-cheatsheet.md).

## Consumer vs deduplicator (the two archetypes)

Most issuers are one of two shapes that differ on **dedup persistence**:

- **Consumer** (first stage): cheap **in-memory** preliminary dedup, **distributes** items across
  `parallel_outputs` slots (so the same id always lands in the same slot), often also extracting
  seeds to the frontier. Short-lived and **cheaply restartable** — **no** `load_last_outputs`; a
  restart only loses in-memory dedup (the authoritative dedup is downstream).
- **Deduplicator** (heavy stage): runs **continuously**, one instance per slot, doing massive
  **persistent** dedup. Calls `load_last_outputs(...)` on init to refill its bloom filter from the
  last `LOAD_DELIVERED_IDS_DAYS` of its own (and downstream) output, so dedup **survives restarts**.

So `load_last_outputs` / `LOAD_DELIVERED_IDS_DAYS` is a **deduplicator** trait (long-term
persistence), not a consumer one. Dedup lives in the deduplicators (not the consumer) deliberately:
the volatile, single-writer consumer stays cheap to restart, and dedup scales across N slots instead
of bottlenecking in one process.

## Core procedure

1. Define your item: subclass `IssuerItem` with your fields; bind it
   (`class X(IssuerScriptWithFileSystemInput[MyItem])`).
2. Pick the input subclass (file vs SC-job). It implements input discovery/reading/consumption; you
   normally just implement **`build_item_id(item)`** and set attributes.
3. Set output: `output_folder` (required), `default_filesize`, and `parallel_outputs` (>1 for
   hash-routed slots so N downstream issuers can each own a slot).
4. Dedup: keep `dedupe=True`; for cross-run dedup set `LOAD_DELIVERED_IDS_DAYS` **and** call
   `load_last_outputs(...)` in `__init__`.
5. Custom logic by overriding `process_item` (filter/transform — call `super()`) or, for delivery,
   accumulate in `process_item` and merge+issue+`flush_files` in `process_input`.
6. `__main__` boilerplate (`Script().run()` — synchronous). Register in `setup.py`; deploy via the
   **`scrapy-cloud-deployment`** skill.

Projects often add a base issuer mixin (`class ProjectIssuerMixin(ProjectScriptMixin, IssuerScript)`)
for shared output-routing/sizing — subclass it (mixin first) when present.

## Critical rules & footguns

- **Implement `build_item_id`.** It's abstract and defines item identity/dedup. (In delivery issuers
  that override `process_item` to accumulate, `build_item_id` may be unused — return a constant.)
- **`LOAD_DELIVERED_IDS_DAYS` requires `load_last_outputs()`** in `__init__`, or `on_start()` raises.
  It's the **deduplicator** trait: it reloads the issuer's own (and optionally **downstream**) output
  folders into `seen` so dedup survives restarts. A **consumer** omits it (in-memory dedup only).
- **`parallel_outputs` hash-routes by id** (`hash_mod(id, N)`) so the same id always lands in the
  same slot — that's what lets N parallel downstream issuers each own one slot. Use `input_slot` to
  pin an instance to one input slot.
- **Delivery pattern differs:** `dedupe=False`, `close_on_no_inputs=True`, large `default_filesize`,
  and **don't call `issue_item` inline** — accumulate in `process_item`, then merge + `issue_item` +
  `flush_files()` in `process_input` (one delivery file per source per job). This replaces
  `BaseDeliverScript`, which is **deprecated** (now warns on instantiation).
- **Stopped-spider flush assumes `py:crawlmanager.py`.** The loop flushes a source's partial batch
  when that source's spider is no longer running, detected via
  `get_project_running_spiders(crawlmanagers=("py:crawlmanager.py",))` — the default expects the
  upstream discovery crawl manager to have that name.
- **Set `loop_mode`** for a continuous stage; use `close_on_no_inputs=True` for a run-to-completion
  stage (e.g. reducers, delivery).
- **Launch is synchronous** (`Script().run()`), and **always `super().add_argparser_options()`** if
  you add CLI args.

## Entry point boilerplate

```python
if __name__ == "__main__":
    import logging
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    MyIssuer().run()
```
