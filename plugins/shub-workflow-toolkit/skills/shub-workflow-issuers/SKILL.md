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

## `IssuerScriptWithSCJobInput` options

`IssuerScriptWithSCJobInput` exposes three flags (defaults in parens) — all generic, not
delivery-specific:

- **`set_item_source`** (`True`) — stamp each read item's `source` with the scanned spider's canonical
  name. Set **`False`** when the scanned spider is a **secondary/post-processing** stage (it processes
  data produced by a primary spider): its items already carry their originating `source`, so conserve
  it instead of overwriting. *This is why a delivery issuer keeps the upstream source.*
- **`flush_on_each_input`** (`False`) — flush output files at the end of **each** scanned job. If you
  want **all items of one job in the same output file**, pair it with a big **`default_filesize`** (so
  a batch is never split by size before the per-job flush).
- **`scope_input_to_flow_id`** (`False`) — read only jobs tagged with this script's own `FLOW_ID`
  (from `--flow-id`, or the `FLOW_ID` tag inside a workflow), so a script scheduled by a **graph
  manager** reads all and only its own workflow instance's jobs. Ports what the deprecated
  `BaseDeliverScript` did automatically; no-op (reads everything, with a warning) if no `flow_id`.

Override the **`post_process_input_items(spider_job, args)`** hook (default no-op) to run logic once
per scanned job **after all its items are read** and **before** the flush (`spider_job` is the read
job — use its metadata/key/items). It enables the
**accumulate-then-merge** pattern below. It's also where you'd aggregate a job's stats if needed —
mix in `SpiderStatsAggregatorMixin` yourself and call `self.aggregate_spider_stats(...)` here (no
built-in flag for it).

`get_new_inputs()` visits the targeted spiders **round-robin, rotating the starting spider across
loops**: when the target matches **multiple** spiders (e.g. `class:BaseSpider`, to consume every source
in one job) a source with a big backlog can't starve the others, since only `max_inputs_per_loop`
inputs run per loop and each loop resumes where the previous left off. It's a no-op for a
single-spider target.

## Accumulate-then-merge (any issuer, not delivery-specific)

When an input's records must be **combined** into fewer output records (join, roll-up, reconcile),
override `process_item` to **accumulate** them (do **not** call `super()`), then in
`post_process_input_items` merge the accumulated records, set each combined record's `id` /
`input_source`, `issue_item()` them, and clear the accumulator; pair with `flush_on_each_input=True`
for one output file per job. This is generic to **any** `IssuerScriptWithSCJobInput` — a delivery
issuer may use it, but so may a filter or roll-up stage. See
[examples/merge_issuer.py](examples/merge_issuer.py).

### Pairing the hook with `flush_on_each_input` (decision guide)

`flush_on_each_input` and `post_process_input_items` are **orthogonal**. `flush_on_each_input` only
controls output **file granularity** — one file per scanned job vs. size-batched by `default_filesize`
— it does **not** gate the hook. They co-occur in the *one-delivery-file-per-job* shape (issue the
job's records in the hook, then flush them together), which is why our delivery and per-job-merge cases
set `True` — but that pairing is not required:

- **Hook issues, but you want size-batched output → `flush_on_each_input=False`.** A **roll-up/reducer**
  that accumulates a whole job and emits only a *few* merged records per job should leave it `False`, so
  those records batch up to `default_filesize`; `True` would spray thousands of tiny one-record files.
  The records issued in the hook are still written (by size / `on_close`), just packed across jobs.
- **Hook issues nothing → `flush_on_each_input` irrelevant (leave `False`).** Non-issuing per-job side
  effects — `aggregate_spider_stats`, pushing seeds to the frontier, capturing job metadata — touch no
  output queue, so flush timing doesn't matter (e.g. a consumer that issues inline in `process_item` and
  does its per-job seed push in the hook).

**Timing:** the hook runs **after** any mid-read size-flushes (an inline `issue_item` that hit
`default_filesize` during the read loop already wrote a file) and **before** the per-job flush. So to
finalize a *whole job's* items before any of them are written, don't inline-issue — **accumulate** in
`process_item` (or use a big `default_filesize`) so nothing flushes before the hook.

## Consumer vs deduplicator (the two archetypes)

Most issuers are one of two shapes that differ on **dedup persistence**:

- **Consumer** (first stage): cheap **in-memory** preliminary dedup, **distributes** items across
  `parallel_outputs` slots (so the same id always lands in the same slot). A **discovery** consumer
  also explodes/side-effects — when each scraped record bundles many issuable objects, set
  **`explode_input_items`** (a jmespath to that list) so the base calls `process_item()` once per object;
  put per-item work in `process_item` and per-job side effects (e.g. extracting seeds and writing them to
  the frontier) in `post_process_input_items` — no `process_input` override needed (see
  [examples/sc_job_input_issuer.py](examples/sc_job_input_issuer.py)). Short-lived and **cheaply
  restartable** — **no** `load_last_outputs`; a restart only loses in-memory dedup (the authoritative
  dedup is downstream).
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
   hash-routed slots so N downstream issuers can each own a slot). Output is grouped **per source** by
   default (one file per `(slot, source)`); set `separate_output_by_source=False` to pack all sources
   into `default_filesize`-sized files per slot instead (one file per slot, no source in the default
   filename). The output queue is an in-memory dict by default; set `persist_items_queue_on_disk=True`
   to back it with an on-disk `SqliteDict` (much lower memory, slower) when items are large enough that
   a batch would not fit in RAM — e.g. a delivery of records carrying heavy metadata.
   `persist_items_queue_dir` chooses where that sqlite file goes (default: cwd), and
   `items_queue_read_chunk_size` (default `100`) caps how many items are held while writing the output
   file. **Never iterate a bucket with `sqlitedict`'s own `values()`/`items()`/`keys()`** — see the
   footgun below.
4. Dedup: keep `dedupe=True`; for cross-run dedup set `LOAD_DELIVERED_IDS_DAYS` **and** call
   `load_last_outputs(...)` in `__init__`.
5. Custom logic by overriding `process_item` (filter/transform — call `super()`); to **combine** an
   input's records, *accumulate* in `process_item` and merge+`issue_item` in the
   `post_process_input_items` hook (the accumulate-then-merge pattern).
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
- **Delivery is a configuration, not a class** (there is **no** `DeliverIssuerScript`): a delivery is
  an `IssuerScriptWithSCJobInput` you configure for the terminal stage. Which knobs you set is
  **use-case dependent**, not fixed by "delivery": `close_on_no_inputs=True` and — for one file per job
  — `flush_on_each_input=True` + a big `default_filesize` are typical; **`dedupe=False` and
  `set_item_source=False` apply only when the data was already deduplicated/post-processed upstream**
  (delivering a primary spider's crawl keeps the defaults). You supply `build_item_id` and the
  destination (`compute_destination_filename`). Replaces the **deprecated** `BaseDeliverScript` (warns
  on instantiation). Example: [examples/delivery_issuer.py](examples/delivery_issuer.py); migrating an
  old one: [references/migrating-from-basedeliverscript.md](references/migrating-from-basedeliverscript.md).
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
