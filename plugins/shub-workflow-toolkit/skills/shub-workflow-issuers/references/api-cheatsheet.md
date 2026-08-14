# Issuer API cheat-sheet

Full reference in the wiki:
[Appendix G: Issuer Classes](https://github.com/scrapinghub/shub-workflow/wiki/Appendix-G:-Issuer-Classes).
Source:
[`shub_workflow/issuer/__init__.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/issuer/__init__.py).

## What it is

`IssuerScript[ITEMTYPE, PROCESS_INPUT_ARGS_TYPE]` is a **`BaseLoopScript`** that reads a massive
input (SC jobs or batch files), processes/dedups items, and writes batch output files — the building
block of the post-crawl data pipeline (consumers, deduplicators, balancers, delivery). Pick a
ready-made input subclass; you usually only write `build_item_id` + a custom `IssuerItem` + a few
attributes.

## Item model

`IssuerItem` (TypedDict): `id`, `source`, `input_source`, optional `search_keywords`. Subclass it for
your fields and bind it: `class X(IssuerScriptWithFileSystemInput[MyItem])`.

## Choose the input subclass

| Class | Input | Implements for you |
| --- | --- | --- |
| `IssuerScriptWithFileSystemInput[T]` | gzipped JSON-lines **batch files** in `input_folder` (opt. `input_slot` prefix); moves to `processed_folder` or deletes | `get_new_inputs`, `process_input`, `remove_inputs` |
| `IssuerScriptWithSCJobInput[T]` | finished **SC spider jobs** chosen by the `target` CLI arg (`spider:`/`canonical:`/`class:`); tags consumed jobs `CONSUMED=True` | same |

`IssuerScriptWithSCJobInput` adds these SC-job-input-only flags:

| Attribute | Default | Meaning |
| --- | --- | --- |
| `set_item_source` | `True` | stamp each item's `source` with the scanned spider's canonical name. Set `False` for a **secondary** spider whose items already carry their originating `source` (conserve it). |
| `flush_on_each_input` | `False` | `flush_files()` at the end of each scanned job. Pair with a big `default_filesize` to keep all of a job's items in one output file. Each job is also **consumed (tagged `CONSUMED`) right after its flush** rather than at the end-of-loop sweep, so a mid-loop crash won't re-deliver already-written jobs. Consuming an input also uploads stats immediately (see below), so a monitor still sees delivery stats for every delivered job even if the script is later killed. |
| `scope_input_to_flow_id` | `False` | read only jobs tagged with this script's own `FLOW_ID` (from `--flow-id` / the workflow tag) — a graph-manager-scheduled script reads only its own workflow instance's jobs. No-op (warns) if no `flow_id`. |

(To aggregate a scanned job's stats, mix in `SpiderStatsAggregatorMixin` and call
`aggregate_spider_stats(...)` from `post_process_input_items()` — there is no built-in flag for it.)

`get_new_inputs()` visits the matched spiders **round-robin, rotating the starting spider across loops**
(only `max_inputs_per_loop` inputs run per loop; each loop resumes where the last left off), so when the
target matches many spiders a big-backlog source can't starve the others. No-op for a single-spider target.

## Methods

| Method | Required? | Purpose |
| --- | --- | --- |
| `build_item_id(item) -> ItemId` | **abstract** | dedup/identity key. |
| `adapt_input_item(raw) -> ITEMTYPE` | optional | adapt raw records to your item type (default casts). |
| `process_item(item, input_source)` | optional override | dedup + enqueue; override to filter/transform (call `super()`), or to *accumulate* (delivery pattern — don't call super). |
| `post_process_input_items(spider_job, args)` | optional override *(SC-job input)* | hook run once per scanned job (`spider_job` = the read job), after all its items are read and before the per-job flush; default no-op. Enables the accumulate-then-merge pattern (any issuer). |
| `get_output_slot_for_item(item)` | optional | route to an output slot (default: hash of id over `parallel_outputs`). |
| `get_filesize_from_item(item)` | optional | per-item batch size (default `default_filesize`). |
| `compute_destination_filename(slot, source)` | optional | output filename (default: timestamped, source/slot-prefixed). |
| `load_last_outputs(folders, prefix="", basename_re=None, id_field=("id",))` | call in `__init__` | refill `seen` from prior output — own **and optionally downstream** folders — so a deduplicator's dedup survives restarts (required if `LOAD_DELIVERED_IDS_DAYS` is set). |
| `get_new_inputs` / `process_input` / `remove_inputs` | abstract* | input discovery / reading / consumption — provided by the two subclasses. |

## Configuration attributes

| Attribute | Default | Meaning |
| --- | --- | --- |
| `output_folder` | *(required)* | where batch output files are written. |
| `default_filesize` | `10_000` | items per output batch file. |
| `parallel_outputs` | `1` | output slots; `>1` ⇒ items hash-routed by id (same id → same slot). |
| `separate_output_by_source` | `True` | one output file per `(slot, source)`. `False` ⇒ all sources of a slot pack into `default_filesize` files (one file per slot, source dropped from the default filename; per-source stopped-spider flush disabled). |
| `persist_items_queue_on_disk` | `False` | back each output-queue bucket with an on-disk `SqliteDict` instead of an in-memory dict — low memory (items read from disk in chunks and streamed to the output file), slower. Use for large items whose batch won't fit in RAM (e.g. metadata-heavy deliveries). |
| `persist_items_queue_dir` | `None` (cwd) | directory for the `persist_items_queue_on_disk` sqlite files. Defaults to cwd (rather than `tempfile`'s default `/tmp`), so a big batch doesn't depend on how much room `/tmp` has. |
| `items_queue_read_chunk_size` | `100` | items read at a time from an on-disk bucket when writing the output file; peak flush memory ≈ this × item size. Lower it for very big items. No effect without `persist_items_queue_on_disk`. |
| `explode_input_items` | `None` | jmespath to a list **inside** each raw record; when set, `process_item()` runs once per selected object (one record → many items) instead of once per record. Avoids overriding `process_input` just to explode. |
| `input_slot` / `output_slot` | `None` | pin this instance to one input / output slot. |
| `dedupe` | `True` | bloom-filter de-duplication (set `False` for delivery). |
| `MAX_ITEMS` | `200_000_000` | bloom capacity + per-job processed ceiling. |
| `ERRORS_RATE` | `1e-8` | bloom false-positive rate. |
| `LOAD_DELIVERED_IDS_DAYS` | *(unset)* | days of prior output to reload into `seen` (needs `load_last_outputs`). |
| `close_on_no_inputs` | `False` | stop when no inputs remain (vs. loop forever). |
| `max_inputs_per_loop` | `-1` | cap inputs per cycle (`-1` = unlimited). |
| `min_wait_time_secs_to_flush_stopped_spiders` | `0` | grace before flushing a stopped source's partial batch. |
| `loop_mode` | `0` | (inherited) seconds between cycles; set for a continuous issuer. |
| `input_folder` / `processed_folder` | — | (file-input subclass) where to read / move processed inputs. |

## Data flow

`workflow_loop` → `get_new_inputs()` → per input `process_input()` → per record `process_item()` →
(dedup) → `issue_item()` → enqueue per `(slot, source)` → `send_file()` when the queue hits filesize.
`on_close` flushes remaining queues; an input is retired only after all its items are written. Retiring
inputs (`_remove_pending`) also **uploads stats at that moment** — consumption and stats persistence are
coupled, so a monitor never ends up missing the stats of a job that was delivered but whose script was
killed before the next periodic/close upload.

## Stats reported (what to read from outside)

Set by `process_item()` / `send_file()` / `workflow_loop()`; `<source>` is the item's canonical
`source`. Read them from a monitor, a pipeline status report, or a scan of issuer jobs:

| Stat | Counts |
| --- | --- |
| `urls/issued`, `urls/issued/<source>` | items actually issued (i.e. **not** discarded as dupes) |
| `urls/seen`, `urls/seen/<source>` | incremented **in the same branch** as `urls/issued` — plus, in `load_last_outputs()`, once per previously delivered id reloaded into `seen` |
| `urls/dupes` | items whose id was already in `seen` (global only, no per source variant) |
| `urls/dupesrate`, `urls/dupesrate/<source>` | a **ratio**, `dupes / processed`, `set` (not incremented) on every loop and rounded to 2 decimals |
| `records/wrote`, `records/<source>/wrote`, `records/<slot>/<source>/wrote` | records written to an output file |
| `inputs/processed` | inputs consumed |

Two things that are easy to get wrong:

- **`seen` is not "read", it is "issued"** — both counters are incremented together, only for issued
  items, so in an issuer that never reloads ids they are **equal** per source. A `seen` far larger than
  `issued` is the signature of a **deduplicator** (`load_last_outputs()` inflated `seen` with the
  reloaded delivered ids); in a **consumer** they match. Neither counts the items read: to count those,
  use the input side (`inputs/processed`, `records/*/wrote`) or the upstream job's item count.
- **`dupesrate` is a ratio, so it cannot be summed or averaged across jobs.** To aggregate several
  jobs of the same source (e.g. one consumer job per crawl over a quarter), sum `urls/issued/<source>`
  and derive whatever rate you need from those sums — the per job ratios are only comparable
  individually.

### Log lines: the timestamped alternative to the stats

Every stat above is **cumulative over the job's whole life**, which is a problem for a long-running
issuer (a deduplicator can run for weeks): a job that spans two reporting windows gives you one number
covering both. The `INFO` log lines are the way out, since they are timestamped — and the storage logs
API filters them server-side, so it costs one fast call, not a full log scan:

```python
logfilter = json.dumps(["message", "matches", [f"Read [0-9]+ records from .*/{source}_[0-9]"]])
for logline in script.get_job(jobkey).logs.iter(filter=logfilter, startts=since_epoch_ms, count=1):
    ...
```

The per-input cycle, in order (`<source>` appears in the batch file names):

| Line | Emitted by |
| --- | --- |
| `File <src> downloaded to <local>.` | the storage helper, fetching the input batch |
| `Read <n> records from <input file>.` | `process_input()` — proof that the input **was processed**, at that time |
| `File <local> uploaded to <dest>.` + `Wrote <n> records to <dest>` | `send_file()` — only when something was actually issued |
| `Deleted <input file>.` | retiring the consumed input |

Hence the useful inference: **an input that was read and then deleted, with no `Wrote` line (and no
`urls/issued/<source>` stat), was entirely deduplicated away** — 100% dupes. Without those lines, "no
`issued` stat" is ambiguous: it looks identical to "this issuer has not reached that input yet". Pair
the lines **by input file path** and require the `Deleted` one: an input is retired only once it is
fully processed, so a read without a later deletion is an input whose job died mid-way — it will be
read again, and it must not be counted as processed.

## Footguns

- The default loop checks `get_project_running_spiders(crawlmanagers=("py:crawlmanager.py",))` to
  decide when to flush a stopped source's partial batch — it assumes the upstream discovery crawl
  manager is named `py:crawlmanager.py`.
- Setting `LOAD_DELIVERED_IDS_DAYS` without calling `load_last_outputs()` in `__init__` raises in
  `on_start()`.
- Launch is synchronous: `Script().run()` (issuers are not async-launched).
- **The output queue buffers a whole batch.** An issuer accumulates a `(slot, source)` batch until it
  hits `default_filesize` (or, with `flush_on_each_input`, the end of the input). For big items × big
  batches (e.g. a metadata-heavy delivery, `flush_on_each_input=True` + a huge `default_filesize`) that
  batch can exceed the container memory → `close_reason: "killed by oom"`. Remedies: shrink each item,
  lower `default_filesize`, or set `persist_items_queue_on_disk=True` to keep the batch on disk.
- **NEVER iterate an on-disk bucket via `sqlitedict`'s `values()` / `items()` / `keys()`.** They look
  lazy but are not: each hands one whole-table `SELECT` to sqlitedict's writer thread, which pushes
  every row into an **unbounded** in-memory `Queue` with no back-pressure from the consumer — its own
  `select()` docstring says "the entire result will be in memory". So iterating a disk-backed batch that
  way holds all of it in RAM, defeating `persist_items_queue_on_disk` entirely and OOM-killing the job
  (measured: a 96 MB batch of 50 KB items → +80 MB resident via `values()`, +0 MB chunked). Read in
  chunks instead — `_iter_bucket_values()` does this, keyset-paginated by `rowid`, bounded by
  `items_queue_read_chunk_size`; use it for any new bucket iteration.
- Relatedly, count with `len(bucket)` (a `COUNT(*)`), **never** `len(bucket.keys())`: on an on-disk
  bucket `keys()` is a generator, so that raises `TypeError` — and it would read the whole table just to
  count it.

## Accumulate-then-merge (any issuer)

To combine an input's records into fewer output records: override `process_item` to **accumulate**
(don't call `super()`), then in `post_process_input_items` merge, set each combined record's
`id`/`input_source`, `issue_item()` them, and clear the accumulator; pair with `flush_on_each_input=True`
for one file per job. Generic to any `IssuerScriptWithSCJobInput` — not delivery-specific.

## Delivery — a configuration, not a class

No dedicated class: a delivery is an `IssuerScriptWithSCJobInput` configured for the terminal stage.
Typical: `close_on_no_inputs=True`, and `flush_on_each_input=True` + big `default_filesize` for one file
per job. `dedupe=False` / `set_item_source=False` apply **only** when the data was already
deduplicated/post-processed upstream (delivering a primary crawl keeps the defaults). Supply
`build_item_id` and `compute_destination_filename`. **Replaces** the deprecated
`shub_workflow.deliver.BaseDeliverScript` (now warns on instantiation) — see
[migrating-from-basedeliverscript.md](migrating-from-basedeliverscript.md). When migrating, set
`CONSUMED_TAG = "delivered"` (the old `DELIVERED_TAG`) or already-delivered jobs get re-delivered.
