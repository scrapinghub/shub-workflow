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

## Methods

| Method | Required? | Purpose |
| --- | --- | --- |
| `build_item_id(item) -> ItemId` | **abstract** | dedup/identity key. |
| `adapt_input_item(raw) -> ITEMTYPE` | optional | adapt raw records to your item type (default casts). |
| `process_item(item, input_source)` | optional override | dedup + enqueue; override to filter/transform (call `super()`), or to *accumulate* (delivery pattern — don't call super). |
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
`on_close` flushes remaining queues; an input is retired only after all its items are written.

## Footguns

- The default loop checks `get_project_running_spiders(crawlmanagers=("py:crawlmanager.py",))` to
  decide when to flush a stopped source's partial batch — it assumes the upstream discovery crawl
  manager is named `py:crawlmanager.py`.
- Setting `LOAD_DELIVERED_IDS_DAYS` without calling `load_last_outputs()` in `__init__` raises in
  `on_start()`.
- Launch is synchronous: `Script().run()` (issuers are not async-launched).

## Delivery

Build delivery as an issuer (`IssuerScriptWithSCJobInput`, `dedupe=False`, `close_on_no_inputs=True`,
accumulate in `process_item`, merge+issue+`flush_files` in `process_input`). This **replaces** the
deprecated `shub_workflow.deliver.BaseDeliverScript` (now warns on instantiation).
