# Migrating a delivery from BaseDeliverScript to an issuer

`shub_workflow.deliver.BaseDeliverScript` is **deprecated** (instantiating it emits a
`DeprecationWarning`). Rebuild the delivery as an `IssuerScriptWithSCJobInput`.

**There is no dedicated delivery class.** A delivery is just an `IssuerScriptWithSCJobInput` you
configure for the terminal stage — the base class already provides everything. Which attributes you
set is **use-case dependent**, so decide each one deliberately rather than copying a fixed recipe.

## Choosing the configuration

| Attribute | Set it when… | Leave default when… |
| --- | --- | --- |
| `close_on_no_inputs = True` | you deliver the finished jobs and stop (the usual case). | you want a continuously-running delivery (`loop_mode`). |
| `flush_on_each_input = True` + large `default_filesize` | you want **one delivery file per scanned job**. | you batch several jobs into one output file. |
| `dedupe = False` | the delivered data was **already deduplicated upstream** (a post-processing pipeline: consumer → deduplicator → … → delivery). | you deliver a **primary** spider's crawl directly and still want dedup. |
| `set_item_source = False` | the delivered spider is a **secondary/post-processing** stage whose items already carry their originating `source`. | you deliver a **primary** spider — `source` should be that spider's canonical name (the default). |

You always implement:

- **`build_item_id()`** — abstract; required even with `dedupe=False`, because `issue_item()` uses the
  id for output queueing/routing. Return a stable per-record id (or a constant if you fully bypass the
  default item flow).
- **the destination** — via `output_folder` and/or a `compute_destination_filename(output_slot, source)`
  override. With `flush_on_each_input=True` there's a per-job → per-file mapping; the path is often
  derived from the scanned job's spider args (read them in `post_process_input_items()`).

## Method / concept mapping

| BaseDeliverScript | Issuer (`IssuerScriptWithSCJobInput`) |
| --- | --- |
| `on_item(item, scrapername)` | `process_item(item, input_source)` — issue inline (default) or accumulate |
| `process_job_items(scrapername, job)` | the base `process_input()` reads the job; do per-job finalization in `post_process_input_items(spider_job, args)` (`spider_job` = the read job) |
| `fshelper.upload_file(...)` to a per-job path | `issue_item()` + `flush_files()` (via `flush_on_each_input=True`); path from `compute_destination_filename()` |
| `DEDUPE_KEY_BY_FIELDS` / `is_seen_item()` | `dedupe` + `build_item_id()` (bloom filter) |
| `scrapername` positional arg | `target` positional arg (`spider:<name>` / `canonical:<name>` / `class:<ClassName>`) |
| `DELIVERED_TAG` (`"delivered"`) marks done jobs | `lacks_tag=CONSUMED_TAG` selects; consumed jobs get `CONSUMED_TAG` (default `"CONSUMED=True"`) — **override `CONSUMED_TAG = "delivered"` on migration** (see gotchas) |
| `FLOW_ID` job selection | *(none by default — see the FLOW_ID gotcha)* |

## Gotchas

- **Preserve the old "delivered" tag (production-critical).** `BaseDeliverScript` tagged already-delivered
  jobs with `DELIVERED_TAG = "delivered"` to avoid re-reading them; the issuer instead selects jobs
  `lacks_tag=CONSUMED_TAG` and tags consumed ones with `CONSUMED_TAG` (default `"CONSUMED=True"`). If you
  deploy the migrated issuer with the default tag, it will **re-deliver every job the old script already
  delivered** (none carry `CONSUMED=True`). Set **`CONSUMED_TAG = "delivered"`** on the issuer so the old
  tags are honored and new deliveries keep tagging `"delivered"`.
- **Invocation changes.** `deliver.py <scrapername...>` → `deliver.py <type>:<name>` (e.g.
  `canonical:example_spider`). Update the periodic job / scheduler that launches it.
- **Conserve the source (the #1 subtle bug).** If the delivered spider is secondary, set
  `set_item_source=False`. Otherwise the base stamps `item["source"]` with the *scanned* spider's
  canonical name and clobbers the real upstream source that the delivery keys stats/paths on.
- **No FLOW_ID scoping.** The SC-job issuer selects **all** finished, un-`CONSUMED` jobs of the target.
  That's correct for a standalone-scheduled delivery (e.g. a Scrapy Cloud periodic job with no
  flow id). If you relied on per-workflow scoping (delivery scheduled by a graph manager), reintroduce
  it — e.g. add a tag filter by overriding `get_new_inputs()`.
- **Destination from job args.** Read the per-job destination (e.g. an `upload_prefix` arg) from
  `spider_job.metadata` (or `args[0]["spider_args"]`) in `post_process_input_items()`, stash it, and
  return it from `compute_destination_filename()`.
- **Serialize plain dicts.** If the delivered records are `scrapy.Item`s, convert them to `dict` before
  `issue_item()` — the writer `json.dumps()` them.
- **Stats aggregation is opt-in.** There is no built-in flag: mix in
  `shub_workflow.utils.monitor.SpiderStatsAggregatorMixin` and call `self.aggregate_spider_stats(...)`
  in `post_process_input_items()`. `get_new_inputs()` fetches only `spider_args` meta, so fetch the
  job's `scrapystats` yourself (via its metadata) when aggregating.
