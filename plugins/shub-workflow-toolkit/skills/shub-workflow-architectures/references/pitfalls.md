# Pitfalls checklist

Check every design against each item and say which ones applied. Each pitfall has the rule that
avoids it.

## Frontier

- **Two writers on one slot.** HCF accepts one writer per slot at a time. *Rule:* name the single writer
  of every slot in the design document. Splitting work means splitting slots, not sharing them.
  Manual seeding with the frontier tool counts as writing: only while the writer is stopped.
- **Re-adding consumed seeds does nothing.** A slot remembers every fingerprint it received, even
  after deletion. *Rule:* recrawl = delete the slot and bootstrap again; failed-job recovery = retry
  the job (`MAX_RETRIES`, same arguments) or write into a different (retry) slot.
- **Seeds lost on failure.** A crawl manager that deletes on read loses the seeds of a failed job.
  *Rule:* decide explicitly — accept it (self-feeding crawls usually can), or retry jobs, or use url
  batch files (moved back to pending on failure).
- **Draining slots look like a broken crawl.** Each seed runs once, so output declines as slots empty.
  *Rule:* plan recrawling (automatic reset conditioned on downstream demand, or periodic sweeps) and
  monitor slot counts.
- **Resetting a leaf slot** re-extracts every item ever found. *Rule:* never reset leaf slots.

## Managers and scheduling

- **Auto-scheduled managers that don't acquire** start blind and double the running jobs. *Rule:*
  `acquire_all_jobs = True` (+ `dont_acquire_finished_jobs = True` unless finished jobs are needed).
- **Instances adopting each other's jobs.** Acquisition matches by name; one class run per spider
  shares the name. *Rule:* override `get_owned_jobs()` to the target spider.
- **Deferring after claiming.** `can_schedule_job_with_params()` runs after the generator yielded; if
  yielding moved a file or deleted a batch, a deferred job strands it in memory. *Rule:* decide before
  claiming, in the generator.
- **Two instances of one role.** Nothing in the library prevents it. *Rule:* the keep-alive scheduler
  checks pending and running jobs by command line; manual starts only with the scheduled one stopped.
- **Stateful components.** *Rule:* for each role, name what a new instance needs and where it lives
  (see the self-resuming table in the design template).

## Data chain

- **Filtering after deduplication** makes filtered items unrecoverable when filters change. *Rule:*
  filters before deduplicators.
- **Heavy dedup in the consumer** makes every consumer restart reload the whole history, and caps dedup
  at one process. *Rule:* cheap consumer + per-slot persistent deduplicators.
- **Deduplicators forgetting what moved downstream.** *Rule:* `load_last_outputs()` includes the
  folders where items go after the deduplicator (download/processing, reducer output).
- **Batch names without a designation.** `load_last_outputs()` dates files by `_<timestamp>` and skips
  names starting with the bare timestamp. *Rule:* names start with a source or slot designation.
- **Stopped-spider flush tied to a script name.** *Rule:* set the issuer `source_crawlmanagers` to the
  real crawl manager scripts.
- **Whole batches in memory.** Big items × big batches OOM the issuer. *Rule:* smaller batches,
  smaller items, or `persist_items_queue_on_disk`.
- **Retry loops for permanent errors.** *Rule:* only add download retries when failures are frequent,
  and classify errors carefully (unrecoverable errors must not be requeued).

## Jobs and operations

- **Jobs too short** (startup overhead, thousands of tiny jobs burning seeds when the spider breaks) or
  **too long** (items and seeds reach the workflow only when the job finishes; monitors only see
  finished jobs). *Rule:* several minutes to about an hour; seeds per job is the knob; cap with
  `CLOSESPIDER_*` settings for uneven seeds.
- **Spiders that never finish** (unbounded login retries, permanent spiders). *Rule:* bound retries;
  stop permanent spiders periodically so their items flow.
- **Back-pressure on the wrong queue.** *Rule:* throttle to protect the stage where waiting loses data,
  with thresholds from measurements.
- **Configuration needing deploys.** *Rule:* knobs that change often (running jobs per source) in
  ScrapyCloud project settings, read each loop; `0` pauses a source.
- **Brownfield migrations re-delivering.** *Rule:* preserve consume tags (`CONSUMED_TAG`) and output
  naming when replacing a component.
