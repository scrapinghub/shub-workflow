# Reference architectures

Four proven starting points, from the wiki chapter
[Reference Architectures](https://github.com/scrapinghub/shub-workflow/wiki/Reference-Architectures)
(with diagrams). Pick the closest one and adapt it; they combine freely.

| The requirements say | Start from |
| --- | --- |
| crawls on demand, each one a unit with its own delivery | A |
| continuous discovery of new items, delivered as they are found | B |
| items selected by customer criteria, and files downloaded | C |
| items extracted from their own pages, or items with versions | D |
| a single crawl now and then, or a fixed list of inputs | none: a generator crawl manager (+ a delivery if needed) |

## A. Request-driven batch crawling

Requests (files listing sites) arrive at any time; each is crawled and delivered as a unit.

- **Intake:** a periodic poller of a requests folder; a scheduler that claims one request file by
  moving it to `processing/`. The file name is the **request id**, propagated to the frontier slot, a
  tag on every job, and the delivery folder.
- **Per request:** a short **graph manager** — task 1 loads the rows into a frontier slot named by the
  request id (deleting the slot first, so re-running is harmless), task 2 is a crawl manager that
  drains the slot (one job per site, deferred jobs to keep one job per site at a time). On close it
  moves the request to `completed/` and notifies.
- **Spiders deliver directly** through item pipelines (no cross-job processing) and write a done
  marker per site.
- **Around it, independent agents:** a periodic stats collector (consume-once tag) keeping a run history
  per site — also used to give incremental crawls the urls already delivered —, a weekly alerts graph
  manager, a watchdog.

## B. Self-replenishing discovery loop

Continuous discovery from many sources through branch seeds; deliver each new item once.

- Crawl managers, one per spider, read seeds and adapt speed to the site errors.
- Spiders with cross seeding (each reads one kind of seed, produces several).
- **One consumer** for all sources (`class:` target): writes seeds back, partitions items into N slots.
- **N deduplicators** (indicatively 10, ~1B-item bloom each), reloading years of output; their output
  *is* the delivery.
- Hourly keep-alive scheduler; daily monitor.

## C. Discovery, filtering, download and delivery

B plus customer filters, downloads and a volume target.

- One consumer per source; the biggest sources with split slots (N consumers taking jobs by job
  number modulo N).
- Filters (one per slot, filters as a data file, applied by restarting) **before** deduplicators.
- A balancer regrouping per source (and filter) into a download queue, batch size per source.
- Downloader managers claiming one batch per source in turn, with a ban-rate jail and a monthly target;
  they can schedule in another project (`--project-id`) when the downloader needs a different runtime.
- A periodic delivery reading downloader jobs, one file per job, writing recoverable failures back as
  retry batches.
- Monitor hooks as back-pressure (flush a permanent spider, reset a slot when a queue runs low).

## D. Two-tier discovery and extraction

Items must be extracted from their own pages; items change over time.

- Discovery spiders produce branch seeds (loop back) and **leaf seeds** (item ids) for extraction slots.
- The discovery consumer only writes seeds.
- Extraction spiders read leaf seeds; leaf slots are **never reset** (they are the "already extracted"
  memory).
- Extraction consumer (filters, ids) → deduplicator (reloads own output + downstream folders) →
  optional reducer (versioned ids, keep the best version) → download queue → downloader manager
  (claim by move, one job at a time for logged-in sources) → delivery joining items with their files.
- Discovery throttled by download-queue depth; automatic discovery slot reset when empty and the queue
  is low; periodic sweeps started by the scheduler for sources that need them.

## Combining

C and D extend B's loop. Any of them can add an A-like request flow for special crawls, or a weekly
graph manager for reports. A url-heavy source can use url batch files built from the frontier while
the others use seeds.
