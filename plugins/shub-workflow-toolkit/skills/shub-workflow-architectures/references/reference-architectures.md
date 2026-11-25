# Reference architectures

Five proven starting points, from the wiki chapter
[Reference Architectures](https://github.com/scrapinghub/shub-workflow/wiki/Reference-Architectures)
(with diagrams). Pick the closest one and adapt it; they combine freely.

| The requirements say | Start from |
| --- | --- |
| a fixed set of sites (one spider each), crawled periodically, each delivered on its own | A |
| crawls on demand, each one a unit with its own delivery | B |
| continuous discovery of new items, delivered as they are found | C |
| items selected by customer criteria, and files downloaded | D |
| items extracted from their own pages, or items with versions | E |
| a single crawl now and then, or a fixed list of input files | none: a generator crawl manager (+ a delivery if needed) |

## A. Periodic crawl of many independent spiders

Hundreds of unrelated sites, one spider each, recrawled every day; nothing crosses jobs and nothing
is discovered. The project's spider list *is* the work list.

- **One generator crawl manager**, started daily by a periodic job: `set_parameters_gen()` yields
  `{"spider": name}` for every spider of `self.spider_loader.list()` not carrying a `deactivated` tag
  in ScrapyCloud (`spider = SpiderName("")`). It finishes when the generator is exhausted and the
  last job is done; adding a site = deploying its spider. `MAX_RETRIES` retries failed outcomes. If a
  run can outlast the period, `acquire_all_jobs = True`.
- **`default_max_jobs` above the project's units** (e.g. 16 for 8): the surplus waits in the
  ScrapyCloud pending queue, so a freed unit is taken at once instead of at the manager's next loop.
- **Spiders deliver directly** (one file per spider, a feed export enabled only in production) and
  check their own item quality (e.g. Spidermon: minimum items, drop vs previous job, field coverage,
  schema), alerting per job.
- **Periodic broken spiders check** (`BaseScript`, e.g. with `SlackMixin`; period per the project,
  daily or weekly): a spider whose last N finished jobs (e.g. 7) all have zero items gets the
  `deactivated` tag and a notification; the crawl manager then skips it. Removing the tag in the UI after the fix re-enables it. The tag is the
  maintainers' to-do list (a third or more of the spiders can be deactivated at a time).

## B. Request-driven batch crawling

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

## C. Self-replenishing discovery loop

Continuous discovery from many sources through branch seeds; deliver each new item once.

- Crawl managers, one per spider, read seeds and adapt speed to the site errors.
- Spiders with cross seeding (each reads one kind of seed, produces several).
- **One consumer** for all sources (`class:` target): writes seeds back, partitions items into N slots.
- **N deduplicators** (indicatively 10, ~1B-item bloom each), reloading years of output; their output
  *is* the delivery.
- Hourly keep-alive scheduler; daily monitor.

## D. Discovery, filtering, download and delivery

C plus customer filters, downloads and a volume target.

- One consumer per source; the biggest sources with split slots (N consumers taking jobs by job
  number modulo N).
- Filters (one per slot, filters as a data file, applied by restarting) **before** deduplicators.
- A balancer regrouping per source (and filter) into a download queue, batch size per source.
- Downloader managers claiming one batch per source in turn, with a ban-rate jail and a monthly target;
  they can schedule in another project (`--project-id`) when the downloader needs a different runtime.
- A periodic delivery reading downloader jobs, one file per job, writing recoverable failures back as
  retry batches.
- Monitor hooks as back-pressure (flush a permanent spider, reset a slot when a queue runs low).

## E. Two-tier discovery and extraction

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

D and E extend C's loop. Any of them can add a B-like request flow for special crawls, a weekly graph
manager for reports, or A's broken spiders check when it runs many spiders of its own. A grows into B
when the crawls must be requested on demand instead of run every day. A url-heavy source can use url
batch files built from the frontier while the others use seeds.
