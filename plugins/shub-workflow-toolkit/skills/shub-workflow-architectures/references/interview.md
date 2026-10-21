# Interview question bank

Ask in rounds of two to four questions, starting with round 1. Offer options with a recommended one
where possible, and skip what the repository or earlier answers already settle. Each question lists
what its answer decides, so you know when a question can be dropped (it decides nothing in this
project) and when it can't be (it decides the shape).

## Round 1 — the shape (always ask)

**1. How is crawling triggered?**
- Continuously: discover new data forever, as the sources publish it.
- On demand: customers (or people) submit requests, each one crawled as a unit.
- Periodically: full recrawls of the same targets every day / week / quarter.
- Once: a one-off crawl.

*Decides:* continuous → independent agents with a frontier (architectures B, C, D). On demand → a
request file queue with a short per-request flow (architecture A). Periodic → periodic jobs or
scheduler-driven sweeps. Once → probably just a generator crawl manager, maybe nothing else.

**2. How do the spiders reach the targets?**
- From short keys: search keywords, categories, tags, usernames, ids (→ seeds).
- From urls that can't be reduced to a key (→ url batch files built from the frontier).
- From a list the customer provides (→ input files, no frontier writer).
- From a single entry point, exploring everything (→ a broad crawl; consider a seeder or sitemaps).

*Decides:* what the frontier holds (seeds, urls or nothing), and how the crawl manager passes work to
the spiders (JSON argument or batch file path).

**3. Do the pages reveal new starting points?** (a tag page lists articles with other tags; a product
lists its seller)

*Decides:* whether the crawl feeds itself — a consumer writing seeds back to the frontier — and which
item fields carry seeds (`seeds_fields`), including cross seeding between spiders.

**4. What is delivered, where, and with which guarantee?**
- Every item once, ever (→ persistent deduplication across the whole history).
- A complete snapshot per run (→ per-run delivery; dedup within the run only, or none).
- Files besides the data (images, videos, documents) (→ a download stage).
- Format/location constraints (bucket, paths, file naming, one file per job/request).

*Decides:* whether there is a post-crawl data chain at all, the deduplicator design, and the delivery
configuration. If nothing crosses jobs, spiders delivering through item pipelines may be enough.

## Round 2 — the data

**5. What identifies an item, and does it change over time?** A stable id (url, product id)? Are there
versions (more images later, edited content) and which one is wanted?

*Decides:* `build_item_id()`; versioned ids + a reducer when only the best/latest version is wanted.

**6. Is everything on the listing, or must each item be extracted from its own page?**

*Decides:* single-tier spiders, or two-tier discovery (branch seeds → leaf seeds) + extraction
(architecture D). Leaf slots double as the "already extracted" memory.

**7. Are there customer filters?** Which criteria, and how often do they change?

*Decides:* a filter stage before the deduplicators; filters as data (a file) applied by restarting
the stage, rather than as code.

**8. Volumes.** Items per day, total history to deduplicate against, number of seeds, batch sizes.
Rough orders of magnitude are enough.

*Decides:* number of slots / deduplicators (`parallel_outputs`), bloom capacity (`MAX_ITEMS`),
`LOAD_DELIVERED_IDS_DAYS`, units per job, one consumer or several.

**9. Downloads (if any).** Expected failure rate? Recoverable errors (timeouts, bans) vs permanent
ones? Special runtime needs (custom libraries or models not in the standard image)?

*Decides:* a retry-batches loop in the consumer of the download stage (only if failures are
frequent); a balancer for per-source batch sizes; a separate project/image only when the runtime
needs it.

## Round 3 — the sources and the sites

**10. How many sources, and how similar?** One spider per site, families of spiders, or one broad
spider receiving the site as an argument?

*Decides:* one crawl manager per spider vs one per family; per-source configuration in one
`ENABLED_SPIDERS` dict; `canonical_name` and slot naming.

**11. Politeness and blocking.** Limits per site (one job at a time)? Known rate limiting or bans?
Logins or sessions (often: one job at a time per account)? Busy hours to avoid?

*Decides:* deferred jobs (`can_schedule_job_with_params`) for per-site limits; governors reacting to
error rates; time-of-day factors; per-source job caps.

**12. Freshness and recrawling.** Must new content be found quickly? Is a periodic full sweep
required? Is "each seed once" acceptable?

*Decides:* automatic slot reset (when empty and downstream is hungry) vs periodic sweeps started by
the scheduler vs a fixed schedule of seed jobs. Never reset leaf slots.

## Round 4 — operations

**13. Who operates it, and how?** Expected alerts and reports, response time, how often the
configuration changes.

*Decides:* the monitor (periodic, aggregated stats, alerts); which knobs live in ScrapyCloud project
settings (changeable without deploy) vs code.

**14. Environments and limits.** Dev and prod projects? Unit budget? Other projects involved?

*Decides:* `--project-id` usage, how many continuous components can afford to run, job sizes.

**15. Existing system (brownfield only).** Is there a central graph manager or Frontera-based
spiders today? What must not change (delivery paths and names, consumed/delivered tags, frontier
contents)?

*Decides:* a migration plan stage by stage; preserve consume tags (e.g. `CONSUMED_TAG`) and output
naming so nothing is re-delivered or lost.

## Playing back

After each round, summarize in a few lines: what you understood, the decisions it implies, and the
assumptions you are making for what was not answered. Ask the developer to confirm or correct before
the next round.
