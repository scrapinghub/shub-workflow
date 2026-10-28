# The ready-made seeds crawl manager: `HCFSeedsCrawlManager`

Before hand-writing a crawl manager that feeds a spider from its slot, subclass
`HCFSeedsCrawlManager` (`shub_workflow.utils.hcf`). One instance per spider (`py:crawlmanager.py <spider>`); it
reads (and deletes) seed batches, schedules a job per `SEEDS_PER_JOB` chunk with the seeds as a json
list in the `seeds_argument` spider arg (default `"seeds"`), ends when the slot is empty (a scheduler
restarts it), and owns every job of its target spider (`acquire_all_jobs` + `get_owned_jobs()` by
spider; no finished jobs acquired).

```python
from shub_workflow.script import SpiderName
from shub_workflow.utils.hcf import HCFSeedsCrawlManager

class CrawlManager(HCFSeedsCrawlManager):
    name = "crawlmanager"
    loop_mode = 60
    ENABLED_SPIDERS = {   # Dict[SpiderName, SeedsSpiderConfig]; subclass the TypedDict to add keys
        SpiderName("mysite.com_tags"): {"SEEDS_PER_JOB": 10, "RUNNING_JOBS": 4},
        # RUNNING_JOBS None/absent: read <SPIDER>_RUNNING_JOBS from SC settings (0 pauses); SC_UNITS: job units
        SpiderName("mysite.com_authors"): {"SEEDS_PER_JOB": 100, "SC_UNITS": 2},
    }
```

- A spider missing from `ENABLED_SPIDERS` is an argument error at start.
- Frontier: `frontier_name` attr, else the `FRONTIER_NAME` project setting.
- Slot: `get_base_slotname()` = `<canonical name>_<seeds_source>` (or the canonical name if the spider
  has no `seeds_source`) — the consumer must write with the same naming.
- **Split slots:** `SLOT_SPLITS = {"<canonical name>": n}` reads `<base>`, `<base>_1` … `<base>_<n-1>`
  in rotation; each read starts after the last slot read and skips empty ones, so the generator ends
  only when all are empty.
- Hooks: `filter_out_seed(seed, qdata)` (default keeps every seed), `requested_seeds()`
  (default `SEEDS_PER_JOB × max_running_jobs`), `process_seeds(seeds)` (transform/fan out before
  chunking), `build_schedule_args(seeds)` (add spider args, job settings, tags), `on_empty_frontier()`
  (e.g. reset the slot and schedule a seed job); `max_running_jobs` is a property (add back-pressure).
- `fatal_outcomes`: spider close reasons that stop the crawl (added to `failed_outcomes`).
- Stats: `hcf/batches/<slot>/read`, `hcf/seeds/<slot>/filtered`.

Wiki: [The ready-made seeds crawl manager](https://github.com/scrapinghub/shub-workflow/wiki/Feeding-Spiders#the-ready-made-seeds-crawl-manager).
