# Patterns and skeletons

Each pattern, the wiki section that explains it, and the skeleton in [../examples/](../examples/) that
implements it. Skeletons are generic: rename (`myproject`, `mysite.com`, bucket paths), keep the
structure, and leave `TODO`s where a project decision is still open.

| Pattern | Wiki | Skeleton |
| --- | --- | --- |
| Shared helpers: frontier name, slot naming, storage paths | [Slot naming](https://github.com/scrapinghub/shub-workflow/wiki/Feeding-Spiders#slot-naming) | `common.py` |
| Spider fed with seeds (`seeds`, `seeds_source`, `seeds_fields`) | [The spider side](https://github.com/scrapinghub/shub-workflow/wiki/Feeding-Spiders#the-spider-side-receiving-seeds) | `seeded_spider.py` |
| Crawl manager reading seeds, per-source config, acquisition, split slots (ready-made `HCFSeedsCrawlManager`) | [The ready-made seeds crawl manager](https://github.com/scrapinghub/shub-workflow/wiki/Feeding-Spiders#the-ready-made-seeds-crawl-manager) | `seeds_crawlmanager.py` |
| Url frontier: batch files built by the crawl manager | [Url frontiers](https://github.com/scrapinghub/shub-workflow/wiki/Feeding-Spiders#url-frontiers-batch-files-built-from-the-frontier) | `url_batch_crawlmanager.py` |
| Consumer: harvest seeds, write the frontier, partition items | [Harvesting seeds](https://github.com/scrapinghub/shub-workflow/wiki/Closing-the-Loop#harvesting-seeds-from-the-items) | `seeds_consumer.py` |
| Per-slot persistent deduplicator | [Deduplicators](https://github.com/scrapinghub/shub-workflow/wiki/Post-Crawl-Data-Chain#deduplicators) | `slot_deduplicator.py` |
| Batch file queue, claim by move, fair rotation over sources | [Feeding spiders with batch files](https://github.com/scrapinghub/shub-workflow/wiki/Feeding-Spiders#feeding-spiders-with-batch-files) | `batch_crawlmanager.py` |
| Delivery issuer, one file per job | [Delivery is a configuration](https://github.com/scrapinghub/shub-workflow/wiki/Issuers#delivery-is-a-configuration) | `delivery.py` |
| Keep-alive scheduler | [The keep-alive scheduler](https://github.com/scrapinghub/shub-workflow/wiki/Keeping-It-Running#the-keep-alive-scheduler) | `keepalive_scheduler.py` |
| Back-pressure, slot reset, fatal outcomes | [Keeping It Running](https://github.com/scrapinghub/shub-workflow/wiki/Keeping-It-Running) | in `seeds_crawlmanager.py` (commented) |
| Short ordered sequence | [When to use a graph manager](https://github.com/scrapinghub/shub-workflow/wiki/Graph-Managers#when-to-use-a-graph-manager) | see the `shub-workflow-graph-managers` skill |

Every script must be registered in the project `setup.py` (`scripts=[...]`) to run on ScrapyCloud as
`py:<name>.py`. Filters, balancers and reducers are file input issuers with the shape of
`slot_deduplicator.py` (see the `shub-workflow-issuers` skill for their specifics).
