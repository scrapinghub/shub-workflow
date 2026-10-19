---
name: shub-workflow-hcf
description: >-
  Use when working with a ScrapyCloud Hubstorage Crawl Frontier (HCF) from a shub-workflow project —
  building or using a frontier tool script on top of `shub_workflow.utils.hcf.FrontierToolMixin` (list
  slots, count, dump, add seeds, delete batches or slots, move a slot into another), installing the
  `with-hcf-backend` extra, or writing crawl-manager / consumer code that reads seeds from a frontier
  slot or writes new seeds into it through `hcf_backend`'s `HCFManager`. Covers the HCF semantics that
  bite (per-slot fingerprint memory, single writer per slot, read does not remove), the tool's commands
  and extension points, and the read → delete → schedule and add → flush seed patterns.
---

# HCF frontier tools (`shub_workflow.utils.hcf`)

[`shub_workflow/utils/hcf.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/utils/hcf.py)
provides `FrontierToolMixin`, a ready-made command line tool for inspecting and handling the slots of
a crawl frontier. It wraps the optional [`hcf-backend`](https://github.com/scrapinghub/hcf-backend)
package (`HCFManager` for queue reads/writes, `HCFPal` for slot listing/counting/dumping).

```
pip install shub-workflow[with-hcf-backend]
```

`hcf_backend` is imported lazily: without it, the module imports fine and only fails, with an
install hint, when a frontier operation is first used.

## HCF semantics you must keep in mind

- **Frontier → slots → batches.** A project has frontiers (by name), each split into slots created on
  the fly by writers. Seeds (`{"fp": <seed>, "qdata": {...}}`) are stored in batches.
- **Reading does not remove.** A read returns batches; they stay until their ids are deleted.
- **Per-slot fingerprint memory.** A slot discards any fp it has already seen, *even after the batch
  holding it was deleted*. So re-adding a consumed seed is a silent no-op, and a slot "drains" for
  good; deleting the slot is the only way to reset that memory (i.e. to re-crawl everything).
- **One writer per slot at a time.** Never add or move seeds into a slot another process (typically a
  consumer) is writing to.

## Building the tool

```python
# scripts/frontier_tool.py  (register it in setup.py `scripts=[...]`)
import logging

from shub_workflow.utils.hcf import FrontierToolMixin


class FrontierTool(FrontierToolMixin):      # combine with your project base-script mixin if you have one
    frontier_name = "myfrontier"            # if omitted, --frontier becomes required


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    FrontierTool().run()
```

| Command line | Effect |
| --- | --- |
| `frontier_tool.py --list` | list the frontier slots |
| `frontier_tool.py <slot>` | dump up to `--max-seeds` (default 100) seeds, without removing them |
| `frontier_tool.py <slot> --count` | count the requests in the slot |
| `frontier_tool.py <slot> -a s1 -a s2` | add seeds (repeatable) |
| `frontier_tool.py <slot> --delete-batch <id>` | delete batch ids (repeatable) |
| `frontier_tool.py <slot> --delete-slot` | delete the slot (resets its fingerprint memory) |
| `frontier_tool.py <slot> --move <target>` | move all seeds, with qdata, into the target slot |

Plus the usual script options (`--frontier`, `--project-id`, ...). Class attributes:
`add_seeds_flush_size` (1000), `move_read_size` (1000). A move deletes each source batch only after
its seeds were flushed into the target, so an interrupted move can be re-run; seeds already seen by
the target are discarded.

**Extending:** add your option in `add_argparser_options()` (always call `super()`), and override
`run()`, handling your option and falling back to `super().run()`. Reuse the handling methods —
`add_seeds(slot, seeds, qdata=None)`, `move_slot(src, dst)`, `delete_batches(slot, ids)`,
`delete_slot(slot)`, `count_slot(slot)`, `list_slots()`, `dump_slot(slot, max_seeds)` — and the
`hcf_manager` / `hcfpal` properties:

```python
class FrontierTool(FrontierToolMixin):
    frontier_name = "myfrontier"

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("--add-range", nargs=3, type=int, metavar=("START", "STOP", "STEP"))

    def run(self):
        if self.args.add_range and self.args.slot:
            self.add_seeds(self.args.slot, (str(n) for n in range(*self.args.add_range)))
        else:
            super().run()
```

The mixin is a full `BaseScript` with its own CLI, so it is meant for tool/maintenance scripts — do
**not** mix it into a crawl manager or an issuer (its positional `slot` would clash with theirs).
Those use `HCFManager` directly, as below.

## Seeds in workflow components

A crawl manager reads seeds and passes them to spiders as arguments; a consumer (the single writer)
writes new seeds back. Both with `hcf_backend.manager.HCFManager(frontier, project_id=...)`:

```python
# crawl manager, inside set_parameters_gen(): read -> delete -> schedule
seeds = []
for batch in self.hcf.read(slot, seeds_wanted):          # batches with at least seeds_wanted requests
    seeds.extend(fp for fp, _qdata in batch["requests"])
    self.hcf.delete(slot, [batch["id"]])                  # deleted now: a failed job loses its seeds
for i in range(0, len(seeds), seeds_per_job):
    yield {"seeds": json.dumps(seeds[i: i + seeds_per_job])}

# consumer, e.g. in post_process_input_items(): add -> flush
for seed in new_seeds:
    self.hcf.add_request(slot, {"fp": seed, "qdata": {}})
self.hcf.flush(slot)
```

`hcf_backend.manager` logs routine frontier operations; silence it in your project package `__init__.py`:
`logging.getLogger("hcf_backend.manager").setLevel(logging.ERROR)`.

For the crawl manager and issuer sides, see the `shub-workflow-crawl-managers` and
`shub-workflow-issuers` skills.
