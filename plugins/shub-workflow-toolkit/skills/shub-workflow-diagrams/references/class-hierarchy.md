# Class hierarchy diagrams: the conventions

One standard for every shub-workflow project, so a reader who knows one project's diagram can read
any other's. The template, [../examples/class_hierarchy_gen.py](../examples/class_hierarchy_gen.py),
already follows all of it: copy it, replace the model, keep everything else.

## What goes in

- **Every script class the project defines**: the classes in its scripts package, deployable or
  local-only (analysis helpers, report tools, `ScanJobs` subclasses). They are what the diagram is for.
- **The project's own mixins and intermediate bases**: its script mixin, its base issuer mixin, a
  base crawl manager that per-variant managers extend.
- **The base spiders, not the per-source ones**: the project's base spider classes (and their
  intermediate bases, one per spider family), plus any standalone spider that is not per-source
  (a downloader). The per-source spiders all hang off those bases, are many, and churn — drawing them
  buries the scripts.
- **The shub-workflow and Scrapy classes the project's classes reach**, up to `BaseScript` /
  `scrapy.Spider`, with their real inheritance between them. The project's classes are mostly a
  handful of overrides on top of these, so without them a box says little about what its class does.
  Draw only the ones on a path from a project class; never the whole library.

## What stays out

- Items, `TypedDict`s, `IssuerItem` subclasses, item loaders, pipelines, middlewares, Spidermon
  monitors and suites, utility and model classes: they are not part of the script hierarchy.
- `Protocol`, `Generic`, `ABC` and `object` bases, and the type parameters of a generic base
  (`IssuerScriptWithSCJobInput[MyItem]` is drawn as `IssuerScriptWithSCJobInput`). Many shub-workflow
  mixins (the HCF seeds writer, the SES mail sender, the spider stats aggregator, the async scheduler)
  derive only from protocols, so they are drawn standalone, with no edge above them.
- Files the repository does not track.

## The boxes

- **Each box lists only the members its class ADDS** — methods it defines or overrides, attributes it
  introduces or sets to something that matters. Never repeat inherited members: the edge says them.
- A member may carry a short parenthesized gloss of what it does here when the name alone does not
  tell — `set_parameters_gen()  (claim a batch)`, `max_running_jobs  (monthly targets)` — two spaces
  before the parenthesis.
- A plain phrase is fine where a behaviour spans several members (`one job per slot`,
  `bloom filter over past outputs`, `speed control  (…)`). `∗` marks an abstract member.
- Keep a box to about six lines; past that, group (`get_<stage>_output_folder()  (one per stage)`)
  or end with `… alert hooks`.
- The label is the class name. For a family of long subclass names under one obvious base, an
  ellipsis prefix (`…CrawlManagerSoW5`) keeps the row narrow; the node id stays the full name.

## Categories and colours

The `GROUPS` table of the template is the standard: the same six categories and fills in every
project. Leave out a category the project has no class in (the legend follows); never add a new
colour for one project.

| Group | For |
| --- | --- |
| `ext` | shub-workflow / Scrapy classes, including their mixins |
| `mixin` | the project's own mixins |
| `manager` | crawl managers |
| `issuer` | issuers: consumers, filters, deduplicators, balancers, reducers, issuer deliveries |
| `script` | other scripts: scheduler, monitor, frontier tool, report and analysis tools, `ScanJobs` |
| `spider` | spiders |

## The edges

- **Solid (`ext`) from the substantive base; dashed (`mix`) from every mixin**, wherever it sits in
  the bases list. A project mixin that comes first in the list is still dashed: what the class *is*
  comes from the base it extends.
- Edges are UML generalization: a hollow triangle at the base end, the base ranked above.
- **Node ids are the class names exactly as the project's code writes them** in a bases list,
  import aliases included (a `ScanJobs` subclass whose base is imported as `ShubScanJobs` gets the id
  `ShubScanJobs` and the label `scanjobs.ScanJobs`). That is what lets the render check the model.

## The drift check

`SOURCES` lists the project files whose classes are drawn: the scripts package and the module(s) of
the base spiders. Every render parses them and compares their declared bases with `EDGES`, and exits
non-zero naming each difference:

- a drawn class with a base that is not drawn, or whose edge is missing;
- an edge the code does not declare (the class was moved onto another base);
- a drawn project class that no longer exists;
- a class in `SOURCES` that derives from a drawn class but is not drawn itself.

So a diagram cannot silently go stale: a refactor that moves a class onto a new shub-workflow base,
a newly mixed-in shub-workflow mixin, a removed script — the next render names it. The check covers
the edges only; whether the members listed in a box are still accurate is still read from the code.

## When to re-render

Whenever a script class is added, removed or moves to another base — including when a shub-workflow
upgrade replaces project code with a library class or mixin. Run the generator as part of that
change, fix the model until the check passes, look at the PNG, and commit the model, the `.dot` and
the PNG together.

## Files

- `diagrams/class_hierarchy_gen.py` — the model and renderer (self-contained; no engine needed).
- `diagrams/class_hierarchy.dot` — written by the render, beside the generator.
- `class_hierarchy.png` — written at the repository root, referenced from the README's diagrams
  section with one line on what it shows and that it is generated (edit the model, never the outputs).
