# Design document template

Fill it in with the developer, save it where they choose (e.g. `docs/architecture.md`), and keep it
updated as the interview goes on. Every table row must be concrete: a script name, a folder, a slot
name pattern — not "a storage".

```markdown
# <Project> — workflow architecture

## Requirements
- Trigger: continuous | on demand | periodic | once — <details>
- Units of work: seeds (<kinds>) | urls | input files — <details>
- Delivery: <what, where, format, guarantee: each item once / snapshot per run>
- Volumes (orders of magnitude): <items/day, history, seeds>
- Sources: <how many, how similar>; politeness: <limits>
- Freshness / recrawl: <requirement>

## Starting point
Reference architecture <A|B|C|D|none>, adapted as follows: <differences>.

## Roles
| Component (script) | Role | Base class | Reads | Writes | Instances | Started by | Mode |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `py:crawlmanager.py <spider>` | feed spiders with seeds | GeneratorCrawlManager | slot `<source>_<kind>` | spider jobs | one per spider | keep-alive scheduler | continuous |
| ... |

## Storage contracts
| Storage | Writer (single) | Readers | Contents and naming |
| --- | --- | --- | --- |
| frontier `<name>`, slot `<source>_<kind>` | consumer | crawl manager | fp = <seed/url>, qdata = <...> |
| `gs://<bucket>/<stage>/` | <issuer> | <next stage> | `<slot>_<source>_<timestamp>.jl.gz` |
| job tag `<TAG>` | <script> | <script> | consume-once marker |

## Self-resuming
| Component | What a new instance needs | Where it comes from |
| --- | --- | --- |
| crawl manager | running jobs of its spider; pending seeds | `acquire_all_jobs` + `get_owned_jobs()` per spider; the frontier |
| ... |

## Scheduling
- Keep-alive scheduler (every <period>): <components and their instances>
- Periodic jobs: <component: period>
- Graph managers (if any): <sequence, trigger>

## Control knobs
| Knob | Where | Default | Effect |
| --- | --- | --- | --- |
| running jobs per spider | project setting `<SPIDER>_RUNNING_JOBS` | <n> | 0 pauses the source |
| seeds per job | `ENABLED_SPIDERS[<spider>]["SEEDS_PER_JOB"]` | <n> | job duration |
| ... |

## Back-pressure and recrawling
<which queue gates which stage, threshold and why; slot reset / sweeps policy>

## Monitoring
<monitor period, aggregated stats, alerts>

## Pitfalls reviewed
<items of the checklist that applied, and what changed because of them>

## Assumptions
- <assumption made where the developer didn't answer — to confirm>

## Open questions
- <questions that don't change the design now, to settle later>
```
