---
name: shub-workflow-architectures
description: >-
  Use when a developer wants help designing (or redesigning) the architecture of a shub-workflow based
  Scrapy Cloud project — "help me design the architecture for this project", "how should I structure the
  crawl managers / consumers / delivery", "which components do I need", "how do I feed my spiders from a
  frontier", "how do I scale this crawl", "should I use a graph manager", or when migrating a project
  away from a central graph manager or from spiders that read/write the frontier (Frontera). Runs an
  INTERACTIVE design session: interviews the developer in short rounds to uncover the requirements
  they haven't stated yet, maps them to the independent-agents + role-separation architecture (or to
  a graph manager where a short ordered sequence fits), writes a design document with roles, storage
  contracts, scheduling and scale knobs, checks it against known pitfalls, and then scaffolds the
  script skeletons for the chosen roles. Covers seeds vs url frontiers, the post-crawl data chain,
  downloads, keep-alive scheduling, back-pressure and recrawling.
---

# Designing shub-workflow architectures

This skill turns a project's requirements into a workflow architecture, and then into script
skeletons. Its knowledge is the architecture described in Part III of the
[shub-workflow wiki](https://github.com/scrapinghub/shub-workflow/wiki/Independent-Agents-and-Role-Separation):

- **Independent agents.** Every component is scheduled on its own (a periodic job or a keep-alive
  scheduler), holds no state that can't be rebuilt from storage, and can be stopped at any moment.
- **Role separation.** Spiders only crawl (seeds or a batch file in their arguments); crawl managers
  read the frontier and schedule spiders; consumers read finished jobs, write new seeds back and
  partition the items; issuers process; a delivery delivers. Components talk only through storage
  contracts: a frontier slot, a folder + filename standard, a job tag.
- **Graph managers** only for short, strictly ordered sequences run as a unit (a per-request flow, a
  weekly report), never to coordinate the whole system.

## The session is a conversation, not a one-shot answer

Developers rarely arrive with complete requirements, and the decisions that shape the architecture
(continuous or on demand? seeds or urls? deduplication across runs? downloads?) are often ones they
haven't thought about yet. So:

1. **Never design from an unstated requirement silently.** Ask, or propose a default and record it
   explicitly as an assumption the developer can correct.
2. **Ask in short rounds** — two to four questions per round, multiple choice with a recommended
   option whenever possible (use your structured question tool if you have one). Start with the
   questions that change the shape of the architecture; leave tuning questions for the end.
3. **Play back what you understood** after each round, in a few lines, before moving on. A wrong
   understanding caught early costs one sentence; caught after scaffolding, it costs the scaffold.
4. **Stop interviewing when the open questions no longer change the design**: record the rest as
   open questions in the design document.

## Procedure

1. **Look before asking.** If there is a repository, read what exists: `setup.py` scripts, spiders
   (base classes, arguments), existing managers/scripts, `scrapinghub.yml`, storage paths in the
   settings. Don't ask what the code already answers; do ask whether what the code does is still
   what they want.
2. **Interview** using [references/interview.md](references/interview.md): the question bank, grouped
   by topic, with the design decision each answer drives. Round 1 is always the shape questions.
3. **Choose a starting point** with [references/reference-architectures.md](references/reference-architectures.md)
   (four proven architectures and how they combine), then adapt it to the answers.
4. **Write the design document** from [references/design-doc-template.md](references/design-doc-template.md):
   roles table, storage contracts, scheduling, scale and control knobs, assumptions, open questions.
   Ask where to save it (e.g. `docs/architecture.md` in the project). Review it with the developer
   and iterate.
5. **Check it** against [references/pitfalls.md](references/pitfalls.md) — every item, explicitly.
   Report what you changed because of a pitfall.
6. **Offer a diagram** of the design with the `shub-workflow-diagrams` skill.
7. **Scaffold on request.** Generate the skeletons of the chosen roles from [examples/](examples/)
   (index: [references/patterns.md](references/patterns.md)), renamed for the project, registered in
   `setup.py`, with `TODO`s where project decisions remain. For the details of each component, defer to
   the `shub-workflow-crawl-managers`, `shub-workflow-issuers`, `shub-workflow-hcf`,
   `shub-workflow-scripts` and `shub-workflow-monitors` skills rather than re-deriving them here.

## Rules that hold for every design

- **One writer per frontier slot**, always: the consumer (or one consumer per source / per split).
  Never the spiders, never a second process "just for bootstrapping" while the consumer runs.
- **Every component restartable from storage.** For each role, name what a new instance needs and where
  it comes from (frontier, folder, consume tag, `load_last_outputs`, job acquisition). If you can't
  name it, the design is not done.
- **Automatically scheduled managers acquire** (`acquire_all_jobs = True`, ownership narrowed per
  spider when several instances share a name); they never resume.
- **Filters before deduplicators**; heavy, stable state (deduplicators) separated from logic that
  changes often (consumer, filters).
- **Prefer seeds over urls** in the frontier; use url batch files built by the crawl manager only when
  the work has no short key.
- **Simplest thing that works.** A single crawl manager, or spiders delivering through item pipelines,
  is a legitimate design for a small project. Don't add stages the requirements don't ask for.
