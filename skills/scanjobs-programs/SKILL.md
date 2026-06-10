---
name: scanjobs-programs
description: >-
  Use for help using shub_workflow's scanjobs tool — scanning ScrapyCloud jobs to extract and plot
  data from stats, logs, items or spider args. Covers building a scanjobs command line (the
  stat/log/item/spider-arg patterns, the postscript post-processor -c, the --plot mini-language,
  time windows, and output modes) and the predefined "programs" shortcut: the PROGRAMS dict in a
  project's scripts/scanjobs.py (a subclass of shub_workflow.utils.scanjobs.ScanJobs), invoked as
  `scanjobs.py -g <program> -v key:val`, including {var} placeholders and {{ }} escaping. Applies to
  any project whose scripts/scanjobs.py subclasses shub_workflow's ScanJobs.
---

# scanjobs

`scanjobs.py` scans ScrapyCloud jobs for a spider/script, extracts data from **stats**, **log
lines**, **items**, or **spider args** via regex, optionally post-processes the extracted numbers
(`-c`), and optionally plots them (`--plot`). A project subclasses
`shub_workflow.utils.scanjobs.ScanJobs` and defines a `PROGRAMS` dict so long, frequently-used
command lines get a short alias.

```python
from shub_workflow.utils.scanjobs import ScanJobs as ShubScanJobs

class ScanJobs(ShubScanJobs):
    PROGRAMS = {
        "response_profile": {
            "description": "Response profile for a spider. Use with -v spider:<spider>",
            "command_line": [
                "--project-id=<PROJECT_ID>", "{spider}",
                "-s", "downloader/(response_count)",
                "--data-headers=auto",
                "--plot", "title={spider} response profile,no_tiles",
            ],
        },
    }
```

Invoke: `scanjobs.py -g response_profile -v spider:myspider`.

> **Human-readable docs.** The shub-workflow wiki has a full prose guide to this tool at
> <https://github.com/scrapinghub/shub-workflow/wiki/ScanJobs>. If the user wants a walkthrough,
> a shareable reference, or asks where the documentation is, point them there.

## How `-g` / `-v` work (read this before editing PROGRAMS)

`PROGRAMS` is consumed by `ArgumentParserScript.parse_args` in `shub_workflow.script`. The mechanics
that bite people:

- **`command_line` is a list of separate argv tokens**, NOT a shell string. A flag and its value
  are two list items: `"-s", "downloader/(response_count)"` — never `"-s downloader/..."`.
- **Every token gets `str.format(**vars)` applied**, where `vars` comes from `-v k1:v1,k2:v2`.
  So `"spider:{spider}"` becomes `"spider:myspider"`.
- **⚠️ Literal braces must be doubled.** Because `.format()` runs on every token, any real `{`/`}`
  in a regex or a postscript `repeat` block must be escaped as `{{`/`}}`. E.g. a log regex
  `"total": (\d+)}` is written `"total\": (\\d+)}}"`, and a repeat block is `"{{ add }}"`.
  Forgetting this raises `KeyError`/`ValueError` at format time. See
  [examples/annotated-programs.md](examples/annotated-programs.md) for worked cases.
- **Explicit CLI flags override the program's value.** `-g response_profile -p "5 days"` runs the
  program but with `--period` replaced. Anything left at argparse default in the program is
  overridden by a value you pass explicitly — so a program can hard-code `--project-id` yet still be
  re-pointed at another project on the CLI.

To list a project's programs: read the `PROGRAMS` dict in its `scripts/scanjobs.py`, or run with a
bogus `-g` (e.g. `-g ?`) — the parser prints `* <name>: <description>` for each.

## Authoring / editing a PROGRAMS entry

1. **Pick the extraction source** and its flag (full reference:
   [references/cli-options.md](references/cli-options.md)):
   - stats → `-s <regex>` (matched against stat keys; regex groups + the stat value are emitted)
   - log lines → `-l <regex>` (regex groups emitted; add `--count` to emit `1` per match for counting)
   - items → `-i <jmespath>:<regex>`
   - filter jobs by spider arg → `-a <arg>:<regex>`
2. **Always set `--project-id`** (jobs are read outside ScrapyCloud): a numeric SC project id, or a
   configured alias if the project defines one.
3. **Parameterize** the per-run bits (spider, source, …) as `{var}` and document them in the
   `description` ("Use with -v spider:<spider>").
4. If you post-process the extracted numbers, write the `-c` expression — see
   [references/postscript.md](references/postscript.md). Remember to escape `repeat` braces as `{{ }}`.
5. If you plot, set `--data-headers` (required by `--plot`) and the `--plot` string — see
   [references/plot-dsl.md](references/plot-dsl.md). `title=` is required.
6. Keep the entry's style consistent with the existing dict (one token per list item, `-z UTC` and
   `--period` conventions). Run the project's linter after editing.

## Running a program

Run from the project's `scripts/` dir as `python scanjobs.py -g <program> [-v ...] [overrides]`.
It must run **inside the project's environment** (it imports `scrapinghub`, and for plotting needs
`pandas`/`seaborn`/`matplotlib`) — invoke through whatever the project uses (pipenv, poetry, a venv,
etc.), not a bare system `python`. Plots are shown interactively or, when no display is available,
saved as `<uuid>.png` in the cwd. A project often keeps a small shell script cataloging its common
invocations — a useful place to copy real examples from.
