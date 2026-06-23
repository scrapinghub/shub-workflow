# scanjobs.py CLI options reference

Source: `shub_workflow/utils/scanjobs.py` (`ScanJobs.add_argparser_options`) plus `--program`/
`--program-variables` and `--project-id` from `shub_workflow/script.py`. All multi-value options
are `action="append"` — repeat the flag to give several.

## Positional

| arg | meaning |
| --- | --- |
| `spider` | Target spider or script name (e.g. `downloader`, `py:deliver.py`). `*` matches everything; combine with `--spiders-only` / `--scripts-only`. |

## Extraction patterns (repeatable)

| flag | format | emits |
| --- | --- | --- |
| `-s`, `--stat-pattern` | regex on stat **keys** | regex groups + the stat's value |
| `-l`, `--log-pattern` | regex on log lines | regex groups (with `--count`, emits `1` per match) |
| `-i`, `--item-field-pattern` | `jmespath:regex` on items | regex groups, or just existence if regex is empty |
| `-a`, `--argument-pattern` | spiders: `arg:regex`; scripts (`py:`): bare `regex` | does not emit; **filters** scan to jobs whose argument matches. For spiders the regex is matched against the spider arg named `arg`; for scripts there is no `arg:` separator — the regex is matched against the job command line (`job_cmd`) |
| `--tag-pattern` | regex | only scan jobs whose tag matches |

## Time window

| flag | default | meaning |
| --- | --- | --- |
| `-p`, `--period` | `86400` (1 day) | window length; seconds or a `timelength` string like `"10 days"` |
| `-e`, `--end-time` | now | window end; any `dateparser`-recognized string |

## Job selection

| flag | meaning |
| --- | --- |
| `--has-tag` (repeatable) | only jobs carrying the given tag (exact) |
| `--include-running-jobs` | also scan running jobs (default: finished only) |
| `--spiders-only` / `--scripts-only` | with `spider='*'`, restrict to spiders / scripts |
| `--first-match-only` | stop a job after its first match |
| `--max-items-per-job N` | scan at most N items/logs per job |

## Post-processing & output shaping

| flag | meaning |
| --- | --- |
| `-c`, `--post-process-code` | postscript-like expression over the extracted tuple — see [postscript.md](postscript.md) |
| `--data-headers` | name the datapoints: comma-list, or `auto` (auto needs alternating text/value extraction). Turns each datapoint from a tuple into a dict. **Required by `--plot`.** |
| `-d`, `--safe-default-stat` (repeatable) | a stat that can be defaulted to `0` when absent — keeps tuple arity stable for `-c` when some jobs lack the stat |
| `--count` | emit `1` per log match so matches can be summed (e.g. `bins=n/sum`) |
| `--separate-matches-per-target` | yield each match (log line / stat) as its own datapoint |
| `--separate-patterns-per-target` | yield matches grouped per pattern per target |
| `--capture-spiderargs` / `--capture-joblink` | with `--data-headers`, also include job spider args / job URL in each record |

## Time formatting

| flag | default | meaning |
| --- | --- | --- |
| `--tstamp-format` | `%Y-%m-%d %H:%M:%S` | output timestamp format |
| `-z`, `--zone-info` | (local) | force output timestamps into a `zoneinfo` zone, e.g. `UTC` |

## Plot / persist

| flag | meaning |
| --- | --- |
| `--plot` | generate a plot; mini-DSL described in [plot-dsl.md](plot-dsl.md). Requires `--data-headers`. |
| `-w`, `--write FILE` | write captured datapoints to a JSON-lines file (for later analysis / `-r`) |
| `-r`, `--read FILE` | plot/process from a previously `-w`-written file instead of scanning |
| `--generate-items-sample N` | with `-i`, reservoir-sample up to N matched items into `items_sample_*.jl.gz` |

## Misc

| flag | meaning |
| --- | --- |
| `--print-progress-each N` | progress log cadence (default 100 jobs) |
| `--no-user-enter` | don't pause for Enter on each match (implicit with `-w`/`--plot`/`--generate-items-sample`) |
| `--project-id` | ScrapyCloud project to read jobs from; required when run outside SC. Accepts a numeric id or a configured alias if the project defines one. |
| `-g`, `--program` | run a `PROGRAMS` alias |
| `-v`, `--program-variables` | `key1:val1,key2:val2` placeholder values for the program |

## parse-time validations

- `--plot` without `--data-headers` → error.
- `--spiders-only` / `--scripts-only` only valid when `spider == '*'`.
