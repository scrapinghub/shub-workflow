# Annotated PROGRAMS examples

Synthetic but realistic entries, dissected so the patterns are reusable. They use standard Scrapy /
scrapy-zyte-api stat names so they translate to any project. Each `command_line` is a list of
separate argv tokens; a flag and its value are two list items. Replace `<PROJECT_ID>` with a numeric
ScrapyCloud project id (or an alias the project defines).

## 1. Stats + plot, parameterized by `{source}`

```python
"pool_download_rates": {
    "description": "Per-pool download rates for a source. Use with -v source:<source>",
    "command_line": [
        "--project-id=<PROJECT_ID>",
        "downloader",                          # spider name (positional)
        "-a", "source:{source}",               # filter: only jobs whose spider arg source matches
        "-s", "ip_type/.+?/(ipv6)",            # 3 stat patterns, each captures a pool-type label
        "-s", "ip_type/.+?/(isp)",
        "-s", "ip_type/.+?/(datacenter)",
        "--separate-patterns-per-target",      # emit one datapoint per pattern
        "-c", "dup 4 prune 4 -1 roll add div", # postscript over the captured tuple
        "--data-headers=auto",                 # name datapoints from alternating label/value
        "--plot", "title=Download rates ({source}),no_tiles",
        "--period", "10 days",
        "--include-running-jobs",
        "-z", "UTC",
    ],
},
```

Run: `scanjobs.py -g pool_download_rates -v source:mysource`. The `{source}` placeholders in both
`-a` and the plot `title` are filled by `-v`.

## 2. Counting log matches with `--count` + bins

When you count log/stat matches, emit `1` per match (`--count`) and aggregate with `bins=n/sum`.
Here the positional spider itself is parameterized:

```python
"scraped": {
    "description": "Scraped items over time for a spider. Use with -v spider:<spider>.",
    "command_line": [
        "--project-id=<PROJECT_ID>",
        "{spider}",                            # the positional target is parameterized
        "-s", "(item_scraped_count)",
        "--data-headers=auto",
        "--plot", "title={spider} scraped,bins=120/sum,xticks=20",
        "--period", "5 days",
        "-z", "UTC",
    ],
},
```

## 3. ⚠️ Escaped braces in a log regex AND in postscript

This is the canonical "doubled braces" case. Because `str.format()` runs on every token, the regex's
literal `}` and the postscript `repeat` block's `{ }` must be written doubled. Suppose a script logs
lines like `Error report: {"errors": 12, ... "total": 3456}`:

```python
"error_rate": {
    "description": "Error rate from a script's report lines",
    "command_line": [
        "--project-id=<PROJECT_ID>",
        "py:report.py",                        # a script target, not a spider
        "-l", r"Error report:.+?\"errors\": (\d+)",
        "-l", r"Error report:.+?\"total\": (\d+)}}",   # literal } -> }}
        "-c", "hold count 1 roll count 2 sub {{ add }} repeat exch div",
        #          ^hold carries the first -l result into the second extraction in the same job
        #                                    ^^^^^^^^^^^^  {{ }} = a literal { } repeat block
        "--data-headers", "error_rate",
        "--plot", "title=Error rate",
        "-p", "15 days",
        "-z", "UTC",
    ]
},
```

Rule of thumb: any `{` or `}` that is part of the regex/postscript itself (not a `{var}` placeholder)
must be doubled.

## 4. Ratio from stats with safe defaults

```python
"ban_rate": {
    "description": "Ban rate (status 520) for a spider. Use with -v spider:<spider>",
    "command_line": [
        "--project-id=<PROJECT_ID>",
        "{spider}",
        "-s", "scrapy-zyte-api/status_codes/520",
        "-s", "scrapy-zyte-api/processed",
        "-d", "scrapy-zyte-api/status_codes/520",   # default to 0 when the stat is absent
        "-c", "0 count 1 roll div 1 prune",         # compute a single ratio, keep 1 value
        "--data-headers=ban_rate",
        "--plot", "title={spider} ban rate (status 520),bins=20/mean",
        "-z", "UTC",
    ],
},
```

## Checklist when adding a new entry

- [ ] `--project-id` set (numeric id, or an alias the project defines).
- [ ] Flag + value are **separate list items**.
- [ ] Per-run values are `{var}` placeholders, documented in `description` ("Use with -v …").
- [ ] Any literal `{`/`}` in a regex or a `repeat` block is doubled to `{{`/`}}`.
- [ ] `--data-headers` present if `--plot` is used; `title=` present in the plot string.
- [ ] `-d` safe-default stats added if `-c` indexing depends on fixed tuple arity.
- [ ] Style matches the surrounding entries; the project's linter passes.
