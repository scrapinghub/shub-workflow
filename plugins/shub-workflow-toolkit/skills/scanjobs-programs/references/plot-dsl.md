# `--plot` mini-DSL

`--plot` takes a single **comma-separated** string of `key=value` pairs and bare flags. Parsed in
`ScanJobs.run()` in `shub_workflow/utils/scanjobs.py`; rendered with seaborn/matplotlib. **Requires
`--data-headers`** so extracted datapoints have names to reference. The x-axis defaults to the
match timestamp.

## Keys

| token | type | meaning |
| --- | --- | --- |
| `title=<str>` | **required** | plot title. May contain `{var}` placeholders filled by `-v`. |
| `X=<key>` | key | x-axis column. Default `tstamp`. |
| `Y=<k1/k2/...>` | keys | y-axis column(s), `/`-separated. If omitted, uses all extracted headers except those used by `X`/`hue`. |
| `hue=<key>` | key | column to color/group series by (e.g. `source`, `pool`). |
| `tile_key=<key>` | key | column whose distinct values become separate subplot tiles. |
| `ylabel=<str>` | label | y-axis label (defaults to the y key). |
| `xticks=<int>` | int | max number of x ticks. |
| `smooth=<int>` | int | rolling-window smoothing width applied to the series. |
| `bins=<n>/<func>` | int/str | bin the x-axis into `n` bins and aggregate each with `func` — any pandas `agg()` name: `sum`, `mean`, `std`, `median`, … |

## Bare flags (presence = true)

| flag | meaning |
| --- | --- |
| `save` | save the plot to `<uuid>.png` instead of (only) displaying. (Also auto-saves when no interactive display is available.) |
| `no_tiles` | plot all y keys on one set of axes instead of separate tiles. |
| `tdiff` | plot the time-difference (delta between consecutive points) of the series rather than raw values. |

## Notes

- `Y` separates keys with `/` (not commas) — commas delimit the top-level `--plot` options.
- The keys you name (`X`, `Y`, `hue`, `tile_key`) must match `--data-headers` names (or `auto`-derived
  ones, or `tstamp`).
- `bins=.../sum` pairs naturally with `--count` for counting log/stat matches per bin.

## Examples

```
--data-headers=auto  --plot "title=Download rates ({source}),no_tiles"
--plot "Y=retries/give_ups_ratio,hue=pool,title=Retries vs give-ups ratio,smooth=5"
--plot "title={spider} scraped,bins=120/sum,xticks=20"
--plot "title={spider} ban rate (status 520),bins=20/mean"
--plot "title=Pending items for {source},bins=10/mean,tdiff"
--plot "title=5xx error rate per source,tile_key=avg_latency,ylabel=error_rate"
```
