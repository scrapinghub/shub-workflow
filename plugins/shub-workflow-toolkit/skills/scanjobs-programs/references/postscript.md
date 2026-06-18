# `-c` postscript post-processor

`-c` / `--post-process-code` runs a tiny **PostScript-like, stack-based** language over the tuple of
values extracted from each match, transforming it before it is printed / written / plotted.
Implemented in `post_process()` in `shub_workflow/utils/scanjobs.py` (the function's docstring has
doctests — the source of truth).

## Mental model

- The extracted values (regex groups, then the matching stat values, in extraction order) seed the
  **stack**, left to right. String numbers are coerced to float by arithmetic ops.
- Instructions are whitespace-separated tokens, evaluated left to right, each mutating the stack.
- Whatever remains on the stack at the end is the emitted datapoint (mapped onto `--data-headers`).
- Non-numeric leading items (like a label/`ipType` group) are typically left in place and carried
  through to become the `hue`/`tile_key` in a plot.

## Instructions

**Binary arithmetic** (pop two, push result, as floats):

| op | effect (… a b →) |
| --- | --- |
| `add` | a+b |
| `sub` | a-b |
| `mul` | a*b |
| `div` | a/b |

**Stack manipulation / counting:**

| op | effect |
| --- | --- |
| `dup` | duplicate top element |
| `pop` | discard top element |
| `exch` | swap the top two elements |
| `roll` | pops `x y`; rolls the top `x` elements by 1 — left if `y=-1`, right if `y=1` (PostScript roll) |
| `count` | push the current stack size |

**Flow:**

| op | effect |
| --- | --- |
| `<n> { … } repeat` | run the block `n` times. **In a PROGRAMS list the braces must be doubled:** `"{{ ... }}"`, because `str.format()` is applied to each token (see SKILL.md). |

**Conversion:**

| op | effect |
| --- | --- |
| `cvi` | pop top, convert to int, push back |

**Special (non-PostScript):**

| op | effect |
| --- | --- |
| `prune <n>` | keep only the top `n` elements of the stack, discard the rest below them |
| `hold` | preserve the stack across extractions **within the same job** (e.g. when combining a log extraction and a stat extraction): pushes the current extraction result without post-processing so it carries into the next extraction in that job. Consumed on next extraction. |

## Worked examples (from the docstring)

```
post_process(["stringA", 3, 4, "dup"])          -> ['stringA', 3, 4, 4]
post_process(["stringA", 3, 4, "div"])           -> ['stringA', 0.75]
post_process(["stringA", 3, 4, 3, 1, "roll"])    -> [4, 'stringA', 3]
post_process(["123", "cvi"])                     -> [123]
post_process(["1","2","3","4","div","2","prune"])-> ['2', 0.75]
post_process(["3","4","5","2","{","add","}","repeat"]) -> [12.0]
```

`"3 -1 roll pop exch div"` on `('datacenter','11558','2500','9059')`:
- `3 -1 roll` rotates the top 3 left → `('datacenter','2500','9059','11558')`
- `pop` drops the top → `('datacenter','2500','9059')`
- `exch div` → divide → `('datacenter', 3.6236)`

See the module docstring (examples 4 and 5) for fully narrated longer pipelines, and
[../examples/annotated-programs.md](../examples/annotated-programs.md) for real PROGRAMS using `-c`.

## Tips

- Use `-d <stat>` (`--safe-default-stat`) so missing stats default to `0` and the tuple keeps a
  fixed arity — otherwise `roll`/`div` indices drift between jobs and the expression breaks.
- Build expressions incrementally; the doctest form `post_process([...])` is the fastest way to
  verify a tricky stack manipulation before pasting it into a program (mind the doubled braces).
