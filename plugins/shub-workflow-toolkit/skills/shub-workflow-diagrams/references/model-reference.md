# The pipeline diagram model

A diagram is plain data: `{"name", "title", "stages", "edges"}`, plus optional `"annotations"` and
`"lane_breaks"`. `name` is the output basename (`<name>.dot`, `<name>.png`).

## Two axes

- The **major** axis is the direction stages follow one another — left to right in the default
  horizontal orientation.
- The **minor** axis is the direction the cells of a stage spread — top to bottom. A position on it
  is a **lane**, and a cell picks its lane with `col`.

Flipping `ORIENTATION` swaps the two. Nothing in a model mentions x or y, which is why the same model
renders either way.

Lanes are shared by every stage, so **a cell at `col=2` in one band lines up with `col=2` in the
next**, and an edge between them comes out horizontal. That alignment is the main layout tool: put
the things that correspond to each other in the same lane.

## Stages

```python
band(key, title, cells, role=[...], gap_after=None)   # a framed stage with a title and a Role box
free(cells, gap_after=None)                           # loose nodes between bands: no frame, no Role
top(cells)                                            # a strip above the bands (see below)
```

- `key` is referenced by annotations and by `align`, so keep it stable.
- `role` is the bulleted list in the band's Role box. Sentence case.
- `gap_after` widens just the gap that follows this stage — use it when something has to fit in
  that gap, not to nudge the whole layout.

A `top` stage costs nothing along the flow axis: its cells sit in a strip above everything and are
positioned by `align` (a stage key, or several to centre between them) instead of `col`. Park a
storage there when a stage both reads and writes it, so it does not force a column of its own.

## Cells

```python
box(label, col=..., align=..., dminor=0, w=..., h=...)   # a job or component
storage(label, col=..., align=...)                       # a bucket / frontier (an ellipse)
stack(label, col, repeat=3, closing=True, dots=1)        # N identical boxes + "⋮" (+ a closing box)
maybe_more(label, col)                                   # one box with a "⋮": "there may be more"
column(entries, col, gaps=(), w=...)                     # differently-labelled boxes piled in one lane
pile(prefix, label, col, count, gaps=())                 # `count` identical boxes, keyed <prefix>0..
```

`col` may be fractional — `col=1.5` centres a cell between lanes 1 and 2.

`dminor` offsets a cell **within** its lane, which is what lets several boxes share one lane;
`column()` and `pile()` use it. The lane grows to contain them.

**`stack` vs `pile`**: a `stack` is one node (an HTML table), so the whole pile is a single edge
endpoint — compact, and right when the edges don't distinguish the instances. A `pile` is `count`
separate nodes, so each box can be its own endpoint. See
[graphviz-traps.md](graphviz-traps.md#2-html-ports-are-ignored-under-splinesortho) for why that is a
real constraint and not a preference.

## Anchors (invisible cells you attach edges to)

```python
waypoint(col=..., align=..., dmajor=0, dminor=0)   # a corner to route an edge through
border(align, dmajor=0)                            # on a band's leading border (its top)
side(align, col, dminor=0)                         # on a band's trailing border (its right edge)
```

- A **waypoint** forces an edge's shape: `a → waypoint` with `arrowhead=none`, then `waypoint → b`.
  It reads as one line. Two waypoints sharing a `dmajor` put their corners on the same vertical.
- **`border` / `side`** are for edges that concern a stage **as a whole** — a bus arrow from a band
  rather than from one job inside it. `dmajor` / `dminor` slide the anchor along that border, which
  is how several parallel arrows leave one container at distinct points.

## Edges

```python
(src, dst, label, kind)                    # kind: thin | thick | dashed | bus | trunk
(src, dst, label, kind, {"dir": "both"})   # 5th element: extra Graphviz attributes
```

`thin` control/naming · `thick` ordinary data flow · `dashed` fan-out · `bus` a whole band's traffic
in one wide arrow · `trunk` wider still, where the bulk of the bytes go.

Useful extras: `{"arrowhead": "none"}` on the first half of a waypoint-routed edge, `{"dir": "both"}`
for a bidirectional arrow, `{"constraint": False}` for a back edge.

An endpoint may carry a port (`"node:p3:w"`) — but read
[graphviz-traps.md](graphviz-traps.md#2-html-ports-are-ignored-under-splinesortho) first: ports do
not work under orthogonal routing.

## Annotations

Free text drawn in the background layer, for things that are not nodes — continuity dots, a label a
wide edge has no room for.

```python
{"text": "⋮", "col": 2, "stage": "<band key>", "dmajor": 0, "size": 14, "color": MUTED}
{"text": "videos download", "at": "<node id>", "dx": 150, "dy": -14, "just": 0}
```

The first form places it on the lane grid, the second pins it to a node with an offset in points.
`just` is -1 left / 0 centred / 1 right **of the given point**, which is how one label sits flush
left of a group of arrows and another flush right.

## Groups

`lane_breaks` is the set of lane indices that begin a new group; those lanes get a wider gap, so
related lanes read as a block. `column()` / `pile()` take the same idea as `gaps=(...)`.

## Geometry

The constants at the top of the engine (`NODE_W`, `LANE_GAP`, `BAND_GAP`, `ROLE_W`, `MARGIN`, …) are
the house proportions. Prefer composing with the model over retuning them; when you do change one,
re-render both diagram kinds and look at them, because they are shared.
