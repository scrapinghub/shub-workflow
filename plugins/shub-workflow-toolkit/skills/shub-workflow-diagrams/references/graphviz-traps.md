# Graphviz traps these diagrams are built around

Every rule in `diagram_engine.py` that looks over-engineered is here because of one of these. They
cost a session each to find; none of them produces a helpful error.

## 1. Rank-based layout reshuffles between runs

Letting `dot` rank a banded pipeline looks fine until you add a node and the bands swap order, the
Role column jumps to the left, or the sources come out reversed. Flat (same-rank) edges, `rank=same`
groups, `ordering=out`, `weight`, `constraint` — all of them influence it, none of them pins it, and
clusters make it worse.

**So:** pipeline diagrams compute every node's position and render with `neato -n2`, which uses the
given `pos` and only routes the edges. A **class hierarchy is the exception** — it is a tree, `dot`
ranks base above subclass correctly, and fixing positions there would be pure cost.

## 2. HTML ports are ignored under `splines=ortho`

`node:port:w` works beautifully under `splines=polyline` / `spline` and is silently ignored under
`ortho`: every edge aimed at a different row of one table lands wherever the router likes along the
table's edge, a few points apart. It renders, so nothing tells you.

**So:** when N boxes must each be the endpoint of their own edge, they must be **N real nodes**, not
N rows of one table node. `column()` / `pile()` stack real nodes tightly in one lane for exactly
this. Use a `stack` (one table node) only when the whole pile is one edge endpoint.

## 3. An endpoint exactly on a node's border crashes the renderer

An edge ending on the boundary of a node — e.g. an anchor placed on a box's bottom edge — makes the
orthogonal router print `add_segment: error` and then **segfault**. `subprocess` surfaces it as
`CalledProcessError ... SIGSEGV`, which looks like a broken install.

**So:** keep such endpoints a couple of points clear of the border. Two points is invisible at any
sane zoom and is the difference between a render and a core dump.

## 4. An edge naming a node that was never positioned also segfaults

Under `-n2` a node with no `pos` is fatal: `Error: node <id> in graph G has no position` followed by
a segfault. The usual cause is a **stale id after a rename** — you renamed a cell and missed an
edge.

**So:** when a segfault appears right after an edit, grep the generated `.dot` for the ids in your
edges before suspecting anything subtle.

## 5. `headport` / `tailport` do not steer an orthogonal edge

`tailport` is honoured, `headport` is not, so "leave east, arrive from the north" comes out as
"leave east, drop immediately, arrive from the west". The router picks the corner, and it often
picks the opposite one to the one you want.

**So:** force the shape with an invisible **waypoint** at the corner and split the edge in two —
`a → corner` with `arrowhead=none`, then `corner → b`. Reads as one line, routes deterministically.
Two waypoints sharing an offset put their corners on the same vertical, which is how two arrows end
up visually symmetric.

## 6. HTML-table nodes size themselves, so a declared size is a guess

A stack or a Role box is an HTML table; its real height is only known once Graphviz has drawn it. A
guessed height leaves slack below every band, or — worse — lets a node overflow its lane and crowd
the next one.

**So:** render twice. Lay out from the declared sizes, ask `neato -n2 -Tplain` for the sizes it
actually drew, lay out again from those. `measure()` does this; the gap between lanes is then exactly
the configured gap rather than "about that".

## 7. Stacked "⋮" rows split into visible clusters

Repeating a `⋮` as several table **rows** inserts cell padding between them, so six dots read as two
separate groups of three.

**So:** put the glyphs in **one** cell joined by `<BR/>`. Same for free-standing dot annotations:
space them by about the glyph's own line height (around 10pt at 14pt type), not more.

## 8. Edge labels under `ortho` float, and collide where edges bundle

`splines=ortho` does not support edge labels at all — Graphviz warns and falls back to `xlabel`,
placed near the middle of the edge. When several edges converge the labels pile up on each other.

**So:** for a fan of edges that share a target, pin the label to the **tail** (`taillabel` with
`labeldistance` / `labelangle`) so one sits beside each source. When even that is not enough —
or when the edge is wide enough that a label cannot sit on it — place the text as an **annotation
anchored to a node**, with an explicit offset and justification. That is the only placement you
fully control.

## 9. The background layer is not in the bounding box

Band frames and titles are drawn through the graph's `_background` attribute (xdot operations), which
keeps them behind the nodes and incapable of colliding with them — but Graphviz does not grow the
canvas to fit them, so they get clipped.

**So:** two invisible `point` nodes pinned at the canvas corners define the extent. If background
drawing disappears off an edge, that is what moved.

## 10. A wide arrow has no room for a label on it

At `penwidth` 6 and above, a label centred on the line is unreadable — the line is thicker than the
text's x-height.

**So:** bus- and trunk-weight edges carry their label as a node-anchored annotation offset clear of
the line, never as an edge label.
