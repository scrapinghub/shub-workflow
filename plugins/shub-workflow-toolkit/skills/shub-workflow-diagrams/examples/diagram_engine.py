#!/usr/bin/env python3
"""Rendering engine for shub-workflow pipeline diagrams. **Copy this file verbatim; do not edit it.**

Your project describes *what* the diagram contains, in a plain-data model; this file decides where
everything goes and emits the Graphviz source. Keeping the two apart means a project only ever owns
its model, and picking up a fix here is a re-copy rather than a merge.

    from diagram_engine import band, free, top, box, storage, render

    MODEL = {"name": "workflow", "title": "my project - discovery", "stages": [...], "edges": [...]}

    if __name__ == "__main__":
        render([MODEL], dot_dir=Path(__file__).parent, png_dir=Path(__file__).parent.parent)

The diagrams are laid out by this module, not by Graphviz: every node is given an explicit `pos` and
rendered with `neato -n2`, which then only routes the edges. That is what makes them stable — adding
a node never reshuffles the rest — and it is also a requirement rather than a preference, because
Graphviz's own ranking reorders bands and columns between runs for no visible reason. Band frames
and their titles are drawn in a background layer (`_background`), so they can never collide with a
node.

See the skill's references/model-reference.md for the model vocabulary and
references/graphviz-traps.md for the rendering constraints baked into this file.
"""
import math
import subprocess
from pathlib import Path

# --- palette: one warm, low-contrast set shared by every diagram ------------------------------
BG = "#F4F1DE"  # page / band background
FILL = "#F2CC8F"  # box fill
STROKE = "#E07A5F"  # box + band border
FONT = "#393C56"  # all text, and edges
MUTED = "#8A8DA6"  # the "many more like this" stack separator

# --- geometry, in points ----------------------------------------------------------------------
FONTNAME = "Helvetica"
ORIENTATION = "horizontal"  # "horizontal": the flow runs left to right; "vertical": top to bottom
NODE_W, NODE_H = 120, 32  # a plain box
# A stack is an HTML table, so its real height is only known once Graphviz has drawn it: these
# two are the first-pass guess, which `measure()` then replaces with the rendered size. That is
# what makes the gap to the next lane exactly LANE_GAP, so a "⋮" can never reach the box below.
STACK_ROW_H = 22  # one box row of a stack
STACK_DOTS_H = 28  # the "⋮" row plus the table's own padding
STORAGE_W, STORAGE_H = 190, 64
ROLE_W = 215  # the Role box is this wide in either orientation
ROLE_LINE_H = 17
ROLE_CHARS = 30  # characters per role line before it wraps
ROLE_GAP = 44  # between the last lane and the Role lane
LANE_GAP = 9  # between two lanes of the same group
LANE_GROUP_GAP = 60  # between two lanes that belong to different groups (see `lane_breaks`)
BAND_GAP = 80  # between two consecutive bands
BAND_PAD = 16  # band frame padding around its content
BAND_TITLE_H = 22
PILE_BOX_H = 22  # one box of a pile
PILE_PITCH = 24  # centre to centre: the boxes are almost touching
PILE_GROUP_GAP = 34  # extra room between two groups of a pile
TOP_GAP = 40  # between the top strip and the band frames
MARGIN = 24  # canvas margin


# --- model helpers ----------------------------------------------------------------------------
def box(label, col=None, align=None, dminor=0, w=NODE_W, h=NODE_H):
    return dict(kind="box", label=label, col=col, align=align, dminor=dminor, w=w, h=h)


def column(entries, col, gaps=(), w=NODE_W):
    """Boxes piled tightly in one lane, each a node of its own so an edge can land on one of them.
    `entries` is a list of (node id, label); `gaps` holds the indices before which the pile opens
    up, to set groups apart.

    They have to be separate nodes rather than the rows of one `stack`: Graphviz's orthogonal
    router ignores HTML ports, so an edge aimed at a row of a table lands wherever it likes along
    that table's edge.
    """
    offsets, at = [], 0.0
    for i in range(len(entries)):
        if i:
            at += PILE_PITCH + (PILE_GROUP_GAP if i in gaps else 0)
        offsets.append(at)
    middle = offsets[-1] / 2
    return {nid: box(label, col=col, dminor=offset - middle, w=w, h=PILE_BOX_H)
            for (nid, label), offset in zip(entries, offsets, strict=True)}


def pile(prefix, label, col, count, gaps=(), w=NODE_W):
    """`count` identical boxes in a column, keyed `<prefix>0` … `<prefix>{count-1}`."""
    return column([(f"{prefix}{i}", label) for i in range(count)], col, gaps=gaps, w=w)


def stack(label, col, repeat=3, closing=True, dots=1, w=NODE_W):
    """`repeat` boxes, `dots` rows of "⋮", and (unless closing=False) one more box under it."""
    return dict(kind="stack", label=label, col=col, repeat=repeat, closing=closing, dots=dots,
                w=w, h=(repeat + (1 if closing else 0)) * STACK_ROW_H + dots * STACK_DOTS_H)


def maybe_more(label, col, w=NODE_W):
    """One box with a "⋮" under it: there may be more jobs of this kind than the one drawn."""
    return stack(label, col, repeat=1, closing=False, w=w)


def storage(label, col=None, align=None, w=STORAGE_W, h=STORAGE_H):
    return dict(kind="storage", label=label, col=col, align=align, w=w, h=h)


def waypoint(col=None, align=None, dmajor=0, dminor=0):
    """An invisible cell: route an edge through one to choose where it turns the corner.

    `dmajor` shifts it off its stage's centre line, along the flow axis; two waypoints sharing a
    `dmajor` line up, so the edges turning at them do too."""
    return dict(kind="waypoint", label="", col=col, align=align, dmajor=dmajor, dminor=dminor,
                w=1, h=1)


def side(align, col, dminor=0):
    """An invisible cell on the trailing border of a band — its right edge, laid out horizontally —
    level with lane `col`, `dminor` away from that lane's centre."""
    return dict(kind="waypoint", label="", col=col, align=align, at="side", dminor=dminor,
                w=1, h=1)


def border(align, dmajor=0):
    """An invisible cell on the leading border of a band, centred on it unless `dmajor` shifts it
    along that border. Attach an edge here when it concerns the band as a whole rather than one of
    the jobs inside it."""
    return dict(kind="waypoint", label="", col=None, align=align, at="border", dmajor=dmajor,
                w=1, h=1)


def band(key, title, cells, role=(), gap_after=None):
    return dict(type="band", key=key, title=title, cells=cells, role=list(role),
                gap_after=gap_after)


def free(cells, gap_after=None):
    return dict(type="free", cells=cells, gap_after=gap_after)


def top(cells):
    """Nodes parked in a strip before the first lane — the top of the figure, laid out
    horizontally. They take no space along the flow: each cell's `align` names the stage it is
    centred over, or several stages to centre it between them."""
    return dict(type="top", cells=cells)


# --- layout -------------------------------------------------------------------------------------
def wrap(text, width):
    lines, line = [], ""
    for word in text.split():
        candidate = f"{line} {word}".strip()
        if len(candidate) > width and line:
            lines.append(line)
            line = word
        else:
            line = candidate
    if line:
        lines.append(line)
    return lines or [""]


def role_height(bullets):
    return sum(len(wrap(b, ROLE_CHARS)) for b in bullets) * ROLE_LINE_H + 16


def layout(model, horizontal, measured):
    """Assign every node a centre (x, y) in top-down screen coordinates, and every band a frame.

    A diagram has two axes: the bands follow one another along the MAJOR axis, and the cells of a
    band spread along the MINOR axis, one per `col` (a "lane"). Flipping the orientation only
    swaps the two — which is why the model never mentions x or y.
    """
    def minor_of(nid, cell):
        return measured.get(nid, (cell["w"], cell["h"]))[1 if horizontal else 0]

    def major_of(nid, cell):
        return measured.get(nid, (cell["w"], cell["h"]))[0 if horizontal else 1]

    def role_cell(stage):
        return {"w": ROLE_W, "h": role_height(stage["role"])}

    # screen coordinates from (minor, major)
    place = (lambda m, j: (j, m)) if horizontal else (lambda m, j: (m, j))

    flow = [st for st in model["stages"] if st["type"] != "top"]
    tops = [st for st in model["stages"] if st["type"] == "top"]
    top_cells = [(nid, c) for st in tops for nid, c in st["cells"].items()]
    top_h = max((minor_of(nid, c) for nid, c in top_cells), default=0)
    top_room = top_h + TOP_GAP if top_cells else 0

    # --- lanes: one per integer col, as wide/tall as the largest cell sitting in it
    lane_size = {}
    for stage in flow:
        for nid, cell in stage["cells"].items():
            if cell["col"] == int(cell["col"]):
                lane = int(cell["col"])
                # a cell offset within its lane still has to fit inside it
                extent = 2 * (abs(cell.get("dminor", 0)) + minor_of(nid, cell) / 2)
                lane_size[lane] = max(lane_size.get(lane, 0), extent)
    widest = max((c["col"] for st in flow for c in st["cells"].values() if c["col"] is not None),
                 default=0)
    nlanes = max(max(lane_size, default=0), math.ceil(widest)) + 1
    lane_size = {i: lane_size.get(i, NODE_H if horizontal else NODE_W) for i in range(nlanes)}

    # a band title is always drawn in a strip at the top of its frame, which in horizontal
    # orientation is the start of the minor axis
    breaks = set(model.get("lane_breaks", ()))
    centers, at = {}, MARGIN + top_room + (BAND_TITLE_H if horizontal else 0) + BAND_PAD
    for i in range(nlanes):
        if i:
            at += LANE_GROUP_GAP if i in breaks else LANE_GAP
        centers[i] = at + lane_size[i] / 2
        at += lane_size[i]
    lanes_end = at

    def lane_center(col):
        lo, hi = int(col // 1), -int(-col // 1)
        return centers[lo] + (centers[hi] - centers[lo]) * (col - lo)

    # the Role boxes share one lane of their own, past the last one
    role_minor = max(
        (minor_of(f'role_{b["key"]}', role_cell(b)) for b in flow if b["type"] == "band"),
        default=0,
    )
    role_center = lanes_end + ROLE_GAP + role_minor / 2
    minor_lo, minor_hi = MARGIN + top_room, role_center + role_minor / 2 + BAND_PAD

    pos, bands, stage_end, at = {}, [], {}, MARGIN
    for stage in flow:
        content = max(major_of(nid, cell) for nid, cell in stage["cells"].items())
        if stage["type"] == "band":
            content = max(content, major_of(f'role_{stage["key"]}', role_cell(stage)))
            start = at
            at += BAND_PAD + (0 if horizontal else BAND_TITLE_H)
            mid = at + content / 2
            pos[f'role_{stage["key"]}'] = place(role_center, mid)
            at += content + BAND_PAD
            bands.append(dict(stage=stage, box=place(minor_lo, start) + place(minor_hi, at)))
            stage_end[stage["key"]] = at
        else:
            mid = at + content / 2
            at += content
        for nid, cell in stage["cells"].items():
            pos[nid] = place(lane_center(cell["col"]) + cell.get("dminor", 0),
                             mid + cell.get("dmajor", 0))
        stage["_mid"] = mid
        at += stage.get("gap_after") or BAND_GAP

    stage_mid = {st["key"]: st["_mid"] for st in flow if st["type"] == "band"}
    for stage in tops:
        for nid, cell in stage["cells"].items():
            over = cell["align"]
            over = [over] if isinstance(over, str) else list(over)
            if cell.get("at") == "side":  # on the band's far border, level with a lane
                minor = lane_center(cell["col"]) + cell.get("dminor", 0)
                major = stage_end[over[0]]
            else:  # in the top strip, or on the band's leading border
                minor = minor_lo if cell.get("at") == "border" else MARGIN + top_h / 2
                major = sum(stage_mid[k] for k in over) / len(over) + cell.get("dmajor", 0)
            pos[nid] = place(minor, major)

    major_end = at - (flow[-1].get("gap_after") or BAND_GAP) + MARGIN
    minor_end = minor_hi + MARGIN
    width, height = (major_end, minor_end) if horizontal else (minor_end, major_end)
    return pos, bands, lane_center, stage_mid, width, height


# --- emission -------------------------------------------------------------------------------------
def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def html_lines(text):
    return '<BR/>'.join(esc(line) for line in text.split("\n"))


def xdot_text(x, y, text, size=11, color=FONT, font=FONTNAME, just=-1):
    """One xdot text operation (used for the background layer)."""
    raw = text.encode()
    return (
        f"F {size} {len(font)} -{font} c {len(color)} -{color} "
        f"T {x:.0f} {y:.0f} {just} {int(size * 0.6 * len(text))} {len(raw)} -{text}"
    )


def xdot_rect(x1, y1, x2, y2, color=STROKE):
    return (
        f"c {len(color)} -{color} "
        f"p 4 {x1:.0f} {y1:.0f} {x2:.0f} {y1:.0f} {x2:.0f} {y2:.0f} {x1:.0f} {y2:.0f}"
    )


def node_stmt(nid, spec, x, y):
    common = (
        f'fontname="{FONTNAME}", fontsize=10, fontcolor="{FONT}",'
        f' pos="{x:.0f},{y:.0f}", penwidth=1.2'
    )
    kind, label = spec["kind"], spec["label"]
    if kind == "waypoint":
        return f'  "{nid}" [shape=point, style=invis, width=0.01, pos="{x:.0f},{y:.0f}"];'
    if kind == "storage":
        return (
            f'  "{nid}" [label=<{html_lines(label)}>, shape=ellipse, style="filled",'
            f' fillcolor="{FILL}", color="{STROKE}", fixedsize=true,'
            f' width={spec["w"] / 72:.3f}, height={spec["h"] / 72:.3f}, {common}];'
        )
    if kind == "box":
        return (
            f'  "{nid}" [label=<{html_lines(label)}>, shape=box, style="filled",'
            f' fillcolor="{FILL}", color="{STROKE}", fixedsize=true,'
            f' width={spec["w"] / 72:.3f}, height={spec["h"] / 72:.3f}, {common}];'
        )
    if kind == "stack":
        def row(port):  # PORT lets an edge target this one box: "<id>:p3:w"
            return (
                f'<TR><TD PORT="p{port}" BGCOLOR="{FILL}" COLOR="{STROKE}"'
                f' WIDTH="{spec["w"]}" HEIGHT="20">{esc(label)}</TD></TR>'
            )
        # the "⋮" glyphs go in one cell, separated by <BR/>: as separate table rows the cell
        # padding would break them into visibly distinct clusters instead of one column of dots
        column = "<BR/>".join("⋮" for _ in range(spec["dots"]))
        gap = f'<TR><TD BORDER="0"><FONT COLOR="{MUTED}" POINT-SIZE="11">{column}</FONT></TD></TR>'
        rows = "".join(row(i) for i in range(spec["repeat"]))
        rows += gap + (row(spec["repeat"]) if spec["closing"] else "")
        return (
            f'  "{nid}" [label=<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="2" CELLPADDING="2">'
            f"{rows}</TABLE>>, shape=plaintext, {common}];"
        )
    raise ValueError(f"unknown node kind {kind!r}")


def role_stmt(nid, bullets, x, y):
    cells = []
    for b in bullets:
        lines = wrap(b, ROLE_CHARS)
        body = f"• {esc(lines[0])}" + "".join(
            f'<BR ALIGN="LEFT"/>   {esc(ln)}' for ln in lines[1:]
        )
        cells.append(f'<TR><TD ALIGN="LEFT" BALIGN="LEFT" WIDTH="{ROLE_W - 10}">{body}</TD></TR>')
    return (
        f'  "{nid}" [label=<<TABLE BORDER="1" COLOR="{STROKE}" CELLBORDER="0" CELLSPACING="0"'
        f' CELLPADDING="4" BGCOLOR="{BG}">{"".join(cells)}</TABLE>>, shape=plaintext,'
        f' fontname="{FONTNAME}", fontsize=10, fontcolor="{FONT}", pos="{x:.0f},{y:.0f}"];'
    )


def endpoint(ref):
    """Quote the node id of an edge endpoint, leaving any ":port:compass" suffix alone."""
    node, *port = ref.split(":")
    return ":".join([f'"{node}"'] + port)


def edge_stmt(src, dst, label, kind, attrs_extra=None):
    attrs = [f'color="{FONT}"']
    if kind == "thick":
        attrs.append("penwidth=2.2")
    elif kind == "dashed":
        attrs += ["penwidth=2.2", "style=dashed"]
    elif kind == "bus":
        attrs += ["penwidth=6", "arrowsize=0.8"]
    elif kind == "trunk":  # wider than a bus: the one path the bulk of the data travels
        attrs += ["penwidth=13", "arrowsize=1.1"]
    if label and kind == "dashed":
        # these fan in on one target, so a mid-edge label lands wherever the router bundles them;
        # pinning it to the tail keeps one label beside each source instead
        attrs += [f'taillabel="{label}"', "labeldistance=3.0", "labelangle=-22"]
    elif label:
        attrs.append(f'xlabel="{label}"')
    for key, value in (attrs_extra or {}).items():
        attrs.append(f'{key}="{value}"')
    return f"  {endpoint(src)} -> {endpoint(dst)} [{', '.join(attrs)}];"


def gen(model, horizontal, measured):
    pos, bands, lane_center, stage_mid, width, height = layout(model, horizontal, measured)
    up = lambda y: height - y  # noqa: E731  -- model is top-down, xdot/neato are bottom-up

    bg = []
    for b in bands:
        x1, y1, x2, y2 = b["box"]
        bg.append(xdot_rect(x1, up(y1), x2, up(y2)))
        bg.append(
            xdot_text(x1 + 10, up(y1) - BAND_TITLE_H + 6, b["stage"]["title"], size=11,
                      font="Helvetica-Bold")
        )
    for ann in model.get("annotations", []):
        if "at" in ann:  # pinned to a node, offset in screen points
            x, y = pos[ann["at"]]
            x, y = x + ann.get("dx", 0), y + ann.get("dy", 0)
        else:  # placed on the (lane, stage) grid
            major = stage_mid[ann["stage"]] + ann.get("dmajor", 0)
            x, y = ((major, lane_center(ann["col"])) if horizontal
                    else (lane_center(ann["col"]), major))
        bg.append(xdot_text(x, up(y), ann["text"], size=ann.get("size", 11),
                            color=ann.get("color", FONT), just=ann.get("just", 0)))
    bg.append(xdot_text(width / 2, up(MARGIN - 6), model["title"], size=13,
                        font="Helvetica-Bold", just=0))

    out = [
        "digraph G {",
        f'  bgcolor="{BG}"; splines=ortho; esep=6; forcelabels=true;',
        f'  graph [fontname="{FONTNAME}", fontsize=11, fontcolor="{FONT}"];',
        f'  node [fontname="{FONTNAME}", fontsize=10, fontcolor="{FONT}"];',
        f'  edge [fontname="{FONTNAME}", fontsize=9, fontcolor="{FONT}"];',
        f'  _background="{" ".join(bg)}";',
        "",
        "  // canvas corners, so the background layer is never clipped",
        '  "__tl" [shape=point, style=invis, width=0.01, pos="0,0"];',
        f'  "__br" [shape=point, style=invis, width=0.01, pos="{width:.0f},{height:.0f}"];',
        "",
    ]
    for stage in model["stages"]:
        for nid, spec in stage["cells"].items():
            x, y = pos[nid]
            out.append(node_stmt(nid, spec, x, up(y)))
        if stage["type"] == "band":
            rid = f'role_{stage["key"]}'
            x, y = pos[rid]
            out.append(role_stmt(rid, stage["role"], x, up(y)))
    out.append("")
    out += [edge_stmt(*e) for e in model["edges"]]
    out.append("}")
    return "\n".join(out) + "\n"


def measure(dot_source):
    """Ask Graphviz how big it actually draws each node (points).

    The HTML-table nodes — the stacks and the Role boxes — size themselves, so their height is
    only known after a render. Laying out from a guess leaves slack (or, worse, overlap), hence
    the two passes: lay out, measure, lay out again. Plain boxes and ellipses are fixedsize, so
    for those the measurement just confirms what was asked for.
    """
    plain = subprocess.run(
        ["neato", "-n2", "-Tplain"], input=dot_source, capture_output=True, text=True, check=True
    ).stdout
    sizes = {}
    for line in plain.splitlines():
        field = line.split()
        if field and field[0] == "node":
            sizes[field[1].strip('"')] = (float(field[4]) * 72, float(field[5]) * 72)
    return sizes


def render(models, dot_dir, png_dir, horizontal=None):
    """Render each model to `<dot_dir>/<name>.dot` and `<png_dir>/<name>.png`.

    Two passes: lay out from the declared sizes, measure what Graphviz actually drew, lay out again
    from the measurements. The second pass is what makes gaps exact — see `measure()`.
    """
    horizontal = ORIENTATION == "horizontal" if horizontal is None else horizontal
    written = []
    for model in models:
        dot = Path(dot_dir) / f'{model["name"]}.dot'
        png = Path(png_dir) / f'{model["name"]}.png'
        dot.write_text(gen(model, horizontal, measure(gen(model, horizontal, {}))))
        subprocess.run(["neato", "-n2", "-Tpng", str(dot), "-o", str(png)], check=True)
        written.append((dot, png))
        print(f"wrote {dot} and {png}")
    return written
