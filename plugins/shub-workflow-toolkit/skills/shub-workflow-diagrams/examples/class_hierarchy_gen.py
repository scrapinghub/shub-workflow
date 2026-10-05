#!/usr/bin/env python3
"""Worked example: a UML-style class-hierarchy diagram.

A hierarchy is a tree, so unlike a pipeline diagram it can be left to Graphviz's own ranking: `dot`
places a subclass below its base, which is exactly what you want and what it is good at. No fixed
positions, no `neato -n2`, no background layer — the whole file is this one model plus a renderer.

Copy it into your project's `diagrams/` folder, replace the model, and run it:

    python3 diagrams/class_hierarchy_gen.py     # writes the .dot here and the PNG one level up

Requires Graphviz (`dot`). Add a class to CLASSES and an edge to EDGES; everything else follows.
"""
import subprocess
from pathlib import Path

HERE = Path(__file__).parent  # diagrams/
ROOT = HERE.parent  # repo root: where the PNG the docs reference is published

NAME = "class_hierarchy"
TITLE = "example project — script class hierarchy"

BG = "#F4F1DE"  # same page colour as the pipeline diagrams, so the two read as one family
FONT = "#393C56"
FONTNAME = "Helvetica"

# A hierarchy needs categorical colours, one per family of classes, unlike the single-fill pipeline
# diagrams. group -> (legend title, fill)
GROUPS = {
    "base": ("Base classes", "#dae8fc"),
    "manager": ("Managers", "#d5e8d4"),
    "issuer": ("Issuers / consumers", "#fff2cc"),
    "mixin": ("Mixins", "#e1d5e7"),
}

# class id -> (label, group, [the methods this class ADDS]).  "∗" marks an abstract method.
CLASSES = {
    "BaseScript": ("BaseScript", "base", ["run() ∗", "schedule_script()", "get_jobs()"]),
    "BaseLoopScript": ("BaseLoopScript", "base", ["workflow_loop() ∗", "on_start()", "on_close()"]),
    "ExampleManager": ("ExampleManager", "manager", ["set_parameters_gen()", "bad_outcome_hook()"]),
    "ExamplePeriodic": ("ExamplePeriodic", "manager", ["workflow_loop()  (fixed cycle)"]),
    "ExampleIssuer": ("ExampleIssuer", "issuer", ["process_item()", "issue_item()", "send_file()"]),
    "ExampleIssuerFS": ("ExampleIssuerFS", "issuer", ["get_new_inputs()  (filesystem)"]),
    "AlertSenderMixin": ("AlertSenderMixin", "mixin", ["append_message()", "send_messages()"]),
}

# (base, subclass, kind) — "ext" = subclass (generalization), "mix" = combined in as a mixin (dashed)
EDGES = [
    ("BaseScript", "BaseLoopScript", "ext"),
    ("BaseScript", "AlertSenderMixin", "ext"),
    ("BaseLoopScript", "ExampleManager", "ext"),
    ("BaseLoopScript", "ExampleIssuer", "ext"),
    ("ExampleManager", "ExamplePeriodic", "ext"),
    ("ExampleIssuer", "ExampleIssuerFS", "ext"),
    ("AlertSenderMixin", "ExampleManager", "mix"),
]


def esc(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def uml_node(cid):
    """A class box: a coloured title row over a left-aligned list of the methods it adds."""
    label, group, methods = CLASSES[cid]
    fill = GROUPS[group][1]
    rows = "".join(f'+ {esc(m)}<BR ALIGN="LEFT"/>' for m in methods)
    return (
        f'  "{cid}" [label=<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="5">'
        f'<TR><TD BGCOLOR="{fill}"><B>{esc(label)}</B></TD></TR>'
        f'<TR><TD ALIGN="LEFT" BALIGN="LEFT">{rows}</TD></TR></TABLE>>];'
    )


def gen():
    legend = "".join(
        f'<TR><TD BGCOLOR="{fill}" ALIGN="LEFT">{esc(title)}</TD></TR>'
        for title, fill in GROUPS.values()
    )
    out = [
        "digraph G {",
        f'  bgcolor="{BG}";',
        "  rankdir=TB; nodesep=0.45; ranksep=0.85; splines=polyline;",
        f'  graph [fontname="{FONTNAME}", fontsize=13, fontcolor="{FONT}"];',
        f'  node [shape=plaintext, fontname="{FONTNAME}", fontsize=10, fontcolor="{FONT}"];',
        f'  labelloc="t"; label=<<B>{esc(TITLE)}</B>>;',
        f'  "legend" [label=<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="3">'
        f"<TR><TD><B>Categories</B></TD></TR>{legend}"
        f'<TR><TD ALIGN="LEFT"><I>∗ = abstract method</I></TD></TR></TABLE>>];',
        "",
    ]
    out += [uml_node(cid) for cid in CLASSES]
    out.append("")
    for base, sub, kind in EDGES:
        # UML generalization: a hollow triangle at the BASE end, so the edge is declared base -> sub
        # and drawn backwards. That also makes `dot` rank the base above its subclasses.
        style = ", style=dashed, color=\"#888888\"" if kind == "mix" else ', color="#333333"'
        out.append(f'  "{base}" -> "{sub}" [dir=back, arrowtail=onormal{style}];')
    out.append("}")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    dot, png = HERE / f"{NAME}.dot", ROOT / f"{NAME}.png"
    dot.write_text(gen())
    subprocess.run(["dot", "-Tpng", str(dot), "-o", str(png)], check=True)
    print(f"wrote {dot} and {png}")
