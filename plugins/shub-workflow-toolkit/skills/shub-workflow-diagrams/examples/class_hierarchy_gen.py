#!/usr/bin/env python3
"""Worked example: the class-hierarchy diagram of a shub-workflow project.

A hierarchy is a tree, so unlike a pipeline diagram it can be left to Graphviz's own ranking: `dot`
places a subclass below its base, which is exactly what you want and what it is good at. No fixed
positions, no `neato -n2`, no background layer — the whole file is this one model plus a renderer.

Copy it into your project's `diagrams/` folder, replace the model (keep GROUPS as is), and run it:

    python3 diagrams/class_hierarchy_gen.py     # writes the .dot here and the PNG one level up

Every render also checks EDGES against the bases the classes declare in SOURCES, and exits non-zero
listing any drift (a missing or extra edge, a drawn class that is gone, a new subclass not drawn).
The conventions this model follows are in the skill's references/class-hierarchy.md.

Requires Graphviz (`dot`).
"""
import ast
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent  # diagrams/
ROOT = HERE.parent  # repo root: where the PNG the docs reference is published

NAME = "class_hierarchy"
TITLE = "myproject — script and spider class hierarchy"

# The project files whose classes are drawn: the scripts package and the module of the base spiders.
SOURCES = ["myproject/scripts", "myproject/spiders/__init__.py"]

BG = "#F4F1DE"  # same page colour as the pipeline diagrams, so the two read as one family
FONT = "#393C56"
FONTNAME = "Helvetica"

# The same categories and fills in every project; leave out a group the project has no class in.
# group -> (legend title, fill)
GROUPS = {
    "ext": ("shub-workflow / Scrapy", "#dae8fc"),
    "mixin": ("Project mixins", "#e1d5e7"),
    "manager": ("Crawl managers", "#d5e8d4"),
    "issuer": ("Issuers (consumers, dedup, delivery)", "#fff2cc"),
    "script": ("Plain scripts, monitor, tools", "#ffe6cc"),
    "spider": ("Spiders", "#f8cecc"),
}

# class id -> (label, group, [the members this class ADDS]).  "∗" marks an abstract member.
# The id is the class name exactly as the project's code writes it in a bases list (an import alias
# included), which is what lets check() compare the model with the code.
CLASSES = {
    # --- shub-workflow / Scrapy: only the bases the project's classes reach ---------------------
    "BaseScript": ("BaseScript", "ext", [
        "run() ∗", "schedule_script()", "schedule_spider()", "get_jobs()",
    ]),
    "BaseLoopScript": ("BaseLoopScript", "ext", ["workflow_loop() ∗", "on_start()", "on_close()"]),
    "WorkFlowManager": ("WorkFlowManager", "ext", ["workflow_loop()  (job bookkeeping)"]),
    "CrawlManager": ("CrawlManager", "ext", ["max_running_jobs", "bad_outcome_hook()"]),
    "GeneratorCrawlManager": ("GeneratorCrawlManager", "ext", ["set_parameters_gen() ∗"]),
    "HCFSeedsCrawlManager": ("HCFSeedsCrawlManager", "ext", [
        "ENABLED_SPIDERS  (per-spider config)", "get_base_slotname() / get_slotname()",
        "read_seeds() / process_seeds()", "on_empty_frontier()",
    ]),
    "IssuerScript": ("IssuerScript", "ext", [
        "build_item_id() ∗", "get_new_inputs() ∗", "process_input() ∗", "process_item()",
        "issue_item()", "load_last_outputs()",
    ]),
    "IssuerScriptWithSCJobInput": ("IssuerScriptWithSCJobInput", "ext", [
        "get_new_inputs()  (finished SC jobs)", "post_process_input_items()",
    ]),
    "IssuerScriptWithFileSystemInput": ("IssuerScriptWithFileSystemInput", "ext", [
        "get_new_inputs()  (a bucket folder)",
    ]),
    "HCFSeedsWriterMixin": ("HCFSeedsWriterMixin", "ext", ["write_seeds()  (thread per slot)"]),
    "SpiderStatsAggregatorMixin": ("SpiderStatsAggregatorMixin", "ext", ["aggregate_spider_stats()"]),
    "AlertSenderMixin": ("AlertSenderMixin", "ext", ["append_message()", "send_messages()"]),
    "SlackMixin": ("SlackMixin", "ext", ["send to a Slack channel"]),
    "BaseMonitor": ("BaseMonitor", "ext", ["stats_postprocessing()", "generate_report()"]),
    "FrontierTool": ("FrontierTool", "ext", ["frontier_name", "list / dump / move / delete slots"]),
    "Spider": ("scrapy.Spider", "ext", ["start_requests()", "parse()"]),

    # --- the project ---------------------------------------------------------------------------
    "MyProjectScriptMixin": ("MyProjectScriptMixin", "mixin", [
        "init_fshelper()", "get_seeds_slotname()", "get_<stage>_output_folder()",
    ]),
    "MyCrawlManager": ("MyCrawlManager", "manager", ["ENABLED_SPIDERS", "get_base_slotname()"]),
    "MyConsumer": ("MyConsumer", "issuer", [
        "process_item()", "post_process_input_items()  (seeds → frontier)",
    ]),
    "MyDeduplicator": ("MyDeduplicator", "issuer", ["one job per slot", "bloom filter over past outputs"]),
    "Scheduler": ("Scheduler", "script", ["run()  (keeps the long-running jobs alive)"]),
    "Monitor": ("Monitor", "script", ["generate_report()", "… alert hooks"]),
    "MyFrontierTool": ("MyFrontierTool", "script", ["frontier_name = FRONTIER_NAME"]),
    "BaseMySpider": ("BaseMySpider", "spider", [
        "canonical_name", "__init_subclass__()  (checks it)", "make_search_request() ∗",
    ]),
}

# (base, subclass, kind). "ext" = the substantive base, drawn solid; "mix" = combined in alongside
# it, drawn dashed — every *Mixin, wherever it sits in the bases list.
EDGES = [
    # shub-workflow's own tree, as far as the project reaches into it
    ("BaseScript", "BaseLoopScript", "ext"),
    ("BaseLoopScript", "WorkFlowManager", "ext"),
    ("WorkFlowManager", "CrawlManager", "ext"),
    ("SpiderStatsAggregatorMixin", "CrawlManager", "mix"),
    ("CrawlManager", "GeneratorCrawlManager", "ext"),
    ("GeneratorCrawlManager", "HCFSeedsCrawlManager", "ext"),
    ("BaseLoopScript", "IssuerScript", "ext"),
    ("IssuerScript", "IssuerScriptWithSCJobInput", "ext"),
    ("IssuerScript", "IssuerScriptWithFileSystemInput", "ext"),
    ("BaseScript", "AlertSenderMixin", "ext"),
    ("AlertSenderMixin", "SlackMixin", "ext"),
    ("BaseScript", "BaseMonitor", "ext"),
    ("AlertSenderMixin", "BaseMonitor", "mix"),
    ("SpiderStatsAggregatorMixin", "BaseMonitor", "mix"),
    ("BaseScript", "FrontierTool", "ext"),

    # the project
    ("HCFSeedsCrawlManager", "MyCrawlManager", "ext"),
    ("MyProjectScriptMixin", "MyCrawlManager", "mix"),
    ("IssuerScriptWithSCJobInput", "MyConsumer", "ext"),
    ("MyProjectScriptMixin", "MyConsumer", "mix"),
    ("HCFSeedsWriterMixin", "MyConsumer", "mix"),
    ("IssuerScriptWithFileSystemInput", "MyDeduplicator", "ext"),
    ("MyProjectScriptMixin", "MyDeduplicator", "mix"),
    ("BaseScript", "Scheduler", "ext"),
    ("SlackMixin", "Scheduler", "mix"),
    ("BaseMonitor", "Monitor", "ext"),
    ("SlackMixin", "Monitor", "mix"),
    ("MyProjectScriptMixin", "Monitor", "mix"),
    ("FrontierTool", "MyFrontierTool", "ext"),
    ("Spider", "BaseMySpider", "ext"),
]


def esc(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def uml_node(cid):
    """A class box: a coloured title row over a left-aligned list of the members it adds."""
    label, group, members = CLASSES[cid]
    fill = GROUPS[group][1]
    rows = "".join(f'+ {esc(m)}<BR ALIGN="LEFT"/>' for m in members)
    return (
        f'  "{cid}" [label=<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="5">'
        f'<TR><TD BGCOLOR="{fill}"><B>{esc(label)}</B></TD></TR>'
        f'<TR><TD ALIGN="LEFT" BALIGN="LEFT">{rows}</TD></TR></TABLE>>];'
    )


def gen():
    used = {group for _, group, _ in CLASSES.values()}
    legend = "".join(
        f'<TR><TD BGCOLOR="{fill}" ALIGN="LEFT">{esc(title)}</TD></TR>'
        for group, (title, fill) in GROUPS.items()
        if group in used
    )
    out = [
        "digraph G {",
        f'  bgcolor="{BG}";',
        "  rankdir=TB; nodesep=0.40; ranksep=0.80; splines=polyline;",
        f'  graph [fontname="{FONTNAME}", fontsize=13, fontcolor="{FONT}"];',
        f'  node [shape=plaintext, fontname="{FONTNAME}", fontsize=10, fontcolor="{FONT}"];',
        f'  labelloc="t"; label=<<B>{esc(TITLE)}</B>>;',
        f'  "legend" [label=<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="3">'
        f"<TR><TD><B>Categories</B></TD></TR>{legend}"
        f'<TR><TD ALIGN="LEFT"><I>∗ = abstract member</I></TD></TR>'
        f'<TR><TD ALIGN="LEFT"><I>dashed = mixed in</I></TD></TR></TABLE>>];',
        "",
    ]
    out += [uml_node(cid) for cid in CLASSES]
    out.append("")
    for base, sub, kind in EDGES:
        # UML generalization: a hollow triangle at the BASE end, so the edge is declared base -> sub
        # and drawn backwards. That also makes `dot` rank the base above its subclasses.
        style = ', style=dashed, color="#888888"' if kind == "mix" else ', color="#333333"'
        out.append(f'  "{base}" -> "{sub}" [dir=back, arrowtail=onormal{style}];')
    out.append("}")
    return "\n".join(out) + "\n"


# Bases that say nothing about what a class does, so they are never drawn (nor are *Protocol ones).
IGNORED_BASES = {"", "object", "Generic", "ABC", "TypedDict"}


def base_name(node):
    """`Name` -> name, `module.Name` -> Name, `Name[T]` -> Name."""
    if isinstance(node, ast.Subscript):
        return base_name(node.value)
    if isinstance(node, ast.Attribute):
        return node.attr
    return node.id if isinstance(node, ast.Name) else ""


def check():
    """The differences between EDGES and the bases the classes in SOURCES declare."""
    declared = {}  # class name -> the names in its bases list
    problems = []
    for src in SOURCES:
        path = ROOT / src
        if not path.exists():
            problems.append(f"source {src} not found")
            continue
        for pyfile in [path] if path.is_file() else sorted(path.rglob("*.py")):
            for node in ast.walk(ast.parse(pyfile.read_text())):
                if isinstance(node, ast.ClassDef):
                    declared[node.name] = {base_name(b) for b in node.bases}
    drawn = {(base, sub) for base, sub, _ in EDGES}
    for cid, (_, group, _) in CLASSES.items():
        if cid not in declared:
            if group != "ext":
                problems.append(f"{cid} is drawn but not defined in SOURCES")
            continue
        for base in sorted(declared[cid] - IGNORED_BASES):
            if base.endswith("Protocol"):
                continue
            if base not in CLASSES:
                problems.append(f"{cid} derives from {base}, which is not drawn")
            elif (base, cid) not in drawn:
                problems.append(f"missing edge {base} -> {cid}")
        for base, sub in sorted(drawn):
            if sub == cid and base not in declared[cid]:
                problems.append(f"edge {base} -> {cid} is not in the code")
    for name, bases in sorted(declared.items()):
        if name not in CLASSES and bases & CLASSES.keys():
            problems.append(f"{name} derives from {', '.join(sorted(bases & CLASSES.keys()))} but is not drawn")
    return problems


if __name__ == "__main__":
    dot, png = HERE / f"{NAME}.dot", ROOT / f"{NAME}.png"
    dot.write_text(gen())
    subprocess.run(["dot", "-Tpng", str(dot), "-o", str(png)], check=True)
    print(f"wrote {dot} and {png}")
    if problems := check():
        print("The model disagrees with the code:", *problems, sep="\n  ")
        sys.exit(1)
