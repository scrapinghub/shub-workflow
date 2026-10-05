#!/usr/bin/env python3
"""Worked example: a pipeline diagram for a shub-workflow project.

Copy this next to `diagram_engine.py` in your project's `diagrams/` folder, replace the model with
your own, and run it. The engine is never edited; everything you change lives in this file.

    python3 diagrams/workflow_gen.py     # writes diagrams/*.dot and the published PNGs

Requires Graphviz (`neato`). The model vocabulary is in the skill's references/model-reference.md.
"""
from pathlib import Path

from diagram_engine import (
    MUTED, band, border, box, free, pile, render, stack, storage, top, waypoint,
)

HERE = Path(__file__).parent  # diagrams/
ROOT = HERE.parent  # repo root: where the PNGs the README references are published

# One crawlmanager, one spider stack and one consumer per source, each in its own lane. Driving the
# repetitive bands from one list means adding a source is a one-line edit.
SOURCES = [("site-a", 0), ("site-b", 1), ("site-c", 2)]
BUSIEST = "site-a"  # the one source scaled out across several parallel jobs


def per_source(prefix, make):
    return {f"{prefix}_{name}": make(name, lane) for name, lane in SOURCES}


WORKFLOW = {
    "name": "workflow",
    "title": "example project — crawl and delivery workflow",
    "stages": [
        # Nodes parked above the bands: they cost nothing along the flow axis, which is how a
        # storage a stage both reads and writes stays out of the column sequence.
        top(
            {
                "frontier": storage("seeds frontier\n(hubstorage)", align="spiders"),
                "delivered": storage("delivery bucket", align="delivery"),
                # the frontier talks to whole stages, so its arrows land on a container border
                "wp_frontier": waypoint(align="crawlmanagers"),
                "border_crawlmanagers": border(align="crawlmanagers"),
                "border_consumer": border(align="consumer"),
                "wp_seeds_written": waypoint(align="consumer"),
            }
        ),
        band(
            "crawlmanagers",
            "CRAWLMANAGERS",
            per_source("cm", lambda name, lane: box(name, col=lane)),
            role=["Read seeds", "Schedule spiders"],
        ),
        band(
            "spiders",
            "SPIDER JOBS",
            per_source(
                "sp",
                lambda name, lane: (
                    stack(name, col=lane)
                    if name == BUSIEST
                    else stack(name, col=lane, repeat=1, closing=False)
                ),
            ),
            role=["Discover urls", "Drop what the customer filters out", "Yield items and seeds"],
        ),
        band(
            "consumer",
            "CONSUMER",
            per_source(
                "co",
                lambda name, lane: (
                    stack("consumer", col=lane, repeat=3)
                    if name == BUSIEST
                    else box("consumer", col=lane)
                ),
            ),
            role=[
                "Consume items from spider jobs",
                "Cheap in-memory dedupe",
                "Extract and write seeds back to the frontier",
                "Assign slots by a hash of the record id",
                "Output batches",
            ],
        ),
        free(
            {
                "slots": storage("GCS (10 slots)", col=1),
                # corner for the busiest consumer's output: across first, then down into the cloud
                "wp_slots": waypoint(col=0),
                # deduplicator 1 and 10 turn on the same vertical, one up and one down
                "wp_dedup_first": waypoint(col=0, dmajor=60),
                "wp_dedup_last": waypoint(col=2, dmajor=60),
            }
        ),
        band(
            "deduplicators",
            "DEDUPLICATORS",
            {
                "dedup_first": box("deduplicator 1", col=0),
                "dedup_last": box("deduplicator 10", col=2),
            },
            role=["Massive persistent dedupe, one bloom filter per slot", "10 parallel jobs"],
        ),
        band(
            "delivery",
            "DELIVERY",
            pile("dl", "deliver job", col=0, count=4, gaps=(2,)),
            role=["Read deduplicated batches", "Write the customer's files"],
        ),
    ],
    "edges": [
        # the frontier exchanges seeds with whole stages, so both arrows are buses on a border
        ("frontier", "wp_frontier", "", "bus", {"arrowhead": "none"}),
        ("wp_frontier", "border_crawlmanagers", "", "bus"),
        ("border_consumer", "wp_seeds_written", "", "bus", {"arrowhead": "none"}),
        ("wp_seeds_written", "frontier", "", "bus"),
        *[(f"cm_{n}", f"sp_{n}", "search seeds", "thin") for n, _ in SOURCES],
        *[(f"sp_{n}", f"co_{n}", "", "thick") for n, _ in SOURCES],
        (f"co_{BUSIEST}", "wp_slots", "x10", "dashed", {"arrowhead": "none"}),
        ("wp_slots", "slots", "", "dashed"),
        *[(f"co_{n}", "slots", "x10", "dashed") for n, _ in SOURCES if n != BUSIEST],
        ("slots", "wp_dedup_first", "", "thick", {"arrowhead": "none"}),
        ("wp_dedup_first", "dedup_first", "", "thick"),
        ("slots", "wp_dedup_last", "", "thick", {"arrowhead": "none"}),
        ("wp_dedup_last", "dedup_last", "", "thick"),
        ("dedup_first", "dl0", "", "thick"),
        ("dedup_last", "dl3", "", "thick"),
        ("dl1", "delivered", "", "trunk"),
    ],
    # The continuity dots standing in for the slots not drawn. `dmajor` nudges them off the band's
    # centre line, where the edge turning at wp_dedup_last would otherwise run straight through them.
    "annotations": [
        {"text": "⋮", "col": 1, "stage": "deduplicators", "size": 14, "color": MUTED, "dmajor": -45},
    ],
}


if __name__ == "__main__":
    render([WORKFLOW], dot_dir=HERE, png_dir=ROOT)
