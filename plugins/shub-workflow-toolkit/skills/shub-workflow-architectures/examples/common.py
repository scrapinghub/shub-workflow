"""
Shared definitions of the workflow scripts. Place it in the project package (e.g. myproject/common.py) and
import it from every script, so that readers and writers of each storage agree on names and paths.

Also add to the root __init__.py of the project package:

    logging.getLogger("hcf_backend.manager").setLevel(logging.ERROR)
"""
import os

# TODO: one bucket/prefix per environment (dev, prod), e.g. selected from a ScrapyCloud project setting
BUCKET = "gs://mybucket/myproject"

CONSUMER_OUTPUT = os.path.join(BUCKET, "consumer")  # partitioned items: <slot>_<source>_<timestamp>.jl.gz
DEDUP_OUTPUT = os.path.join(BUCKET, "deduped")
DOWNLOAD_QUEUE = os.path.join(BUCKET, "download_queue")  # batches waiting for a downloader job
DOWNLOADING = os.path.join(BUCKET, "downloading")  # batches claimed by a downloader job
DELIVERY = "s3://customer-bucket/deliveries"

# number of slots of the consumer output = number of deduplicators
NUM_SLOTS = 10


def get_slotname(canonical_name: str, seeds_field: str) -> str:
    """The frontier slot of a source and kind of seed. Used by both its reader and its single writer."""
    return f"{canonical_name}_{seeds_field}"
