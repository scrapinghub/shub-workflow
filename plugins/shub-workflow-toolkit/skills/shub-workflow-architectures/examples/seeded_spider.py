"""
Spiders fed with seeds by a crawl manager. They know nothing about the frontier or the workflow: they receive a
JSON list of seeds and yield items, which carry the new seeds the consumer will write back to the frontier.

Test standalone:  scrapy crawl mysite.com_tags -a seeds='["python", "scraping"]'
"""
import json
from typing import Tuple

from scrapy import Request, Spider


class SeededSpider(Spider):

    # the source: shared by all the spiders of a site; used in slot names, item sources and file names
    canonical_name: str = ""
    # the kind of seed this spider reads: its slot is get_slotname(canonical_name, seeds_source)
    seeds_source: str = ""
    # item fields carrying new seeds, harvested by the consumer (each field feeds the slot of that kind)
    seeds_fields: Tuple[str, ...] = ()

    def __init_subclass__(cls, **kwargs):
        # fail at import time (every scrapy command) instead of misbehaving in production
        super().__init_subclass__(**kwargs)
        if getattr(cls, "name", None):
            assert cls.canonical_name, f"{cls.__name__}: canonical_name is required"
            assert cls.seeds_source, f"{cls.__name__}: seeds_source is required"

    def __init__(self, seeds: str = "[]", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seeds = json.loads(seeds)

    def start_requests(self):
        for seed in self.seeds:
            yield self.make_seed_request(seed)

    def make_seed_request(self, seed: str) -> Request:
        raise NotImplementedError


class MySiteTagsSpider(SeededSpider):

    name = "mysite.com_tags"
    canonical_name = "mysite.com"
    seeds_source = "tags"
    seeds_fields = ("tags", "authors")  # cross seeding: tag listings also reveal authors

    def make_seed_request(self, seed: str) -> Request:
        return Request(f"https://mysite.com/tag/{seed}", callback=self.parse_listing)

    def parse_listing(self, response):
        # TODO: yield one item per entry, with its "tags" and "authors"; follow the next page
        ...
