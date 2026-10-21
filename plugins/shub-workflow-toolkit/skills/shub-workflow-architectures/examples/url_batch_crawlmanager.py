"""
Url frontier: the crawl manager converts frontier urls into batch files, one at a time, and feeds each spider job with
the path of one file (spider argument input_batch_file). Use it only when the work can't be reduced to short seeds.

    frontier slot ──► pending/<spider>/ ──claim──► processing/<spider>/ ──► spider job

Order of operations = guarantees: frontier batches are deleted only after the file is written (an interruption
duplicates urls, never loses them); pending files are scheduled before reading the frontier again (nothing
orphaned); a failed job's file goes back to pending (its work is retried without touching the frontier).
"""
import gzip
import json
import logging
import os
import tempfile
from datetime import datetime

from hcf_backend.manager import HCFManager
from shub_workflow.crawl import GeneratorCrawlManager

from myproject.common import BUCKET, get_slotname

LOGGER = logging.getLogger(__name__)


class UrlsCrawlManager(GeneratorCrawlManager):

    name = "urls_crawlmanager"
    loop_mode = 60
    default_max_jobs = 10
    acquire_all_jobs = True
    dont_acquire_finished_jobs = True

    urls_per_job = 5000
    batches_folder = os.path.join(BUCKET, "url_batches")

    def __init__(self):
        super().__init__()
        self.hcf = HCFManager(self.project_settings["FRONTIER_NAME"], project_id=self.project_id)
        self.slot = get_slotname(self.get_canonical_spidername(self.spider), "urls")
        self.pending_folder = os.path.join(self.batches_folder, "pending", self.spider)
        self.processing_folder = os.path.join(self.batches_folder, "processing", self.spider)

    def get_owned_jobs(self, project_id=None, **kwargs):
        yield from self.get_jobs(project_id, spider=self.spider, **kwargs)

    def set_parameters_gen(self):
        while True:
            pending = list(self.fshelper.list_path(os.path.join(self.pending_folder, "")))
            if not pending and not self.frontier_to_batch_file():
                break
            for fname in pending:
                dest = os.path.join(self.processing_folder, os.path.basename(fname))
                self.fshelper.mv_file(fname, dest)  # the claim
                yield {"input_batch_file": dest}

    def frontier_to_batch_file(self) -> bool:
        urls, batch_ids = [], []
        for batch in self.hcf.read(self.slot, self.urls_per_job):
            urls.extend((qdata or {}).get("url", fp) for fp, qdata in batch["requests"])
            batch_ids.append(batch["id"])
        if not urls:
            return False
        name = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S.%f')}.jl.gz"
        local = os.path.join(tempfile.mkdtemp(), name)
        with gzip.open(local, "wt") as f:
            for url in urls:
                f.write(json.dumps({"url": url}) + "\n")
        self.fshelper.mv_file(local, os.path.join(self.pending_folder, name))
        self.hcf.delete(self.slot, batch_ids)  # only once the file is safely written
        LOGGER.info(f"Wrote a batch of {len(urls)} urls from slot {self.slot}.")
        return True

    @staticmethod
    def _batch_file(job_args_override):
        return (job_args_override.get("spider_args") or {}).get("input_batch_file")

    def finished_ok_hook(self, spider, outcome, job_args_override, jobkey):
        if fname := self._batch_file(job_args_override):
            self.fshelper.rm_file(fname)

    def bad_outcome_hook(self, spider, outcome, job_args_override, jobkey):
        # back to pending: another job will crawl these urls (no MAX_RETRIES needed)
        if (fname := self._batch_file(job_args_override)) and self.fshelper.exists(fname):
            self.fshelper.mv_file(fname, os.path.join(self.pending_folder, os.path.basename(fname)))


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    UrlsCrawlManager().run()
