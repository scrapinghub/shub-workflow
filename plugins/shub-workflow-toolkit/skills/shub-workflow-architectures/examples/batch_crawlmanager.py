"""
Crawl manager feeding spiders with batch files from a file queue (typically a downloader: the batches come from a
balancer or a deduplicator). It claims a file by MOVING it into the processing folder before scheduling the job that
reads it, so a file is never scheduled twice, even across restarts. It rotates over the sources, one batch per
source per turn, so a source with a big backlog can't monopolize the jobs.

Never defer a job (can_schedule_job_with_params) after the claim: decide before moving the file.
"""
import logging
import os
from itertools import cycle

from shub_workflow.crawl import GeneratorCrawlManager

from myproject.common import DOWNLOAD_QUEUE, DOWNLOADING

LOGGER = logging.getLogger(__name__)

ENABLED_SOURCES = ("mysite.com", "othersite.com")  # batch file names start with the source


class DownloaderCrawlManager(GeneratorCrawlManager):

    name = "downloader_crawlmanager"
    loop_mode = 60
    default_max_jobs = 8
    spider = "downloader"
    acquire_all_jobs = True
    dont_acquire_finished_jobs = True

    def source_is_available(self, source: str) -> bool:
        # TODO: per-source limits decided BEFORE claiming (e.g. one job at a time for logged-in sources)
        return True

    def set_parameters_gen(self):
        exhausted = set()
        for source in cycle(ENABLED_SOURCES):
            if len(exhausted) == len(ENABLED_SOURCES):
                break
            if self.source_is_available(source):
                for fname in self.fshelper.list_path(os.path.join(DOWNLOAD_QUEUE, f"{source}_")):
                    dest = os.path.join(DOWNLOADING, os.path.basename(fname))
                    self.fshelper.mv_file(fname, dest)  # the claim
                    yield {"input_batch_file": dest, "units": 2}
                    exhausted.discard(source)
                    break
                else:
                    exhausted.add(source)
            else:
                exhausted.add(source)


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    DownloaderCrawlManager().run()
