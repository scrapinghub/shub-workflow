import logging
from pprint import pformat

from scrapy.signals import item_scraped

from shub_workflow.script import BaseScriptProtocol


LOGGER = logging.getLogger(__name__)


class ItemHSIssuerMixin(BaseScriptProtocol):
    """
    A class for allowing to issue items on hubstorage, so a script running on SC can return items as a spider.
    """

    def __init__(self):
        super().__init__()
        try:
            from sh_scrapy.extension import HubstorageExtension

            class _HubstorageExtension(HubstorageExtension):
                def item_scraped(slf, item, spider):
                    try:
                        return super().item_scraped(item, spider)
                    except RuntimeError:
                        self.log_item(item, spider)

            self.hextension = _HubstorageExtension.from_crawler(self._pseudo_crawler)
        except ImportError:
            self._pseudo_crawler.signals.connect(self.log_item, item_scraped)

    def log_item(self, item, spider, **kwargs):
        LOGGER.info(pformat(item))

    def hs_issue_item(self, item):
        self._pseudo_crawler.signals.send_catch_log_deferred(item_scraped, dont_log=True, item=item, spider=self)
