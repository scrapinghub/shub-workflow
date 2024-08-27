import logging
from pprint import pformat
from typing import List, Dict, Any

from spidermon.contrib.actions.sentry import SendSentryMessage

from shub_workflow.script import BaseScriptProtocol
from shub_workflow.utils import resolve_shub_jobkey
from shub_workflow.utils.monitor import BaseMonitorProtocol


LOG = logging.getLogger(__name__)


class SentryMixin(BaseScriptProtocol):
    """
    A class for adding sentry alert capabilities to a shub_workflow class.
    """

    def __init__(self):
        super().__init__()
        self.sentry_handler = SendSentryMessage(
            fake=self.project_settings.get("SPIDERMON_SENTRY_FAKE"),
            sentry_dsn=self.project_settings.get("SPIDERMON_SENTRY_DSN"),
            sentry_log_level=self.project_settings.get("SPIDERMON_SENTRY_LOG_LEVEL"),
            project_name=self.project_settings.get("SPIDERMON_SENTRY_PROJECT_NAME"),
            environment=self.project_settings.get("SPIDERMON_SENTRY_ENVIRONMENT_TYPE"),
        )
        self.messages: List[str] = []

    def send_sentry_message(self):
        if self.messages:
            message: Dict[str, Any] = dict()
            title = f"{self.sentry_handler.project_name} | {self.sentry_handler.environment} | Monitor notification"
            message["title"] = title
            message["failure_reasons"] = "/n".join(self.messages)
            job_key = resolve_shub_jobkey()
            if job_key:
                message["job_link"] = f"https://app.zyte.com/p/{job_key}"
            if self.sentry_handler.fake:
                message["failure_reasons"] = self.messages
                LOG.info(pformat(message))
            else:
                self.sentry_handler.send_message(message)

    def append_message(self, message: str):
        self.messages.append(message)


class MonitorSentryMixin(SentryMixin, BaseMonitorProtocol):
    """
    Mixin for adding sentry capabilities to shub_workflow monitors.
    """
    def close(self):
        super().close()  # type: ignore
        self.send_sentry_message()
