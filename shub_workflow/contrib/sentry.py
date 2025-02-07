import logging
from pprint import pformat
from typing import Dict, Any

from shub_workflow.utils import resolve_shub_jobkey
from shub_workflow.utils.alert_sender import AlertSenderMixin


LOG = logging.getLogger(__name__)


class SentryMixin(AlertSenderMixin):
    """
    A class for adding sentry alert capabilities to a shub_workflow class.
    """

    def __init__(self):
        super().__init__()
        try:
            from spidermon.contrib.actions.sentry import SendSentryMessage
        except ImportError:
            raise ImportError("spidermon[sentry-sdk] is required for using SentryMixin.")
        self.sentry_handler = SendSentryMessage(
            fake=self.project_settings.getbool("SPIDERMON_SENTRY_FAKE"),
            sentry_dsn=self.project_settings.get("SPIDERMON_SENTRY_DSN"),
            sentry_log_level=self.project_settings.get("SPIDERMON_SENTRY_LOG_LEVEL"),
            project_name=self.project_settings.get("SPIDERMON_SENTRY_PROJECT_NAME"),
            environment=self.project_settings.get("SPIDERMON_SENTRY_ENVIRONMENT_TYPE"),
        )
        self.register_sender_method(self.send_sentry_message)

    def send_sentry_message(self):
        if self.messages:
            message: Dict[str, Any] = dict()
            title = f"{self.sentry_handler.project_name} | {self.sentry_handler.environment} | {self.args.subject}"
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
