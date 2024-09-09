import logging
from pprint import pformat
from typing import Any, Dict, List

from spidermon.contrib.actions.slack import SlackMessageManager

from shub_workflow.script import BaseScriptProtocol
from shub_workflow.utils import resolve_shub_jobkey
from shub_workflow.utils.monitor import BaseMonitorProtocol

LOG = logging.getLogger(__name__)


class SlackMixin(BaseScriptProtocol):
    """
    A class for adding slack alert capabilities to a shub_workflow class.
    """

    def __init__(self):
        super().__init__()
        self.slack_handler = SlackMessageManager(
            fake=self.project_settings.getbool("SPIDERMON_SLACK_FAKE"),
            sender_token=self.project_settings.get("SPIDERMON_SLACK_SENDER_TOKEN"),
            sender_name=self.project_settings.get("SPIDERMON_SLACK_SENDER_NAME"),
        )
        self.messages: List[str] = []

    def send_slack_message(self):
        if self.messages:
            message: Dict[str, Any] = dict()
            title = f"{self.slack_handler.sender_name} | Monitor notification"
            message["title"] = title
            message["failure_reasons"] = "\n".join(self.messages)
            job_key = resolve_shub_jobkey()
            if job_key:
                message["job_link"] = f"https://app.zyte.com/p/{job_key}"
            text = (
                f"{title}\n\n"
                "Failure Reasons:\n"
                f"{message['failure_reasons']}\n\n"
                f"Job Link: {message.get('job_link', 'N/A')}"
            )
            to = self.project_settings.get("SPIDERMON_SLACK_RECIPIENTS")
            if self.slack_handler.fake:
                message["failure_reasons"] = self.messages
                LOG.info(pformat(message))
            else:
                self.slack_handler.send_message(to, text)

    def append_message(self, message: str):
        self.messages.append(message)


class MonitorSlackMixin(SlackMixin, BaseMonitorProtocol):
    """
    Mixin for adding slack capabilities to shub_workflow monitors.
    """

    def close(self):
        super().close()  # type: ignore
        self.send_slack_message()
