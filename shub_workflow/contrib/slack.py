import logging
from pprint import pformat
from typing import Any, Dict

from spidermon.contrib.actions.slack import SlackMessageManager

from shub_workflow.utils import resolve_shub_jobkey
from shub_workflow.utils.alert_sender import AlertSenderMixin

LOG = logging.getLogger(__name__)


class SlackMixin(AlertSenderMixin):
    """
    A class for adding slack alert capabilities to a shub_workflow class.
    """

    def __init__(self):
        super().__init__()
        self.slack_handler = SlackMessageManager(
            fake=self.project_settings.getbool("SPIDERMON_SLACK_FAKE"),
            sender_token=self.project_settings.get("SPIDERMON_SLACK_SENDER_TOKEN"),
            sender_name=self.args.sender_name or self.project_settings.get("SPIDERMON_SLACK_SENDER_NAME"),
        )
        self.register_sender_method(self.send_slack_message)

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
