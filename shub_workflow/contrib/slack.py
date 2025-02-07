import logging
from pprint import pformat
from typing import Any, Dict, List

from shub_workflow.utils import resolve_shub_jobkey
from shub_workflow.utils.alert_sender import AlertSenderMixin

LOG = logging.getLogger(__name__)


class SlackSender:
    def __init__(self, project_settings):
        super().__init__()
        try:
            from spidermon.contrib.actions.slack import SlackMessageManager
        except ImportError:
            raise ImportError("spidermon[slack-sdk] is required for using SlackSender")
        self.slack_handler = SlackMessageManager(
            fake=project_settings.getbool("SPIDERMON_SLACK_FAKE"),
            sender_token=project_settings.get("SPIDERMON_SLACK_SENDER_TOKEN"),
            sender_name=project_settings.get("SPIDERMON_SLACK_SENDER_NAME"),
        )
        self.recipients = project_settings.get("SPIDERMON_SLACK_RECIPIENTS")

    def send_slack_messages(self, messages: List[str], subject: str, attachments=None):
        message: Dict[str, Any] = dict()
        title = f"{self.slack_handler.sender_name} | {subject}"
        message["title"] = title
        message["failure_reasons"] = "\n".join(messages)
        job_key = resolve_shub_jobkey()
        if job_key:
            message["job_link"] = f"https://app.zyte.com/p/{job_key}"
        text = (
            f"{title}\n\n"
            "Alert Reasons:\n"
            f"{message['failure_reasons']}\n\n"
            f"Job Link: {message.get('job_link', 'N/A')}\n"
            "-------------------------------------"
        )
        if self.slack_handler.fake:
            message["failure_reasons"] = messages
            LOG.info(pformat(message))
        else:
            self.slack_handler.send_message(self.recipients, text, attachments=attachments)


class SlackMixin(AlertSenderMixin):
    """
    A class for adding slack alert capabilities to a shub_workflow class.
    """

    def __init__(self):
        super().__init__()
        self.register_sender_method(self.send_slack_queued_messages)
        self.sender = SlackSender(self.project_settings)

    def send_slack_queued_messages(self):
        if self.messages:
            self.sender.send_slack_messages(self.messages, self.args.subject)
