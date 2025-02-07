import logging
from typing import List, Callable

from shub_workflow.script import BaseScript

LOG = logging.getLogger(__name__)


class AlertSenderMixin(BaseScript):
    """
    A class for adding slack alert capabilities to a shub_workflow class.
    """

    default_subject = "No Subject"

    def __init__(self):
        self.messages: List[str] = []
        self.registered_senders: List[Callable[[], None]] = []
        super().__init__()

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("--subject", help="Set alert message subject.", default=self.default_subject)

    def append_message(self, message: str):
        self.messages.append(message)

    def register_sender_method(self, sender: Callable[[], None]):
        self.registered_senders.append(sender)

    def send_messages(self):
        for sender in self.registered_senders:
            try:
                sender()
            except Exception as e:
                LOG.error(repr(e))
