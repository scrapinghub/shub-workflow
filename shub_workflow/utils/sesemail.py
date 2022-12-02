import os
import logging
import boto3
from botocore.client import Config
from email.message import Message
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SESHelper:

    DEFAULT_SES_REGION = "us-east-1"

    DEFAULT_EMAIL_SUBJECT_PREFIX = "[Zyte]"
    DEFAULT_EMAIL_SUBJECT = "Notification from Zyte"
    DEFAULT_FROM_ADDR = None

    def __init__(self, aws_key: str, aws_secret: str, aws_region: Optional[str] = None):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        assert self.aws_key and self.aws_secret, "SES Credentials not set."
        self.aws_region = aws_region or self.DEFAULT_SES_REGION

    def send_ses_email(
        self,
        from_addr: str,
        to_addrs: List[str],
        msg: Message,
        region: str = DEFAULT_SES_REGION,
        cc_addrs: Optional[List[str]] = None,
        bcc_addrs: Optional[List[str]] = None,
        reply_to: Optional[str] = None,
    ) -> Dict:

        config = Config(connect_timeout=60, retries={"max_attempts": 20})
        client = boto3.client(
            "ses", region, aws_access_key_id=self.aws_key, aws_secret_access_key=self.aws_secret, config=config
        )
        logger.info("Sending mail as %s to: %s", from_addr, to_addrs)
        msg["From"] = from_addr or self.DEFAULT_FROM_ADDR
        msg["To"] = ",".join(to_addrs)
        if cc_addrs:
            msg["cc"] = ",".join(cc_addrs)
        if bcc_addrs:
            msg["bcc"] = ",".join(bcc_addrs)
        if reply_to:
            msg["Reply-To"] = reply_to
        destinations = {"Destination": {"ToAddresses": to_addrs}}
        if cc_addrs:
            destinations["Destination"]["CcAddresses"] = cc_addrs
        if bcc_addrs:
            destinations["Destination"]["CcAddresses"] = bcc_addrs

        response = client.send_raw_email(Source=from_addr, Destinations=to_addrs, RawMessage={"Data": msg.as_string()})
        return response

    def build_email_message(
        self,
        body: str,
        image_attachments: Optional[List[str]] = None,
        text_attachments: Optional[List[str]] = None,
        other_attachments: Optional[List[Message]] = None,
        subject_prefix: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Message:
        subject_prefix = (subject_prefix or self.DEFAULT_EMAIL_SUBJECT_PREFIX).strip()
        subject_header = ""
        if subject_prefix:
            subject_header += subject_prefix + " "
        subject_header += subject or self.DEFAULT_EMAIL_SUBJECT

        msg = MIMEMultipart()
        msg["Subject"] = subject_header

        msg.attach(MIMEText(body))

        for imgpath in image_attachments or []:
            imgatt = MIMEImage(open(imgpath, "rb").read())
            imgatt.add_header("Content-Disposition", "attachment", filename=os.path.basename(imgpath))
            msg.attach(imgatt)

        for path in text_attachments or []:
            textatt: MIMEBase
            if path.endswith(".gz"):
                textatt = MIMEApplication(open(path, "rb").read(), "gzip")
            else:
                textatt = MIMEText(open(path, "r").read())
            textatt.add_header("Content-Disposition", "attachment", filename=os.path.basename(path))
            msg.attach(textatt)

        for att in other_attachments or []:
            msg.attach(att)

        return msg


class SESMailSenderMixin:
    """Use this mixin for enabling ses email sending capabilities on your script class"""

    def __init__(self):
        self.notification_emails = []
        super().__init__()
        self.seshelper = None
        try:
            self.seshelper = SESHelper(
                self.project_settings["AWS_EMAIL_ACCESS_KEY"], self.project_settings["AWS_EMAIL_SECRET_KEY"]
            )
        except AssertionError:
            logger.warning("No SES credentials set. No mails will be sent.")

    def send_ses_email(self, body, subject, text_attachments=None, image_attachments=None):
        if self.notification_emails and self.seshelper is not None:
            msg = self.seshelper.build_email_message(
                body,
                text_attachments=text_attachments,
                image_attachments=image_attachments,
                subject=subject,
            )
            self.seshelper.send_ses_email("noreply@zyte.com", self.notification_emails, msg)
