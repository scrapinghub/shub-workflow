import os
import json
import base64
from datetime import datetime, timedelta, timezone
from tempfile import mkstemp
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from shub_workflow.utils import gcstorage
from shub_workflow.utils.gcstorage import certificate

from .test_gcstorage import service_account_info

EXPIRATION = datetime(2030, 6, 1, 12, 0, tzinfo=timezone.utc)
KEY_ID = service_account_info()["private_key_id"]
CERT_URL = "https://www.googleapis.com/robot/v1/metadata/x509/example%40example-project.iam.gserviceaccount.com"
DOC = "the README section 'Credentials rotation'"


def make_certificate_pem(not_after: datetime) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "example")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_after - timedelta(days=365))
        .not_valid_after(not_after)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def published_certificates_response(certificates: Dict[str, str]) -> MagicMock:
    response = MagicMock()
    response.json.return_value = certificates
    return response


class CertificateExpirationTest(TestCase):
    pem: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.pem = make_certificate_pem(EXPIRATION)

    def setUp(self) -> None:
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop(gcstorage.CREDENTIALS_JSON_ENV, None)
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        get = patch.object(certificate.requests, "get", autospec=True)
        self.get: MagicMock = get.start()
        self.addCleanup(get.stop)
        # Google also publishes the certificates of the account's other keys: only ours must be read.
        self.get.return_value = published_certificates_response(
            {KEY_ID: self.pem, "otherkeyid": make_certificate_pem(EXPIRATION + timedelta(days=3650))}
        )

    def set_inline_key(self, encode: bool = True) -> None:
        info = service_account_info()
        info["client_x509_cert_url"] = CERT_URL
        text = json.dumps(info)
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = base64.b64encode(text.encode()).decode() if encode else text

    def test_reads_the_certificate_of_the_key_in_use(self) -> None:
        self.set_inline_key()
        email, expiration = certificate.get_certificate_expiration()
        self.assertEqual(email, "example@example-project.iam.gserviceaccount.com")
        self.assertEqual(expiration, EXPIRATION)
        self.get.assert_called_once_with(CERT_URL, timeout=60)

    def test_reads_raw_json_key(self) -> None:
        self.set_inline_key(encode=False)
        self.assertEqual(certificate.get_certificate_expiration()[1], EXPIRATION)

    def test_reads_key_file(self) -> None:
        info = service_account_info()
        info["client_x509_cert_url"] = CERT_URL
        fd, path = mkstemp(suffix=".json")
        self.addCleanup(os.remove, path)
        with os.fdopen(fd, "w") as f:
            json.dump(info, f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        self.assertEqual(certificate.get_certificate_expiration()[1], EXPIRATION)

    def test_no_alert_out_of_the_window(self) -> None:
        self.set_inline_key()
        self.assertIsNone(certificate.get_expiration_alert(DOC, now=EXPIRATION - timedelta(days=31)))

    def test_alert_within_the_window(self) -> None:
        self.set_inline_key()
        message = certificate.get_expiration_alert(DOC, note="Shared by A and B.", now=EXPIRATION - timedelta(days=20))
        assert message is not None
        self.assertIn("expires on 2030-06-01 12:00 UTC (20 days left)", message)
        self.assertIn("example@example-project.iam.gserviceaccount.com", message)
        self.assertIn(DOC, message)
        self.assertTrue(message.endswith(" Shared by A and B."))

    def test_custom_window(self) -> None:
        self.set_inline_key()
        now = EXPIRATION - timedelta(days=50)
        self.assertIsNone(certificate.get_expiration_alert(DOC, now=now))
        self.assertIsNotNone(certificate.get_expiration_alert(DOC, window=timedelta(days=60), now=now))

    def test_alert_when_expired(self) -> None:
        self.set_inline_key()
        message = certificate.get_expiration_alert(DOC, now=EXPIRATION + timedelta(days=1))
        assert message is not None
        self.assertIn("EXPIRED on 2030-06-01 12:00 UTC", message)

    def test_alert_when_no_key_is_set(self) -> None:
        message = certificate.get_expiration_alert(DOC)
        assert message is not None
        self.assertIn("Could not verify", message)
        self.assertIn("InvalidCredentialsError", message)
        self.assertIn(DOC, message)
        self.get.assert_not_called()

    def test_alert_never_includes_the_key(self) -> None:
        # A corrupted key must not leak into the alert, whatever the exception raised while decoding it.
        private_key = "-----BEGIN PRIVATE KEY-----\nSECRETMATERIAL\n-----END PRIVATE KEY-----\n"
        info = service_account_info(private_key=private_key)
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = json.dumps(info)  # no client_x509_cert_url: KeyError
        message = certificate.get_expiration_alert(DOC)
        assert message is not None
        self.assertIn("KeyError", message)
        self.assertNotIn("SECRETMATERIAL", message)
        self.assertNotIn("PRIVATE KEY", message)

    def test_alert_when_key_id_is_not_published(self) -> None:
        self.set_inline_key()
        self.get.return_value = published_certificates_response({"otherkeyid": self.pem})
        message = certificate.get_expiration_alert(DOC)
        assert message is not None
        self.assertIn("builtins.LookupError", message)

    def test_alert_when_the_request_fails(self) -> None:
        self.set_inline_key()
        self.get.side_effect = requests.ConnectionError("unreachable")
        message = certificate.get_expiration_alert(DOC)
        assert message is not None
        self.assertIn("requests.exceptions.ConnectionError", message)
