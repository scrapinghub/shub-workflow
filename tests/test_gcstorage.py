import os
import json
import base64
from tempfile import mkstemp
from typing import Any, Dict
from unittest import TestCase, skipUnless
from unittest.mock import MagicMock, patch

from google.oauth2 import service_account

from shub_workflow.utils import gcstorage

try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False

FAKE_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\nNOTAREALKEY\n-----END PRIVATE KEY-----\n"


def service_account_info(private_key: str = FAKE_PRIVATE_KEY) -> Dict[str, Any]:
    return {
        "type": "service_account",
        "project_id": "example-project",
        "private_key_id": "0123456789abcdef",
        "private_key": private_key,
        "client_email": "example@example-project.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "token_uri": "https://oauth2.googleapis.com/token",
    }


class GCStorageClientTest(TestCase):
    def setUp(self) -> None:
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        os.environ.pop(gcstorage.CREDENTIALS_JSON_ENV, None)
        gcstorage._build_client.cache_clear()
        self.addCleanup(gcstorage._build_client.cache_clear)

        self.credentials = MagicMock(name="credentials")
        for p in (
            patch.object(gcstorage.storage, "Client", autospec=True),
            patch.object(
                gcstorage.service_account.Credentials,
                "from_service_account_info",
                return_value=self.credentials,
            ),
        ):
            p.start()
            self.addCleanup(p.stop)
        self.client_cls: MagicMock = gcstorage.storage.Client  # type: ignore[assignment]
        self.from_info: MagicMock = gcstorage.service_account.Credentials.from_service_account_info  # type: ignore

    def assert_no_chained_exception_shown(self, exc: BaseException) -> None:
        # a traceback would also print the original exception, whose message may hold the value
        self.assertIsNone(exc.__cause__)
        self.assertTrue(exc.__context__ is None or exc.__suppress_context__)

    def assert_built_from_info(self) -> None:
        self.from_info.assert_called_once_with(service_account_info())
        self.client_cls.assert_called_once_with(project="example-project", credentials=self.credentials)

    def test_raw_json_in_credentials_json_variable(self) -> None:
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = json.dumps(service_account_info())
        gcstorage._get_client()
        self.assert_built_from_info()

    def test_base64_json_in_credentials_json_variable(self) -> None:
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = base64.b64encode(
            json.dumps(service_account_info()).encode()
        ).decode()
        gcstorage._get_client()
        self.assert_built_from_info()

    def test_credentials_json_variable_wins_over_a_credentials_file(self) -> None:
        fd, path = mkstemp()
        os.close(fd)
        self.addCleanup(os.remove, path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = json.dumps(service_account_info())
        gcstorage._get_client()
        self.assert_built_from_info()

    def test_key_stored_in_the_file_variable_is_never_handed_to_google_auth(self) -> None:
        for value in (
            json.dumps(service_account_info()),
            base64.b64encode(json.dumps(service_account_info()).encode()).decode(),
        ):
            with self.subTest(value=value[:10]):
                gcstorage._build_client.cache_clear()
                self.from_info.reset_mock()
                self.client_cls.reset_mock()
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = value
                gcstorage._get_client()
                self.assert_built_from_info()

    def test_credentials_file_is_left_to_google_auth(self) -> None:
        fd, path = mkstemp()
        os.close(fd)
        self.addCleanup(os.remove, path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        gcstorage._get_client()
        self.from_info.assert_not_called()
        self.client_cls.assert_called_once_with()

    def test_missing_credentials_file_is_left_to_google_auth(self) -> None:
        # a wrong path is reported by google-auth, and a path is no secret
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/nonexistent/key.json"
        gcstorage._get_client()
        self.from_info.assert_not_called()
        self.client_cls.assert_called_once_with()

    def test_no_credentials_variable_uses_the_defaults(self) -> None:
        gcstorage._get_client()
        self.from_info.assert_not_called()
        self.client_cls.assert_called_once_with()

    def test_errors_never_include_the_variable_value(self) -> None:
        cases = [
            (gcstorage.CREDENTIALS_JSON_ENV, "not json at all, secret-ish text"),
            (gcstorage.CREDENTIALS_JSON_ENV, '{"private_key": "SECRETSECRET", broken'),
            (gcstorage.CREDENTIALS_JSON_ENV, '["SECRETSECRET"]'),
            ("GOOGLE_APPLICATION_CREDENTIALS", '{"private_key": "SECRETSECRET", broken'),
            ("GOOGLE_APPLICATION_CREDENTIALS", base64.b64encode(b'{"private_key": "SECRETSECRET", broken').decode()),
        ]
        for envname, value in cases:
            with self.subTest(envname=envname, value=value):
                gcstorage._build_client.cache_clear()
                os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
                os.environ.pop(gcstorage.CREDENTIALS_JSON_ENV, None)
                os.environ[envname] = value
                with self.assertRaises(gcstorage.InvalidCredentialsError) as ctx:
                    gcstorage._get_client()
                self.assertIn(envname, str(ctx.exception))
                self.assertNotIn(value, str(ctx.exception))
                self.assertNotIn("SECRETSECRET", str(ctx.exception))
                self.assert_no_chained_exception_shown(ctx.exception)
        self.client_cls.assert_not_called()

    def test_key_loading_errors_are_scrubbed(self) -> None:
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = json.dumps(service_account_info())
        self.from_info.side_effect = ValueError(f"cannot load {FAKE_PRIVATE_KEY}")
        with self.assertRaises(gcstorage.InvalidCredentialsError) as ctx:
            gcstorage._get_client()
        self.assertNotIn("NOTAREALKEY", str(ctx.exception))
        self.assertIn("ValueError", str(ctx.exception))
        self.assert_no_chained_exception_shown(ctx.exception)

    def test_client_is_cached_until_the_credentials_change(self) -> None:
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = json.dumps(service_account_info())
        first = gcstorage._get_client()
        self.assertIs(gcstorage._get_client(), first)
        self.assertEqual(self.client_cls.call_count, 1)
        os.environ[gcstorage.CREDENTIALS_JSON_ENV] = json.dumps(dict(service_account_info(), client_id="other"))
        gcstorage._get_client()
        self.assertEqual(self.client_cls.call_count, 2)


@skipUnless(HAS_CRYPTOGRAPHY, "needs cryptography to generate a throwaway key")
class GCStorageRealKeyTest(TestCase):
    """End to end through google-auth with a throwaway key: no network is used to build a client."""

    def setUp(self) -> None:
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop(gcstorage.CREDENTIALS_JSON_ENV, None)
        gcstorage._build_client.cache_clear()
        self.addCleanup(gcstorage._build_client.cache_clear)

    def test_client_is_built_from_a_key_held_in_the_file_variable(self) -> None:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ).decode()
        # the legacy layout that leaked the key through google-auth's "File <value> was not found." error
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json.dumps(service_account_info(pem))
        client = gcstorage._get_client()
        self.assertEqual(client.project, "example-project")
        self.assertIsInstance(client._credentials, service_account.Credentials)
        self.assertEqual(
            client._credentials.service_account_email, "example@example-project.iam.gserviceaccount.com"
        )
