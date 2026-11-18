"""
Early warning for the expiration of the service account key used for GCS.

A key generated on our side and uploaded to a service account (rather than created by Google) inherits the validity of
its X.509 certificate: Google stops accepting the key at the certificate's expiration date, and from then on every GCS
operation authenticated with it fails. Renewing it usually needs whoever administers the service account to act, so a
project wants to know well in advance. Typically from its daily monitor, before anything else it does, so the alert goes
out even if the rest of the monitor fails:

    from shub_workflow.utils.gcstorage.certificate import get_expiration_alert

    if (message := get_expiration_alert("the README section 'Credentials rotation'")) is not None:
        ...  # send it

The certificate doesn't need to be kept anywhere. Google publishes the public certificates of every service account at
the url the key carries in `client_x509_cert_url`, keyed by key id, so the one of the key in use is selected by its
`private_key_id`. Keys created by Google itself usually never expire, so they never trigger the alert.

The key is read from the same environment variables as the storage client (see the package docs). Nothing derived from
the private key is ever logged or included in a message.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from cryptography import x509

from shub_workflow.utils.gcstorage import (
    CREDENTIALS_JSON_ENV,
    _CREDENTIALS_FILE_ENV,
    InvalidCredentialsError,
    _inline_service_account_info,
    _parse_service_account_info,
)

DEFAULT_ALERT_WINDOW = timedelta(days=30)


def _load_service_account_info() -> Dict[str, Any]:
    """The service account key in use: held in the environment itself, or in the file GOOGLE_APPLICATION_CREDENTIALS
    points to."""
    credentials_file = os.environ.get(_CREDENTIALS_FILE_ENV)
    found = _inline_service_account_info(credentials_file, os.environ.get(CREDENTIALS_JSON_ENV))
    if found is not None:
        return found[1]
    if credentials_file:
        with open(credentials_file) as f:
            return _parse_service_account_info(f.read(), _CREDENTIALS_FILE_ENV)
    raise InvalidCredentialsError(
        f"No service account key found: neither {CREDENTIALS_JSON_ENV} nor {_CREDENTIALS_FILE_ENV} is set."
    )


def get_certificate_expiration() -> Tuple[str, datetime]:
    """
    Returns the service account email and the expiration date (timezone aware, UTC) of the certificate of the key in
    use.
    """
    info = _load_service_account_info()
    response = requests.get(info["client_x509_cert_url"], timeout=60)
    response.raise_for_status()
    certificates: Dict[str, str] = response.json()
    if info["private_key_id"] not in certificates:
        raise LookupError("The key's private_key_id is not among the service account's public certificates.")
    certificate = x509.load_pem_x509_certificate(certificates[info["private_key_id"]].encode())
    return info["client_email"], certificate.not_valid_after_utc


def get_expiration_alert(
    doc_reference: str,
    note: str = "",
    window: timedelta = DEFAULT_ALERT_WINDOW,
    now: Optional[datetime] = None,
) -> Optional[str]:
    """
    The alert message to send when the certificate of the key in use expires within `window` (or already did), or when
    its expiration can't be verified; None otherwise.

    - doc_reference: where the project describes the rotation procedure, e.g. "the README section 'Credentials
      rotation'". Every message points there.
    - note: an optional sentence appended to the expiration messages, e.g. which other projects share the key.

    A failed verification alerts too, whatever the failure: a check that fails silently is the same as no check, and
    this one must never break the script that runs it. Only the exception type is reported, as the message of an
    exception raised while decoding the credentials could carry a piece of them.
    """
    now = now or datetime.now(timezone.utc)
    try:
        email, expiration = get_certificate_expiration()
    except Exception as e:
        return (
            "Alert! Could not verify the expiration date of the GCS credentials certificate "
            f"({type(e).__module__}.{type(e).__qualname__}). Check it by hand as described in {doc_reference}."
        )
    remaining = expiration - now
    if remaining > window:
        return None
    when = expiration.strftime("%Y-%m-%d %H:%M UTC")
    if remaining <= timedelta(0):
        status = f"EXPIRED on {when}: every GCS operation authenticated with it fails"
    else:
        status = f"expires on {when} ({remaining.days} days left)"
    message = f"Alert! The GCS credentials certificate of {email} {status}. Rotate it following {doc_reference}."
    return f"{message} {note}" if note else message
