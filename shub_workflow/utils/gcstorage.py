import os
import re
import json
import base64
import logging
from functools import lru_cache
from importlib.resources import files
from typing import Any, Dict, Generator, List, Optional, Tuple

from google.cloud import storage
from google.oauth2 import service_account


_GS_FOLDER_RE = re.compile(r"gs://([-\w]+)/(.*)$")

_LOGGER = logging.getLogger(__name__)

# How long, in seconds, to keep retrying a failed storage request (timeouts, connection and DNS errors, 429 and 5xx
# responses) before giving up. The library default, 120 s, is shorter than an ordinary network blip, and a long running
# script that dies on one may need hours to rebuild its state.
GCS_RETRY_TIMEOUT_SECS = float(os.environ.get("GCS_RETRY_TIMEOUT_SECS", 30 * 60))
_RETRY = (
    storage.retry.DEFAULT_RETRY.with_timeout(GCS_RETRY_TIMEOUT_SECS)
    if hasattr(storage.retry.DEFAULT_RETRY, "with_timeout")
    else storage.retry.DEFAULT_RETRY.with_deadline(GCS_RETRY_TIMEOUT_SECS)  # older google-api-core
)


# Environment variable that holds the service account key itself (its JSON, raw or base64-encoded), for environments
# where no key file exists, e.g. a Scrapy Cloud job whose secrets come in environment variables. The client is then
# built from it directly, with no file written anywhere. GOOGLE_APPLICATION_CREDENTIALS is the PATH of a key file:
# google-auth repeats its value verbatim in its errors ("File <value> was not found.") and in its debug log, so a key
# stored in it gets printed as soon as anything reads it before it has been moved into a file.
CREDENTIALS_JSON_ENV = "GOOGLE_APPLICATION_CREDENTIALS_JSON"
_CREDENTIALS_FILE_ENV = "GOOGLE_APPLICATION_CREDENTIALS"


class InvalidCredentialsError(ValueError):
    """A service account key held in an environment variable can't be used. The message never includes the key."""


def _get_client() -> storage.Client:
    """
    A storage client shared by all the functions of this module. Building one per call costs a new authorization and a
    new connection for every operation, which dominates the time of many small operations (e.g. downloading thousands of
    small files). Keyed by process (a client must not cross a fork) and by the credentials variables (so setting
    GOOGLE_APPLICATION_CREDENTIALS later, as set_credential_file_environ() does, still takes effect).
    """
    return _build_client(
        os.getpid(), os.environ.get(_CREDENTIALS_FILE_ENV), os.environ.get(CREDENTIALS_JSON_ENV)
    )


@lru_cache(maxsize=None)
def _build_client(pid: int, credentials_file: Optional[str], credentials_json: Optional[str]) -> storage.Client:
    found = _inline_service_account_info(credentials_file, credentials_json)
    if found is None:
        return storage.Client()
    source, info = found
    try:
        credentials = service_account.Credentials.from_service_account_info(info)
    except Exception as exc:
        message = str(exc)
        for secret in (info.get("private_key"), credentials_json, credentials_file):
            if secret:
                message = message.replace(secret, "<redacted>")
        raise InvalidCredentialsError(
            f"The service account key in {source} can't be loaded: {type(exc).__name__}: {message}"
        ) from None
    # An explicit project, even None, keeps the client from looking one up through google-auth's defaults, which read
    # GOOGLE_APPLICATION_CREDENTIALS again.
    return storage.Client(project=info.get("project_id"), credentials=credentials)


def _inline_service_account_info(
    credentials_file: Optional[str], credentials_json: Optional[str]
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    The service account key held in the environment itself, as (variable name, info), or None when there is none and
    the client must be built the default way (GOOGLE_APPLICATION_CREDENTIALS as a file path, or other google-auth
    sources).

    CREDENTIALS_JSON_ENV is read first. A key stored in GOOGLE_APPLICATION_CREDENTIALS instead of a file path is
    accepted too, for backward compatibility, so that it is never handed to google-auth.
    """
    if credentials_json:
        return CREDENTIALS_JSON_ENV, _parse_service_account_info(credentials_json, CREDENTIALS_JSON_ENV)
    if credentials_file and not os.path.exists(credentials_file) and _decode_inline(credentials_file) is not None:
        return _CREDENTIALS_FILE_ENV, _parse_service_account_info(credentials_file, _CREDENTIALS_FILE_ENV)
    return None


def _decode_inline(value: str) -> Optional[str]:
    """The JSON text held in value, raw or base64-encoded, or None if value doesn't hold one (e.g. a file path)."""
    text = value.strip()
    if text.startswith("{"):
        return text
    try:
        decoded = base64.b64decode(text, validate=True).decode()
    except ValueError:  # binascii.Error and UnicodeDecodeError are ValueErrors
        return None
    return decoded if decoded.lstrip().startswith("{") else None


def _parse_service_account_info(value: str, envname: str) -> Dict[str, Any]:
    text = _decode_inline(value)
    if text is None:
        raise InvalidCredentialsError(f"{envname} holds neither JSON nor base64-encoded JSON.")
    try:
        info = json.loads(text)
    except ValueError:
        raise InvalidCredentialsError(f"{envname} does not hold valid JSON.") from None
    if not isinstance(info, dict):
        raise InvalidCredentialsError(f"{envname} does not hold a JSON object.")
    return info


def get_credfile_path(module, resource, check_exists=True):
    credfile = str(files(module).joinpath(resource))
    if not check_exists or os.path.exists(credfile):
        return credfile


def set_credential_file_environ(module, resource, check_exists=True):
    credfile = get_credfile_path(module, resource, check_exists)

    assert credfile, "Google application credentials file does not exist."
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credfile


def upload_file(src_path: str, dest_path: str, **kwargs):
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(dest_path)
    if m is None:
        raise ValueError(f"Invalid destination {dest_path}")
    bucket_name, destination_blob_name = m.groups()
    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(bucket_name, **bucket_kwargs)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(src_path, retry=_RETRY)
    _LOGGER.info(f"File {src_path} uploaded to {dest_path}.")


def list_path(path: str, **kwargs) -> Generator[str, None, None]:
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(path)
    if m is None:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket_name, blob_prefix = m.groups()
    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(bucket_name, **bucket_kwargs)

    for blob in bucket.list_blobs(prefix=blob_prefix, retry=_RETRY):
        yield f"gs://{bucket_name}/{blob.name}"


def list_folder(path: str, **kwargs) -> List[str]:
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(path)
    if m is None:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket_name, blob_prefix = m.groups()
    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(bucket_name, **bucket_kwargs)

    start_offset = ""
    new_results = True
    result = []
    while new_results:
        new_results = False
        for blob in bucket.list_blobs(prefix=blob_prefix, retry=_RETRY, start_offset=start_offset):
            new_results = True
            if blob_prefix:
                suffix = blob.name.split(blob_prefix, 1)[1]
            else:
                suffix = blob.name
            if "/" not in suffix.lstrip("/"):
                result.append(f"gs://{bucket_name}/{blob.name}")
            else:
                folder = (blob_prefix + suffix.split("/")[0]).lstrip("/")
                result.append(f"gs://{bucket_name}/{folder}/")
                start_offset = folder + "0"
                break
        else:
            break
    return result


def download_file(path: str, dest: str, **kwargs):
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(path)
    if m:
        bucket_name, blob_name = m.groups()
    else:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(bucket_name, **bucket_kwargs)
    blob = bucket.blob(blob_name)
    with open(dest, "wb") as w:
        blob.download_to_file(w, retry=_RETRY)
    _LOGGER.info(f"File {path} downloaded to {dest}.")


def rm_file(path: str, **kwargs):
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(path)
    if m:
        bucket_name, blob_name = m.groups()
    else:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(bucket_name, **bucket_kwargs)
    blob = bucket.blob(blob_name)
    blob.delete(retry=_RETRY)
    _LOGGER.info(f"Deleted {path}.")


def mv_file(src: str, dest: str, **kwargs):
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    m = _GS_FOLDER_RE.match(dest)
    assert m is not None, "Destination must be in the format gs://<bucket>/<blob>"
    dest_bucket_name, dest_blob_name = m.groups()

    assert src_bucket_name == dest_bucket_name, "Source and destination bucket must be the same."

    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(src_bucket_name, **bucket_kwargs)
    bucket.rename_blob(bucket.blob(src_blob_name), dest_blob_name)
    _LOGGER.info(f"Moved {src} to {dest}")


def cp_file(src: str, dest: str, **kwargs):
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    m = _GS_FOLDER_RE.match(dest)
    assert m is not None, "Destination must be in the format gs://<bucket>/<blob>"
    dest_bucket_name, dest_blob_name = m.groups()

    assert src_bucket_name == dest_bucket_name, "Source and destination bucket must be the same."

    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(src_bucket_name, **bucket_kwargs)
    dest_bucket = storage_client.bucket(dest_bucket_name, **bucket_kwargs)
    bucket.copy_blob(bucket.blob(src_blob_name), dest_bucket, dest_blob_name)
    _LOGGER.info(f"Copied {src} to {dest}")


def exists(src: str, **kwargs) -> bool:
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(src_bucket_name, **bucket_kwargs)
    return bucket.blob(src_blob_name).exists(retry=_RETRY)


def get_object(src: str, **kwargs):
    storage_client = _get_client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    bucket_kwargs = kwargs.pop("bucket_kwargs", {})
    bucket = storage_client.bucket(src_bucket_name, **bucket_kwargs)
    return bucket.blob(src_blob_name)


def get_file(src: str, *args, **kwargs):
    kwargs_without_bucket_kwargs = kwargs.copy()
    kwargs_without_bucket_kwargs.pop("bucket_kwargs", None)
    return get_object(src, **kwargs).open(*args, **kwargs_without_bucket_kwargs)
