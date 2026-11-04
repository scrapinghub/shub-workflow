import os
import re
import logging
from functools import lru_cache
from importlib.resources import files
from typing import Generator, List, Optional

from google.cloud import storage


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


def _get_client() -> storage.Client:
    """
    A storage client shared by all the functions of this module. Building one per call costs a new authorization and a
    new connection for every operation, which dominates the time of many small operations (e.g. downloading thousands of
    small files). Keyed by process (a client must not cross a fork) and by credentials file (so setting
    GOOGLE_APPLICATION_CREDENTIALS later, as set_credential_file_environ() does, still takes effect).
    """
    return _build_client(os.getpid(), os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))


@lru_cache(maxsize=None)
def _build_client(pid: int, credentials_file: Optional[str]) -> storage.Client:
    return storage.Client()


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
