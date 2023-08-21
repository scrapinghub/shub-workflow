import os
import re
import logging
from typing import Generator

from pkg_resources import resource_filename

from google.cloud import storage


_GS_FOLDER_RE = re.compile(r"gs://([-\w]+)/(.*)$")

_LOGGER = logging.getLogger(__name__)


def get_credfile_path(module, resource, check_exists=True):
    credfile = resource_filename(module, resource)
    if not check_exists or os.path.exists(credfile):
        return credfile


def set_credential_file_environ(module, resource, check_exists=True):
    credfile = get_credfile_path(module, resource, check_exists)

    assert credfile, "Google application credentials file does not exist."
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credfile


def upload_file(src_path: str, dest_path: str):
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(dest_path)
    if m is None:
        raise ValueError(f"Invalid destination {dest_path}")
    bucket_name, destination_blob_name = m.groups()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(src_path, retry=storage.retry.DEFAULT_RETRY)
    _LOGGER.info(f"File {src_path} uploaded to {dest_path}.")


def list_folder(path: str) -> Generator[str, None, None]:
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(path)
    if m:
        bucket_name, blob_prefix = m.groups()
    else:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket = storage_client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=blob_prefix, retry=storage.retry.DEFAULT_RETRY):
        yield f"gs://{bucket_name}/{blob.name}"


def download_file(path: str, dest: str):
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(path)
    if m:
        bucket_name, blob_name = m.groups()
    else:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with open(dest, "wb") as w:
        blob.download_to_file(w, retry=storage.retry.DEFAULT_RETRY)
    _LOGGER.info(f"File {path} downloaded to {dest}.")


def rm_file(path: str):
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(path)
    if m:
        bucket_name, blob_name = m.groups()
    else:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete(retry=storage.retry.DEFAULT_RETRY)
    _LOGGER.info(f"Deleted {path}.")
