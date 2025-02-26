import os
import re
import logging
from typing import Generator, List

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


def list_path(path: str) -> Generator[str, None, None]:
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(path)
    if m is None:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket_name, blob_prefix = m.groups()
    bucket = storage_client.bucket(bucket_name)

    for blob in bucket.list_blobs(prefix=blob_prefix, retry=storage.retry.DEFAULT_RETRY):
        yield f"gs://{bucket_name}/{blob.name}"


def list_folder(path: str) -> List[str]:
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(path)
    if m is None:
        raise ValueError(f"Invalid path {path} for GCS.")
    bucket_name, blob_prefix = m.groups()
    bucket = storage_client.bucket(bucket_name)

    start_offset = ""
    new_results = True
    result = []
    while new_results:
        new_results = False
        for blob in bucket.list_blobs(prefix=blob_prefix, retry=storage.retry.DEFAULT_RETRY, start_offset=start_offset):
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


def mv_file(src: str, dest: str):
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    m = _GS_FOLDER_RE.match(dest)
    assert m is not None, "Destination must be in the format gs://<bucket>/<blob>"
    dest_bucket_name, dest_blob_name = m.groups()

    assert src_bucket_name == dest_bucket_name, "Source and destination bucket must be the same."

    bucket = storage_client.bucket(src_bucket_name)
    bucket.rename_blob(bucket.blob(src_blob_name), dest_blob_name)
    _LOGGER.info(f"Moved {src} to {dest}")


def cp_file(src: str, dest: str):
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    m = _GS_FOLDER_RE.match(dest)
    assert m is not None, "Destination must be in the format gs://<bucket>/<blob>"
    dest_bucket_name, dest_blob_name = m.groups()

    assert src_bucket_name == dest_bucket_name, "Source and destination bucket must be the same."

    bucket = storage_client.bucket(src_bucket_name)
    dest_bucket = storage_client.bucket(dest_bucket_name)
    bucket.copy_blob(bucket.blob(src_blob_name), dest_bucket, dest_blob_name)
    _LOGGER.info(f"Copied {src} to {dest}")


def exists(src: str) -> bool:
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    bucket = storage_client.bucket(src_bucket_name)
    return bucket.blob(src_blob_name).exists()


def get_object(src: str):
    storage_client = storage.Client()
    m = _GS_FOLDER_RE.match(src)
    assert m is not None, "Source must be in the format gs://<bucket>/<blob>"
    src_bucket_name, src_blob_name = m.groups()

    bucket = storage_client.bucket(src_bucket_name)
    return bucket.blob(src_blob_name)


def get_file(src: str, *args, **kwargs):
    return get_object(src).open(*args, **kwargs)
