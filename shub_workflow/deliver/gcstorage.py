import os
import re
import logging
from pkg_resources import resource_filename

from google.cloud import storage


_GS_FOLDER_RE = re.compile(r"gs://([-\w]+)/(.*)$")

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


def get_credfile_path(module, resource, check_exists=True):
    credfile = resource_filename(module, resource)
    if not check_exists or os.path.exists(credfile):
        return credfile


def set_credential_file_environ(module, resource, check_exists=True):
    credfile = get_credfile_path(module, resource, check_exists)

    assert credfile, "Google application credentials file does not exist."
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credfile


def upload_file(src_path, dest_path):
    storage_client = storage.Client()
    try:
        bucket_name, destination_blob_name = _GS_FOLDER_RE.match(dest_path).groups()
    except AttributeError:
        raise ValueError(f"Invalid destination {dest_path}")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(src_path)
    _LOGGER.info(f"File {src_path} uploaded to {dest_path}.")
