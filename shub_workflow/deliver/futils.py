import logging
from typing import List, Generator
from operator import itemgetter
from glob import iglob
from os import listdir, remove, environ, makedirs
from os.path import exists as os_exists, dirname, getctime
from shutil import copyfile

from retrying import retry
from s3fs import S3FileSystem as OriginalS3FileSystem

from shub_workflow.deliver import gcstorage


_S3_ATTRIBUTE = "s3://"
_GS_ATTRIBUTE = "gs://"
BUFFER = 1024 * 1024
DEFAULT_REGION = "us-west-2"


logger = logging.getLogger(__name__)


def just_log_exception(exception):
    logger.error(str(exception))
    for etype in (KeyboardInterrupt, SystemExit, ImportError):
        if isinstance(exception, etype):
            return False
    return True  # retries any other exception


retry_decorator = retry(retry_on_exception=just_log_exception, wait_fixed=60000, stop_max_attempt_number=10)


class S3FileSystem(OriginalS3FileSystem):
    read_timeout = 120
    connect_timeout = 60


def s3_path(path, is_folder=False):
    if not path:
        return ""
    path = path.strip()
    if is_folder and not path.endswith("/"):
        path = path + "/"
    return path[len(_S3_ATTRIBUTE):]


def s3_credentials(key, secret, include_region=False):
    if key and secret:
        return dict(key=key, secret=secret)

    creds = dict(key=environ.get("AWS_ACCESS_KEY_ID", key), secret=environ.get("AWS_SECRET_ACCESS_KEY", secret),)

    if not include_region:
        return creds

    return {
        **creds,
        "region": environ.get("AWS_REGION", DEFAULT_REGION),
        "config_kwargs": {"retries": {"max_attempts": 20}},
    }


def get_file(path, *args, aws_key=None, aws_secret=None, **kwargs):
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        fp = fs.open(s3_path(path), *args, **kwargs)
        try:
            fp.name = path
        except Exception:
            pass
    else:
        fp = open(path, *args, **kwargs)

    return fp


DOWNLOAD_CHUNK_SIZE = 500 * 1024 * 1024
UPLOAD_CHUNK_SIZE = 100 * 1024 * 1024


def download_file(path, dest, aws_key=None, aws_secret=None, **kwargs):
    assert path.startswith(_S3_ATTRIBUTE), f"Not a s3 source: {path}"
    with get_file(path, "rb", aws_key=aws_key, aws_secret=aws_secret, **kwargs) as r, open(dest, "wb") as w:
        total = 0
        while True:
            size = w.write(r.read(DOWNLOAD_CHUNK_SIZE))
            total += size
            logger.info(f"Downloaded {total} bytes from {path}")
            if size < DOWNLOAD_CHUNK_SIZE:
                break


def upload_file(path, dest, aws_key=None, aws_secret=None, **kwargs):
    assert dest.startswith(_S3_ATTRIBUTE), f"Not a s3 source: {dest}"
    with open(path, "rb") as r, get_file(dest, "wb", aws_key=aws_key, aws_secret=aws_secret, **kwargs) as w:
        total = 0
        while True:
            size = w.write(r.read(UPLOAD_CHUNK_SIZE))
            total += size
            logger.info(f"Uploaded {total} bytes to {dest}")
            if size < UPLOAD_CHUNK_SIZE:
                break


def get_glob(path, aws_key=None, aws_secret=None):
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        fp = [_S3_ATTRIBUTE + p for p in fs.glob(s3_path(path))]
    else:
        fp = iglob(path)

    return fp


def cp_file(src_path, dest_path, aws_key=None, aws_secret=None):
    if src_path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        fs.copy(s3_path(src_path), s3_path(dest_path))
    else:
        copyfile(src_path, dest_path)


def mv_file(src_path, dest_path, aws_key=None, aws_secret=None, **kwargs):
    if src_path.startswith(_S3_ATTRIBUTE) and dest_path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        fs.mv(s3_path(src_path), s3_path(dest_path))
    elif src_path.startswith(_S3_ATTRIBUTE):
        download_file(src_path, dest_path, aws_key, aws_secret, **kwargs)
        rm_file(src_path)
    elif dest_path.startswith(_S3_ATTRIBUTE):
        upload_file(src_path, dest_path, aws_key, aws_secret, **kwargs)
        rm_file(src_path)
    elif dest_path.startswith(_GS_ATTRIBUTE):
        gcstorage.upload_file(src_path, dest_path)
        rm_file(src_path)
    else:
        dname = dirname(dest_path)
        makedirs(dname, exist_ok=True)
        cp_file(src_path, dest_path)
        rm_file(src_path)


def rm_file(path, aws_key=None, aws_secret=None):
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        fs.rm(s3_path(path))
    else:
        remove(path)


def list_folder(path, aws_key=None, aws_secret=None) -> List[str]:
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))

        try:
            listing = [f"s3://{name}" for name in fs.ls(s3_path(path, is_folder=True))]
        except FileNotFoundError:
            listing = []

    else:
        if path.strip().endswith("/"):
            path = path[:-1]
        try:
            listing = [f"{path}/{name}" for name in listdir(path)]
        except FileNotFoundError:
            listing = []

    return listing


def list_folder_in_ts_order(input_folder: str) -> Generator[str, None, None]:
    results = []
    for input_file in list_folder(input_folder):
        if input_folder.startswith(_S3_ATTRIBUTE):
            with get_file(input_file, "rb") as f:
                results.append((input_file, f.info()["LastModified"]))
        else:
            results.append((input_file, getctime(input_file)))
    for n, _ in sorted(results, key=itemgetter(1)):
        yield n


def list_folder_files_recursive(folder: str) -> Generator[str, None, None]:
    for f in list_folder(folder):
        if f == folder:
            yield f
        else:
            yield from list_folder_files_recursive(f)


def s3_folder_size(path, aws_key=None, aws_secret=None):
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        return sum(fs.du(s3_path(path, is_folder=True), deep=True).values())


def exists(path, aws_key=None, aws_secret=None):
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        return fs.exists(path)
    return os_exists(path)


def empty_folder(path, aws_key=None, aws_secret=None) -> List[str]:
    removed_files = []
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret))
        for s3file in list_folder(path, aws_key=aws_key, aws_secret=aws_secret):
            try:
                fs.rm(s3file)
                removed_files.append(s3file)
            except FileNotFoundError:
                pass
    else:
        if path.strip().endswith("/"):
            path = path[:-1]
        for fsfile in listdir(path):
            remove(f"{path}/{fsfile}")
            removed_files.append(f"{path}/{fsfile}")

    return removed_files


def touch(path, aws_key=None, aws_secret=None):
    if not exists(path):
        if not path.startswith(_S3_ATTRIBUTE):
            dname = dirname(path)
            makedirs(dname, exist_ok=True)
        with get_file(path, "w", aws_key=aws_key, aws_secret=aws_secret) as f:
            f.write("")
    else:
        raise ValueError(f"File {path} already exists")
