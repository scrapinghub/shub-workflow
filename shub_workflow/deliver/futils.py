import re
import logging
import uuid
from typing import List, Generator
from operator import itemgetter
from glob import iglob
from os import listdir, remove, environ, makedirs
from os.path import exists as os_exists, dirname, getctime, basename
from shutil import copyfile
from datetime import datetime, timedelta, timezone

from s3fs import S3FileSystem as OriginalS3FileSystem
import boto3

from shub_workflow.deliver import gcstorage

S3_PATH_RE = re.compile("s3://(.+?)/(.+)")
_S3_ATTRIBUTE = "s3://"
_GS_ATTRIBUTE = "gs://"
BUFFER = 1024 * 1024


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def just_log_exception(exception):
    logger.error(str(exception))
    for etype in (KeyboardInterrupt, SystemExit, ImportError):
        if isinstance(exception, etype):
            return False
    return True  # retries any other exception


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


def s3_credentials(key, secret, token, region=None):
    creds = dict(
        key=environ.get("AWS_ACCESS_KEY_ID", key), secret=environ.get("AWS_SECRET_ACCESS_KEY", secret), token=token
    )

    result = {
        **creds,
        "config_kwargs": {"retries": {"max_attempts": 20}},
    }
    region = region or environ.get("AWS_REGION")
    if region is not None:
        result.update({"client_kwargs": {"region_name": region}})
    return result


def get_file(path, *args, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    op_kwargs = kwargs.pop("op_kwargs", {})
    region = kwargs.pop("region", None)
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        if "ACL" in op_kwargs:
            op_kwargs["acl"] = op_kwargs.pop("ACL")
        fp = fs.open(s3_path(path), *args, **op_kwargs)
        try:
            fp.name = path
        except Exception:
            pass
    else:
        fp = open(path, *args, **kwargs)

    return fp


DOWNLOAD_CHUNK_SIZE = 500 * 1024 * 1024
UPLOAD_CHUNK_SIZE = 100 * 1024 * 1024


def download_file(path, dest=None, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    assert path.startswith(_S3_ATTRIBUTE), f"Not a s3 source: {path}"
    if dest is None:
        dest = basename(path)
    with get_file(path, "rb", aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs) as r, open(
        dest, "wb"
    ) as w:
        total = 0
        while True:
            size = w.write(r.read(DOWNLOAD_CHUNK_SIZE))
            total += size
            logger.info(f"Downloaded {total} bytes from {path}")
            if size < DOWNLOAD_CHUNK_SIZE:
                break


def upload_file_obj(robj, dest, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    assert dest.startswith(_S3_ATTRIBUTE), f"Not a s3 source: {dest}"
    with get_file(dest, "wb", aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs) as w:
        total = 0
        while True:
            size = w.write(robj.read(UPLOAD_CHUNK_SIZE))
            total += size
            logger.info(f"Uploaded {total} bytes to {dest}")
            if size < UPLOAD_CHUNK_SIZE:
                break


def upload_file(path, dest, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if dest.endswith("/"):
        dest = dest + basename(path)
    with open(path, "rb") as r:
        upload_file_obj(r, dest, aws_key, aws_secret, aws_token, **kwargs)


def get_glob(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    region = kwargs.pop("region", None)
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        fp = [_S3_ATTRIBUTE + p for p in fs.glob(s3_path(path))]
    else:
        fp = iglob(path)

    return fp


def cp_file(src_path, dest_path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    region = kwargs.pop("region", None)
    if src_path.startswith(_S3_ATTRIBUTE):
        op_kwargs = kwargs.pop("op_kwargs", {})
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        fs.copy(s3_path(src_path), s3_path(dest_path), **op_kwargs)
    else:
        copyfile(src_path, dest_path)


def mv_file(src_path, dest_path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if src_path.startswith(_S3_ATTRIBUTE) and dest_path.startswith(_S3_ATTRIBUTE):
        cp_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
        rm_file(src_path, aws_key, aws_secret, aws_token, **kwargs)
    elif src_path.startswith(_S3_ATTRIBUTE):
        download_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
        rm_file(src_path, aws_key, aws_secret, aws_token, **kwargs)
    elif dest_path.startswith(_S3_ATTRIBUTE):
        upload_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
        rm_file(src_path)
    elif dest_path.startswith(_GS_ATTRIBUTE):
        gcstorage.upload_file(src_path, dest_path)
        rm_file(src_path)
    else:
        dname = dirname(dest_path)
        if dname:
            makedirs(dname, exist_ok=True)
        cp_file(src_path, dest_path)
        rm_file(src_path)


def rm_file(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    region = kwargs.pop("region", None)
    if path.startswith(_S3_ATTRIBUTE):
        op_kwargs = kwargs.pop("op_kwargs", {})
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        fs.rm(s3_path(path), **op_kwargs)
    else:
        remove(path)


def list_path(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    """
    More efficient boto3 based path listing, that accepts prefix
    """
    region = kwargs.pop("region", None)
    session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        aws_session_token=aws_token,
        region_name=region,
        **kwargs,
    )
    if path.startswith(_S3_ATTRIBUTE):
        s3 = session.resource("s3")
        bucket_name, path = S3_PATH_RE.match(path).groups()
        bucket = s3.Bucket(bucket_name)
        for result in bucket.objects.filter(Prefix=path):
            yield f"s3://{bucket_name}/{result.key}"
    else:
        raise NotImplementedError("Only implemented for s3.")


def list_folder(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs) -> List[str]:
    region = kwargs.pop("region", None)
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)

        try:
            path = s3_path(path, is_folder=True)
            listing = [f"s3://{name}" for name in fs.ls(path) if name != path]
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


def list_folder_in_ts_order(
    input_folder: str, aws_key=None, aws_secret=None, aws_token=None, **kwargs
) -> Generator[str, None, None]:
    results = []
    for input_file in list_folder(input_folder, aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs):
        if input_folder.startswith(_S3_ATTRIBUTE):
            with get_file(input_file, "rb", aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs) as f:
                results.append((input_file, f.info()["LastModified"]))
        else:
            results.append((input_file, getctime(input_file)))
    for n, _ in sorted(results, key=itemgetter(1)):
        yield n


def list_folder_files_recursive(
    folder: str, aws_key=None, aws_secret=None, aws_token=None, **kwargs
) -> Generator[str, None, None]:
    for f in list_folder(folder, aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs):
        if f == folder:
            yield f
        else:
            yield from list_folder_files_recursive(
                f, aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs
            )


def s3_folder_size(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    region = kwargs.pop("region", None)
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        return sum(fs.du(s3_path(path, is_folder=True), deep=True).values())


def exists(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    region = kwargs.pop("region", None)
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        return fs.exists(path)
    return os_exists(path)


def empty_folder(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs) -> List[str]:
    region = kwargs.pop("region", None)
    removed_files = []
    if path.startswith(_S3_ATTRIBUTE):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        for s3file in list_folder(path, aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token):
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


def touch(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if not exists(path):
        if not path.startswith(_S3_ATTRIBUTE):
            dname = dirname(path)
            makedirs(dname, exist_ok=True)
        with get_file(path, "w", aws_key=aws_key, aws_secret=aws_secret, aws_token=aws_token, **kwargs) as f:
            f.write("")
    else:
        raise ValueError(f"File {path} already exists")


class S3SessionFactory:

    """
    Generate s3 temporal session credentials in cases where that is required
    """

    def __init__(self, aws_key, aws_secret, aws_role, aws_external_id, expiration_margin_minutes=10):
        self.aws_role = aws_role
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.aws_external_id = aws_external_id
        self.expiration_margin_minutes = expiration_margin_minutes
        self.credentials_expiration = None
        self.sts_client = boto3.client("sts", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        self.full_credentials = {}
        self.s3_client = None

    def get_credentials(self):
        """
        Get a new set of credentials if the existing ones are close to expire,
        update the S3 client.
        """
        if self.credentials_expiration is None or datetime.now(timezone.utc) > self.credentials_expiration - timedelta(
            minutes=self.expiration_margin_minutes
        ):
            session_name = str(uuid.uuid4())
            self.full_credentials = self.sts_client.assume_role(
                RoleArn=self.aws_role,
                RoleSessionName=session_name,
                ExternalId=self.aws_external_id,
            )["Credentials"]
            self.credentials_expiration = self.full_credentials["Expiration"]
            self.s3_client = boto3.session.Session(
                aws_access_key_id=self.full_credentials["AccessKeyId"],
                aws_secret_access_key=self.full_credentials["SecretAccessKey"],
                aws_session_token=self.full_credentials["SessionToken"],
            ).client("s3")
        return {
            "aws_key": self.full_credentials["AccessKeyId"],
            "aws_secret": self.full_credentials["SecretAccessKey"],
            "aws_token": self.full_credentials["SessionToken"],
        }


class S3Helper:
    def __init__(self, aws_key, aws_secret, aws_role=None, aws_external_id=None, expiration_margin_minutes=10):
        self.s3_session_factory = None
        self.credentials = {"aws_key": aws_key, "aws_secret": aws_secret}
        if aws_role is not None and aws_external_id is not None:
            self.s3_session_factory = S3SessionFactory(
                aws_key, aws_secret, aws_role, aws_external_id, expiration_margin_minutes
            )

        for method in (
            get_file,
            upload_file_obj,
            upload_file,
            download_file,
            list_folder,
            mv_file,
            rm_file,
            get_glob,
            empty_folder,
            exists,
            touch,
            cp_file,
            list_path,
            list_folder_in_ts_order,
            list_folder_files_recursive,
            s3_folder_size,
        ):
            self._wrap_method(method)

    def _wrap_method(self, method):
        def _method(*args, **kwargs):
            if self.s3_session_factory is not None:
                self.credentials = self.s3_session_factory.get_credentials()
            kwargs.update(self.credentials)
            return method(*args, **kwargs)

        setattr(self, method.__name__, _method)


class FSHelper(S3Helper):
    def __init__(self, aws_key=None, aws_secret=None, **kwargs):
        super().__init__(aws_key, aws_secret, **kwargs)
