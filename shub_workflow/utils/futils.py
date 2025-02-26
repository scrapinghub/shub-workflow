import re
import logging
import uuid
from typing import List, Generator, Callable
from operator import itemgetter
from glob import iglob
from os import listdir, remove, environ, makedirs
from os.path import exists as os_exists, dirname, getctime, basename
from shutil import copyfile
from datetime import datetime, timedelta, timezone

try:
    from s3fs import S3FileSystem as OriginalS3FileSystem
    import boto3

    s3_enabled = True
except ImportError:
    s3_enabled = False


try:
    from shub_workflow.utils import gcstorage

    gcs_enabled = True
except ImportError:
    gcs_enabled = False


S3_PATH_RE = re.compile("s3://(.+?)/(.+)")
_S3_ATTRIBUTE = "s3://"
_GS_ATTRIBUTE = "gs://"
BUFFER = 1024 * 1024


logger = logging.getLogger(__name__)


def just_log_exception(exception):
    logger.error(str(exception))
    for etype in (KeyboardInterrupt, SystemExit, ImportError):
        if isinstance(exception, etype):
            return False
    return True  # retries any other exception


if s3_enabled:

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


def check_s3_path(path):
    if path.startswith(_S3_ATTRIBUTE):
        if s3_enabled:
            return True
        raise ModuleNotFoundError(
            "S3 dependencies are not installed. Install shubw-workflow as shub-workflow[with-s3-tools]"
        )
    return False


def check_gcs_path(path):
    if path.startswith(_GS_ATTRIBUTE):
        if gcs_enabled:
            return True
        raise ModuleNotFoundError(
            "GCS dependencies are not installed. Install shubw-workflow as shub-workflow[with-gcs-tools]"
        )
    return False


def get_s3_bucket_keyname(s3path, aws_key, aws_secret, aws_token, **kwargs):
    region = kwargs.pop("region", None)
    region_name = kwargs.pop("region_name", region)
    botocore_session = kwargs.pop("botocore_session", None)
    profile_name = kwargs.pop("profile_name", None)

    session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        aws_session_token=aws_token,
        region_name=region_name,
        botocore_session=botocore_session,
        profile_name=profile_name,
    )
    s3 = session.resource("s3")
    m = S3_PATH_RE.match(s3path)
    if m:
        bucket_name, path = m.groups()
        bucket = s3.Bucket(bucket_name)
    else:
        raise ValueError(f"Bad s3 path specification: {s3path}")
    return bucket, path, kwargs.pop("op_kwargs", {})


def get_file(path, *args, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    op_kwargs = kwargs.pop("op_kwargs", {})
    region = kwargs.pop("region", None)
    if check_s3_path(path):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        if "ACL" in op_kwargs:
            op_kwargs["acl"] = op_kwargs.pop("ACL")
        fp = fs.open(s3_path(path), *args, **op_kwargs)
        try:
            fp.name = path
        except Exception:
            pass
    elif check_gcs_path(path):
        fp = gcstorage.get_file(path, *args, **kwargs)
    else:
        fp = open(path, *args, **kwargs)

    return fp


def get_object(path, *args, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    fp = None
    if check_gcs_path(path):
        fp = gcstorage.get_object(path)
    return fp


DOWNLOAD_CHUNK_SIZE = 500 * 1024 * 1024
UPLOAD_CHUNK_SIZE = 100 * 1024 * 1024


def download_file(path, dest=None, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if dest is None:
        dest = basename(path)
    if check_s3_path(path):
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
    elif check_gcs_path(path):
        gcstorage.download_file(path, dest)
    else:
        raise ValueError(f"Not supported file system fpr path {path}")


def upload_file_obj(robj, dest, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if check_s3_path(dest):
        bucket, keyname, op_kwargs = get_s3_bucket_keyname(dest, aws_key, aws_secret, aws_token, **kwargs)
        bucket.upload_fileobj(robj, keyname, ExtraArgs=op_kwargs)
        logger.info(f"Uploaded file obj to {dest}.")
    else:
        raise ValueError("Not a supported cloud.")


def upload_file(path, dest, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if dest.endswith("/"):
        dest = dest + basename(path)
    if check_s3_path(dest):
        bucket, keyname, op_kwargs = get_s3_bucket_keyname(dest, aws_key, aws_secret, aws_token, **kwargs)
        bucket.upload_file(path, keyname, ExtraArgs=op_kwargs)
        logger.info(f"Uploaded {path} to {dest}.")
    elif check_gcs_path(dest):
        gcstorage.upload_file(path, dest)
    else:
        raise ValueError("Not a supported cloud.")


def get_glob(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs) -> List[str]:
    region = kwargs.pop("region", None)
    if check_s3_path(path):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        fp = [_S3_ATTRIBUTE + p for p in fs.glob(s3_path(path))]
    else:
        fp = list(iglob(path))

    return fp


def cp_file(src_path, dest_path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if dest_path.endswith("/"):
        dest_path = dest_path + basename(src_path)
    region = kwargs.pop("region", None)
    if check_s3_path(src_path) and check_s3_path(dest_path):
        op_kwargs = kwargs.pop("op_kwargs", {})
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        fs.copy(s3_path(src_path), s3_path(dest_path), **op_kwargs)
    elif check_s3_path(src_path):
        download_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
    elif check_s3_path(dest_path):
        upload_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
    elif check_gcs_path(src_path) and check_gcs_path(dest_path):
        gcstorage.cp_file(src_path, dest_path)
    elif check_gcs_path(src_path):
        gcstorage.download_file(src_path, dest_path)
    elif check_gcs_path(dest_path):
        gcstorage.upload_file(src_path, dest_path)
    else:
        dname = dirname(dest_path)
        if dname:
            makedirs(dname, exist_ok=True)
        copyfile(src_path, dest_path)


def mv_file(src_path, dest_path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    if dest_path.endswith("/"):
        dest_path = dest_path + basename(src_path)
    if check_s3_path(src_path) and check_s3_path(dest_path):
        cp_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
        rm_file(src_path, aws_key, aws_secret, aws_token, **kwargs)
    elif check_s3_path(src_path):
        download_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
        rm_file(src_path, aws_key, aws_secret, aws_token, **kwargs)
    elif check_s3_path(dest_path):
        upload_file(src_path, dest_path, aws_key, aws_secret, aws_token, **kwargs)
        rm_file(src_path)
    elif check_gcs_path(src_path) and check_gcs_path(dest_path):
        gcstorage.mv_file(src_path, dest_path)
    elif check_gcs_path(src_path):
        gcstorage.download_file(src_path, dest_path)
        gcstorage.rm_file(src_path)
    elif check_gcs_path(dest_path):
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
    if check_s3_path(path):
        op_kwargs = kwargs.pop("op_kwargs", {})
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        fs.rm(s3_path(path), **op_kwargs)
    elif check_gcs_path(path):
        gcstorage.rm_file(path)
    else:
        remove(path)


def list_path(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs) -> Generator[str, None, None]:
    """
    More efficient boto3 based path listing, that accepts prefix
    """
    if check_s3_path(path):
        bucket, path, op_kwargs = get_s3_bucket_keyname(path, aws_key, aws_secret, aws_token, **kwargs)
        for result in bucket.objects.filter(Prefix=path):
            yield f"s3://{bucket.name}/{result.key}"
    elif check_gcs_path(path):
        yield from gcstorage.list_path(path)
    else:
        listing = []
        if path.strip().endswith("/"):
            path = path[:-1]
        try:
            listing = [f"{path}/{name}" for name in listdir(path)]
        except FileNotFoundError:
            pass
        yield from listing


def list_folder(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs) -> List[str]:
    if check_s3_path(path):
        region = kwargs.pop("region", None)
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)

        try:
            path = s3_path(path, is_folder=True)
            listing = [f"s3://{name}" for name in fs.ls(path) if name != path]
        except FileNotFoundError:
            listing = []
    elif check_gcs_path(path):
        listing = list(gcstorage.list_folder(path))
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
        if check_s3_path(input_folder):
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
    if check_s3_path(path):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        return sum(fs.du(s3_path(path, is_folder=True), deep=True).values())


def exists(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs):
    region = kwargs.pop("region", None)
    if check_s3_path(path):
        fs = S3FileSystem(**s3_credentials(aws_key, aws_secret, aws_token, region), **kwargs)
        return fs.exists(path)
    if check_gcs_path(path):
        return gcstorage.exists(path)
    return os_exists(path)


def empty_folder(path, aws_key=None, aws_secret=None, aws_token=None, **kwargs) -> List[str]:
    region = kwargs.pop("region", None)
    removed_files = []
    if check_s3_path(path):
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
        if not check_s3_path(path):
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

    cp_file: Callable
    download_file: Callable
    empty_folder: Callable
    exists: Callable
    get_file: Callable
    get_glob: Callable
    get_object: Callable
    list_folder: Callable
    list_folder_files_recursive: Callable
    list_folder_in_ts_order: Callable
    list_path: Callable
    mv_file: Callable
    rm_file: Callable
    s3_folder_size: Callable
    touch: Callable
    upload_file: Callable
    upload_file_obj: Callable

    def __init__(
        self,
        aws_key,
        aws_secret,
        aws_role=None,
        aws_external_id=None,
        expiration_margin_minutes=10,
        op_kwargs_by_method_name=None,
    ):
        self.s3_session_factory = None
        self.credentials = {"aws_key": aws_key, "aws_secret": aws_secret}
        if aws_role is not None and aws_external_id is not None:
            self.s3_session_factory = S3SessionFactory(
                aws_key, aws_secret, aws_role, aws_external_id, expiration_margin_minutes
            )

        op_kwargs_by_method_name = op_kwargs_by_method_name or {}
        for method_name, _type in S3Helper.__annotations__.items():
            if not hasattr(self, method_name) and _type is Callable:
                method = globals()[method_name]
                self._wrap_method(method, **op_kwargs_by_method_name.get(method_name, {}))

    def _wrap_method(self, method, **op_kwargs):
        def _method(*args, **kwargs):
            if self.s3_session_factory is not None:
                self.credentials = self.s3_session_factory.get_credentials()
            if op_kwargs:
                kwargs.update({"op_kwargs": op_kwargs})
            kwargs.update(self.credentials)
            return method(*args, **kwargs)

        setattr(self, method.__name__, _method)


class FSHelper(S3Helper):
    def __init__(self, aws_key=None, aws_secret=None, **kwargs):
        super().__init__(aws_key, aws_secret, **kwargs)
