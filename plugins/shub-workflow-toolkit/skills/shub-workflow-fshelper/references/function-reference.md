# `futils` function reference

Every function lives in
[`shub_workflow/utils/futils.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/utils/futils.py)
and, unless noted, accepts `aws_key=None, aws_secret=None, aws_token=None, **kwargs` and dispatches
on the path prefix (`s3://` → S3, `gs://` → GCS, else local). With `S3Helper`/`FSHelper` you call
these as methods and the credentials are injected for you.

## Reading / opening

| Function | Returns | Notes |
| --- | --- | --- |
| `get_file(path, *args, **kwargs)` | open file handle | Like builtin `open()`. S3 via `s3fs.open`; sets `fp.name = path`. `op_kwargs` with `ACL` is lowercased to `acl` for s3fs. Local/GCS supported. |
| `get_object(path, **kwargs)` | object or `None` | **GCS only** — returns `None` for non-GCS paths. |
| `download_file(path, dest=None, ...)` | `None` | Downloads to `dest` (default = `basename(path)`). S3 path streamed in 500 MB chunks. Raises `ValueError` for unsupported FS. |
| `get_glob(path, ...)` | `List[str]` | Glob match. S3: `fs.glob` (results re-prefixed with `s3://`); local: `iglob`. No GCS branch. |

## Writing / moving / deleting

| Function | Returns | Notes |
| --- | --- | --- |
| `upload_file(path, dest, ...)` | `None` | Local→cloud upload. If `dest` ends with `/`, appends `basename(path)`. `op_kwargs` → boto3 `ExtraArgs` (e.g. `{"ACL": "bucket-owner-full-control"}`). S3 + GCS. |
| `upload_file_obj(robj, dest, ...)` | `None` | Upload an open file object. **S3 only** (`ValueError` otherwise). `op_kwargs` → `ExtraArgs`. |
| `cp_file(src, dest, ...)` | `None` | Copy, cross-backend aware: s3↔s3 (`fs.copy`), s3→local (download), local→s3 (upload), gcs variants, local→local (`copyfile`, makes parent dirs). Trailing-slash `dest` appends basename. |
| `mv_file(src, dest, ...)` | `None` | `cp_file` then `rm_file(src)`. Same cross-backend matrix. Trailing-slash `dest` appends basename. |
| `rm_file(path, ...)` | `None` | Delete one file. S3 `fs.rm`, GCS `gcstorage.rm_file`, local `os.remove`. |
| `empty_folder(path, ...)` | `List[str]` | Remove **all files** in a folder (one level); returns the removed paths. Ignores missing files on S3. |
| `touch(path, ...)` | `None` | Create an empty file; **raises `ValueError` if it already exists**. Makes parent dirs for local paths. |

## Listing

| Function | Returns | Semantics |
| --- | --- | --- |
| `list_folder(path, ...)` | `List[str]` | **One level** (like `ls`). Uses `s3fs.ls`; excludes the folder marker itself. Treats `path` as a folder (adds trailing `/`). Missing folder → `[]`. |
| `list_path(path, ...)` | `Generator[str]` | **Prefix-based**, efficient `boto3` `bucket.objects.filter(Prefix=…)`. Effectively recursive over the prefix. Accepts a bare key prefix, not just a clean folder. |
| `list_folder_files_recursive(folder, ...)` | `Generator[str]` | Full recursive walk by repeatedly calling `list_folder`. |
| `list_folder_in_ts_order(folder, ...)` | `Generator[str]` | `list_folder` results sorted by `LastModified` (S3) / `getctime` (local), oldest→newest. |

## Metadata / existence

| Function | Returns | Notes |
| --- | --- | --- |
| `exists(path, ...)` | `bool` | S3 `fs.exists`, GCS `gcstorage.exists`, local `os.path.exists`. |
| `s3_folder_size(path, ...)` | `int` bytes | **S3 only** — `fs.du(..., deep=True)` summed. Returns `None` for non-S3. |

## Credentials helpers (internal, but useful to understand)

- `s3_credentials(key, secret, token, region=None)` — builds the `s3fs` credential dict.
  **Environment overrides:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` take
  precedence over passed values. Sets `boto3` retry `max_attempts=20`.
- `check_s3_path(path)` / `check_gcs_path(path)` — `True` if the path targets that backend; raise
  `ModuleNotFoundError` if the backend's optional dependency isn't installed
  (`shub-workflow[with-s3-tools]` / `[with-gcs-tools]`).
- `s3_path(path, is_folder=False)` — strips the `s3://` prefix (and appends `/` for folders).

## Classes

### `S3SessionFactory(aws_key, aws_secret, aws_role, aws_external_id, expiration_margin_minutes=10)`
Generates **temporary STS credentials** by assuming `aws_role` (with `aws_external_id`).
`get_credentials()` returns `{aws_key, aws_secret, aws_token}` and transparently **re-assumes the
role** once the current credentials are within `expiration_margin_minutes` of expiry. Used internally
by `S3Helper` when a role is configured.

### `S3Helper(aws_key, aws_secret, aws_role=None, aws_external_id=None, expiration_margin_minutes=10, op_kwargs_by_method_name=None, **kwargs)`
**Deprecated — kept only for backward compatibility; use `FSHelper` instead.** Constructing
`S3Helper` *directly* emits a `DeprecationWarning` (subclasses such as `FSHelper` don't). It binds
every module function annotated as `Callable` on the class as a **method** that:
1. refreshes role credentials (if `aws_role`+`aws_external_id` were given) before the call,
2. injects per-method default `op_kwargs` from `op_kwargs_by_method_name={ "method": {...} }`,
3. forwards `bucket_kwargs` (popped from `**kwargs`) and the current credentials.

### `FSHelper(aws_key=None, aws_secret=None, **kwargs)`  ← recommended
The general, recommended helper (and the one `shub_workflow.script` uses): an `S3Helper` subclass
with credentials defaulting to `None` — for local-filesystem work or when AWS credentials come from
the ambient environment / instance role. Same methods as `S3Helper`. So
`FSHelper().download_file("s3://b/k")` calls module `download_file` with creds filled in.
