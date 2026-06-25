---
name: shub-workflow-fshelper
description: >-
  Use when reading, writing, listing, copying, moving, or deleting files through shub-workflow's
  cloud-agnostic filesystem layer — the `shub_workflow.utils.futils` module and its `FSHelper` class
  (the recommended one; `S3Helper` is its deprecated backward-compatibility base) — i.e. any code that
  touches `s3://`, `gs://`, or local paths the same way, needs AWS role-assumed (STS) credentials, or
  must list/download deliveries in a bucket. Covers the prefix-based dispatch, module functions vs.
  the helper class, credential/role handling, the three listing variants (`list_folder` vs `list_path`
  vs recursive), `op_kwargs`/ACLs, and the install extras. Reach for it whenever you see
  `from shub_workflow.utils.futils import ...` or an `FSHelper`.
---

# shub-workflow filesystem utils (`futils` / `FSHelper`)

[`shub_workflow/utils/futils.py`](https://github.com/scrapinghub/shub-workflow/blob/master/shub_workflow/utils/futils.py)
is a **cloud-agnostic filesystem layer**. Every operation is one function that **dispatches on the
path prefix**: `s3://…` → S3 (via `s3fs` + `boto3`), `gs://…` → Google Cloud Storage (via
`shub_workflow.utils.gcstorage`), anything else → the local filesystem. So the *same* call works
across backends — you write `download_file(path)` and it does the right thing whether `path` is an
S3 key, a GCS object, or a local file.

## Use `FSHelper` (not `S3Helper`)

**`FSHelper` is the recommended class** — it's the general one (works for local, S3 and GCS, with
credentials defaulting to the ambient environment) and it's what `shub_workflow.script` uses. Prefer
it everywhere.

`S3Helper` is its **base class, kept only for backward compatibility** in some older projects.
Constructing it **directly** now emits a `DeprecationWarning`; `FSHelper` (and any other subclass)
does not. New or updated code should always use `FSHelper`.

## Two ways to call it

1. **Module-level functions** — stateless; you pass credentials (or rely on env / local fs) on every
   call: `futils.list_folder("s3://bucket/prefix/", aws_key=..., aws_secret=...)`.
2. **`FSHelper` instance** — construct once with credentials, then call the same names as **methods**
   with no creds needed: `helper.list_folder("s3://bucket/prefix/")`. This is the normal path in
   scripts. Every module function listed in `S3Helper.__annotations__` is auto-bound as a method that
   injects the stored credentials (and refreshes them if a role is in play).

```python
from shub_workflow.utils.futils import FSHelper

# Ambient/env or local-fs credentials:
helper = FSHelper()

# Explicit key/secret:
helper = FSHelper(aws_key=KEY, aws_secret=SECRET)

# Role-assumed temporary credentials (STS), auto-refreshed before expiry:
helper = FSHelper(aws_key=KEY, aws_secret=SECRET, aws_role=ROLE_ARN, aws_external_id=EXT_ID)

for path in helper.list_folder("s3://my-bucket/some/folder/"):
    print(path)
helper.download_file("s3://my-bucket/some/folder/file.json", "file.json")
```

- Inside a `BaseScript`, a ready-made helper is already available as **`self.fshelper`** — prefer it
  rather than constructing your own. Override the **`init_fshelper()`** hook to customize how it's
  built (e.g. a GCS project, default ACLs).

## The three listing functions — pick the right one

This is the most common mistake. They are **not** interchangeable:

| Function | Semantics | Use when |
| --- | --- | --- |
| `list_folder(path)` | **one level only** — like `ls`. Returns immediate children, excludes the folder marker itself. Returns a `list`. | You want the direct contents of a single folder. |
| `list_path(path)` | **prefix-based**, efficient paginated `boto3` scan. Returns a **generator** over every key under the prefix (effectively recursive). | You want *all* objects under a prefix, cheaply, or you only have a key prefix (not a clean folder). |
| `list_folder_files_recursive(folder)` | recurses by repeatedly calling `list_folder`. Generator. | You need a full recursive walk and want folder-by-folder descent semantics. |

Also: `list_folder_in_ts_order(folder)` yields `list_folder`'s results **sorted by last-modified
time** (oldest→newest) — handy for "process the earliest delivered file first".

> **Footgun:** `list_folder` is **not recursive**. On a nested delivery tree it returns subfolders,
> not the files inside them. For "every file under here", use `list_path` (cheapest) or
> `list_folder_files_recursive`.

## Core operations (all dispatch on prefix)

`get_file` (open a file handle like `open()`), `download_file`, `upload_file`, `upload_file_obj`,
`cp_file`, `mv_file`, `rm_file`, `exists`, `touch`, `get_glob`, `empty_folder`, `s3_folder_size`.
Full signatures, return types, and cross-backend behavior:
[references/function-reference.md](references/function-reference.md).

## Critical rules & footguns

- **Use `FSHelper`, not `S3Helper`.** `S3Helper` is the deprecated backward-compat base; direct use
  warns. `FSHelper` is the general, recommended class.
- **Trailing-slash = "into this folder".** `upload_file`, `cp_file`, `mv_file` treat a `dest` ending
  in `/` as a folder and append the source's basename. Omit the slash to control the final name.
- **`op_kwargs` carries S3 ExtraArgs / ACL.** Pass per-call S3 options via `op_kwargs`, e.g.
  `op_kwargs={"ACL": "bucket-owner-full-control"}` on uploads/copies — essential when writing into a
  bucket owned by another account (a customer's). `get_file` lowercases `ACL`→`acl` for `s3fs`; the
  upload path passes `ACL` straight through as boto3 `ExtraArgs`. With a helper, set defaults
  per-method via `op_kwargs_by_method_name={"upload_file": {"ACL": "bucket-owner-full-control"}}`.
- **Env credentials win.** `s3_credentials` prefers `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  from the environment over the `aws_key`/`aws_secret` you pass. `AWS_REGION` is honored too.
- **Role assumption is automatic & rotating.** Give the helper both `aws_role` and `aws_external_id`
  and it builds an `S3SessionFactory` that calls STS `assume_role` and **refreshes the temporary
  credentials before they expire** (`expiration_margin_minutes`, default 10) on every wrapped call.
- **Backends are optional extras.** S3 needs `pip install shub-workflow[with-s3-tools]`
  (`s3fs`+`boto3`); GCS needs `shub-workflow[with-gcs-tools]`. Hitting an `s3://`/`gs://` path
  without the extra raises `ModuleNotFoundError` from `check_s3_path`/`check_gcs_path`.
- **Only annotated names are wrapped.** The helper binds exactly the methods declared as `Callable`
  in `S3Helper`'s class annotations; a new module function isn't a helper method until it's annotated
  there.
- **GCS coverage is narrower.** A few helpers (e.g. `get_object`, `upload_file_obj`) are S3-only or
  GCS-only — check the reference before assuming a function is fully cross-backend.
