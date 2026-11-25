import logging
import json
import os
import hashlib
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from scrapy.settings import BaseSettings
from tenacity import RetryCallState, retry, retry_if_exception_type, before_sleep_log
from scrapinghub.client.exceptions import ServerError, Unauthorized
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError


logger = logging.getLogger(__name__)


def hashstr(text: str) -> str:
    u = hashlib.sha1()
    u.update(text.encode("utf8"))
    return u.hexdigest()


def resolve_shub_jobkey() -> Optional[str]:
    return os.environ.get("SHUB_JOBKEY")


def resolve_project_id(project_id=None) -> int:
    """
    Gets project id from following sources in following order of precedence:
    - default parameter values
    - environment variables
    - scrapinghub.yml file

    in order to allow to use codes that needs HS or dash API,
    either locally or from scrapinghub, correctly configured
    """
    if project_id:
        try:
            retval = int(project_id)
            logger.info(f"Project id {retval} is explicit.")
            return retval
        except ValueError:
            pass
    else:
        # read from environment only if not explicitly provided
        if os.environ.get("PROJECT_ID") is not None:
            retval = int(os.environ["PROJECT_ID"])
            logger.info(f"Project id {retval} resolved from PROJECT_ID environment variable.")
            return retval

        # for ScrapyCloud jobs:
        jobkey = resolve_shub_jobkey()
        if jobkey:
            retval = int(jobkey.split("/")[0])
            logger.info(f"Project id {retval} resolved from jobkey.")
            return retval

    # read from scrapinghub.yml
    try:
        from shub.config import load_shub_config

        cfg = load_shub_config()
        try:
            project_id = project_id or "default"
            retval = int(cfg.get_project_id(project_id))
            logger.info(f"Project id {retval} resolved from scrapinghub.yml {project_id} entry.")
            return retval
        except Exception:
            logger.error(f"Project entry '{project_id}' not found in scrapinghub.yml.")
    except ImportError:
        logger.error("Install shub package if want to access scrapinghub.yml.")
    except TypeError:
        logger.error("Default project entry not available in scrapinghub.yml.")

    raise ValueError(
        "No default project id found. Use either PROJECT_ID env. variable or set 'default' entry in scrapinghub.yml, "
        "or use --project-id with a project numeric id or an existing entry in scrapinghub.yml."
    )


MINS_IN_A_DAY = 24 * 60
ONE_MIN_IN_S = 60


# The retry settings are environment variables, read when a retry happens rather than at import time. So they can be
# set from Scrapy Cloud settings in the project settings.py (see kumo_settings()), which is loaded after this module.
#
# DASH_RETRY_MAX: max attempts of a call, whatever the error (default: a day of attempts, one per minute).
# DASH_AMBIGUOUS_RETRY_MAX: max retries of a call after an ambiguous exception (see DASH_AMBIGUOUS_EXCEPTIONS).
# DASH_RETRY_WAIT_SECS: seconds to wait between attempts.
# DASH_RETRY_LOGGING_LEVEL: log level of the message logged before each retry.
DEFAULT_DASH_RETRY_MAX = MINS_IN_A_DAY
DEFAULT_DASH_AMBIGUOUS_RETRY_MAX = 5
DEFAULT_DASH_RETRY_WAIT_SECS = ONE_MIN_IN_S
DEFAULT_DASH_RETRY_LOGGING_LEVEL = "ERROR"


F = TypeVar("F", bound=Callable[..., Any])

# Exceptions that are certainly a temporary Scrapy Cloud issue. Retried up to DASH_RETRY_MAX attempts.
# ServerError is the only ScrapinghubAPIError that certainly is temporary. Don't capture ScrapinghubAPIError here.
DASH_TEMPORARY_EXCEPTIONS = (ServerError, ReadTimeout, ConnectionError, HTTPError)
# Exceptions that usually mean a real error (i.e. lack of permissions), but that Scrapy Cloud can also raise during
# a temporary blip. Retried at most DASH_AMBIGUOUS_RETRY_MAX times.
DASH_AMBIGUOUS_EXCEPTIONS = (Unauthorized,)

_AMBIGUOUS_FAILURES_ATTR = "_dash_ambiguous_failures"


def _dash_retry_stop(retry_state: RetryCallState) -> bool:
    if retry_state.attempt_number >= int(os.environ.get("DASH_RETRY_MAX", DEFAULT_DASH_RETRY_MAX)):
        return True
    assert retry_state.outcome is not None
    if isinstance(retry_state.outcome.exception(), DASH_AMBIGUOUS_EXCEPTIONS):
        # retry_state lives along a single decorated call, so this counts the ambiguous failures of that call only.
        failures = getattr(retry_state, _AMBIGUOUS_FAILURES_ATTR, 0) + 1
        setattr(retry_state, _AMBIGUOUS_FAILURES_ATTR, failures)
        return failures > int(os.environ.get("DASH_AMBIGUOUS_RETRY_MAX", DEFAULT_DASH_AMBIGUOUS_RETRY_MAX))
    return False


def _dash_retry_wait(retry_state: RetryCallState) -> float:
    return int(os.environ.get("DASH_RETRY_WAIT_SECS", DEFAULT_DASH_RETRY_WAIT_SECS))


def _dash_retry_before_sleep(retry_state: RetryCallState) -> None:
    level = os.environ.get("DASH_RETRY_LOGGING_LEVEL", DEFAULT_DASH_RETRY_LOGGING_LEVEL)
    before_sleep_log(logger, getattr(logging, level))(retry_state)


_dash_retry = retry(
    retry=retry_if_exception_type(DASH_TEMPORARY_EXCEPTIONS + DASH_AMBIGUOUS_EXCEPTIONS),
    before_sleep=_dash_retry_before_sleep,
    reraise=True,
    stop=_dash_retry_stop,
    wait=_dash_retry_wait,
)


def dash_retry_decorator(func: F) -> F:
    # tenacity >= 9.2 types decorated callables as _RetryDecorated, which mypy doesn't accept as a method
    # override or protocol member. Keep the original signature instead.
    return cast(F, _dash_retry(func))


# /proc/self/status fields (reported in kB) -> memusage stat names
_PROC_STATUS_FIELDS = {"VmRSS": "rss", "VmHWM": "max", "RssAnon": "rss/anon", "RssFile": "rss/file"}
# cgroup v2, then v1: usage file, limit file, stat file, and the memory.stat keys of anonymous and file backed memory
_CGROUP_FILES = (
    ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.stat", "anon", "file"),
    (
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "/sys/fs/cgroup/memory/memory.stat",
        "rss",
        "cache",
    ),
)


def get_memory_usage() -> Dict[str, int]:
    """
    Memory of the current process and of its container (cgroup), in bytes, keyed by stat name. Only what the platform
    exposes (Linux /proc and cgroup files), so it is empty elsewhere. Resident memory is split into anonymous (the heap)
    and file backed pages (e.g. a memory mapped file), because a container memory limit may count both.
    """
    usage: Dict[str, int] = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                name, _, value = line.partition(":")
                if name in _PROC_STATUS_FIELDS:
                    usage[_PROC_STATUS_FIELDS[name]] = int(value.split()[0]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    for current_file, limit_file, stat_file, anon_key, file_key in _CGROUP_FILES:
        try:
            with open(current_file) as f:
                cgroup = {"cgroup": int(f.read())}
            with open(stat_file) as f:
                stat = dict(line.split() for line in f)
            cgroup["cgroup/anon"] = int(stat[anon_key])
            cgroup["cgroup/file"] = int(stat[file_key])
        except (OSError, ValueError, KeyError):
            continue
        try:
            with open(limit_file) as f:
                cgroup["cgroup/limit"] = int(f.read())  # cgroup v2 says "max" when there is no limit
        except (OSError, ValueError):
            pass
        usage.update(cgroup)
        break
    return usage


_settings_warning_issued = False


def kumo_settings():
    global _settings_warning_issued
    settings = {}
    shub_job_data = json.loads(os.environ.get("SHUB_SETTINGS", "{}"))
    if shub_job_data:
        settings.update(shub_job_data["project_settings"])
        settings.update(shub_job_data["spider_settings"])
    elif not _settings_warning_issued:
        logger.warning("Couldn't find Dash project settings, probably not running in Dash")
        _settings_warning_issued = True
    return settings


def get_project_settings() -> BaseSettings:
    from scrapy.utils.project import get_project_settings as scrapy_get_project_settings  # pylint: disable=import-error

    settings = scrapy_get_project_settings()
    settings.setdict(kumo_settings(), priority="project")
    try:
        # test sh_scrapy is available
        import sh_scrapy  # noqa: F401

        settings.setdict({"STATS_CLASS": "sh_scrapy.stats.HubStorageStatsCollector"}, priority="cmdline")
    except ImportError:
        pass
    return settings


def get_kumo_loglevel(default="INFO"):
    loglevel = kumo_settings().get("LOG_LEVEL", default)
    return getattr(logging, loglevel)
