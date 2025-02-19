import logging
import json
import os
import hashlib
from typing import Optional

from scrapy.settings import BaseSettings
from tenacity import retry, retry_if_exception_type, before_sleep_log, stop_after_attempt, wait_fixed
from scrapinghub.client.exceptions import ServerError
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
            return int(project_id)
        except ValueError:
            pass
    else:
        # read from environment only if not explicitly provided
        if os.environ.get("PROJECT_ID") is not None:
            return int(os.environ["PROJECT_ID"])

        # for ScrapyCloud jobs:
        jobkey = resolve_shub_jobkey()
        if jobkey:
            return int(jobkey.split("/")[0])

    # read from scrapinghub.yml
    try:
        from shub.config import load_shub_config

        cfg = load_shub_config()
        try:
            project_id = project_id or "default"
            return int(cfg.get_project_id(project_id))
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


DASH_RETRY_MAX = int(os.environ.get("DASH_RETRY_MAX", MINS_IN_A_DAY))
DASH_RETRY_WAIT_SECS = int(os.environ.get("DASH_RETRY_WAIT_SECS", ONE_MIN_IN_S))
DASH_RETRY_LOGGING_LEVEL = os.environ.get("DASH_RETRY_LOGGING_LEVEL", "ERROR")


dash_retry_decorator = retry(
    # ServerError is the only ScrapinghubAPIError that should be retried. Don't capture ScrapinghubAPIError here
    retry=retry_if_exception_type((ServerError, ReadTimeout, ConnectionError, HTTPError)),
    before_sleep=before_sleep_log(logger, getattr(logging, DASH_RETRY_LOGGING_LEVEL)),
    reraise=True,
    stop=stop_after_attempt(DASH_RETRY_MAX),
    wait=wait_fixed(DASH_RETRY_WAIT_SECS),
)

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
