import logging
import json
import os
import hashlib
from typing import Optional

from tenacity import retry, retry_if_exception_type, before_sleep_log, stop_after_attempt, wait_fixed
from scrapinghub.client.exceptions import ServerError
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def hashstr(text: str) -> str:
    u = hashlib.sha1()
    u.update(text.encode("utf8"))
    return u.hexdigest()


def resolve_project_id(project_id=None) -> Optional[int]:
    """
    Gets project id from following sources in following order of precedence:
    - default parameter values
    - environment variables
    - sh_scrapy.hsref (kumo)
    - scrapinghub.yml file

    in order to allow to use codes that needs HS or dash API,
    either locally or from scrapinghub, correctly configured
    """
    if project_id:
        return int(project_id)

    # read from environment
    if os.environ.get("PROJECT_ID") is not None:
        return int(os.environ["PROJECT_ID"])

    # for ScrapyCloud jobs:
    if os.environ.get("SHUB_JOBKEY"):
        return int(os.environ["SHUB_JOBKEY"].split("/")[0])

    # read from scrapinghub.yml
    try:
        from shub.config import load_shub_config  # pylint: disable=import-error

        cfg = load_shub_config()
        project_id = cfg.get_project_id("default")
        if project_id:
            return int(project_id)
    except ImportError:
        logger.warning("Install shub package if want to access scrapinghub.yml")

    if not project_id:
        logger.warning("Project id not found. Use either PROJECT_ID env. variable or scrapinghub.yml default target.")

    return None


MINS_IN_A_DAY = 24 * 60
ONE_MIN_IN_S = 60


dash_retry_decorator = retry(
    retry=retry_if_exception_type((ServerError, ReadTimeout, ConnectionError, HTTPError)),
    before_sleep=before_sleep_log(logger, logging.ERROR),
    reraise=True,
    stop=stop_after_attempt(MINS_IN_A_DAY),
    wait=wait_fixed(ONE_MIN_IN_S),
)


def kumo_settings():
    settings = {}
    shub_job_data = json.loads(os.environ.get("SHUB_SETTINGS", "{}"))
    if shub_job_data:
        settings.update(shub_job_data["project_settings"])
        settings.update(shub_job_data["spider_settings"])
    else:
        logger.info("Couldn't find Dash project settings, probably not running in Dash")
    return settings


def get_project_settings():
    from scrapy.utils.project import get_project_settings as scrapy_get_project_settings  # pylint: disable=import-error

    settings = scrapy_get_project_settings()
    settings.setdict(kumo_settings(), priority="project")
    return settings
