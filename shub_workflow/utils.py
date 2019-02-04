import os
import logging
import json
import subprocess
from retrying import retry

from scrapinghub import APIError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def resolve_project_id(project_id=None):
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
        return project_id

    # read from environment
    if os.environ.get('PROJECT_ID'):
        return os.environ.get('PROJECT_ID')

    # for ScrapyCloud jobs:
    if os.environ.get('SHUB_JOBKEY'):
        return os.environ['SHUB_JOBKEY'].split('/')[0]

    # read from scrapinghub.yml
    try:
        from shub.config import load_shub_config
        cfg = load_shub_config()
        project_id = cfg.get_project_id('default')
        if project_id:
            return project_id
    except:
        logger.warning("Install shub package if want to access scrapinghub.yml")

    if not project_id:
        logger.warning('Project id not found. Use either PROJECT_ID env. variable or scrapinghub.yml default target.')


MINS_IN_A_DAY = 24 * 60
ONE_MIN_IN_S = 60


def just_log_exception(exception):
    logger.error(str(exception))
    for etype in (KeyboardInterrupt, SystemExit, ImportError):
        if isinstance(exception, etype):
            return False
    logger.info("Waiting %d seconds", ONE_MIN_IN_S)
    return True  # retries any other exception


dash_retry_decorator = retry(retry_on_exception=just_log_exception, wait_fixed=ONE_MIN_IN_S*1000,
                             stop_max_attempt_number=MINS_IN_A_DAY)


@dash_retry_decorator
def schedule_script_in_dash(shubproject, cmd, tags=None, units=None, meta=None):
    """
    :type shubproject: scrapinghub.Project
    shubproject - an instance of python-scrapinghub project
    cmd - command line list. First element is script name and rest are options and arguments
    tags - a list of tags to be added to the job
    units - how many Kumo units assign to the job
    meta - a dict with metadata to add to job metadata
    """
    tags = tags or []
    meta = json.dumps(meta) if meta else None
    # lock = str(hash(tuple(cmd))) lock is still not a feature of dash schedule api
    scriptname = cmd[0]
    if not scriptname.startswith('py:'):
        scriptname = 'py:' + scriptname
    try:
        cmd_args = subprocess.list2cmdline(cmd[1:])
        schedule_kwargs = dict(
            spider=scriptname, cmd_args=cmd_args, add_tag=tags, units=units, meta=None)
        logger.info("Scheduling script:\n%s", schedule_kwargs)
        return shubproject.jobs.run(**schedule_kwargs)
    except APIError as e:
        raise RuntimeError(
            "Error scheduling script %s in %s: %s",
            scriptname, shubproject.id, str(e),
        )


def kumo_settings():
    if os.environ.get('SHUB_SETTINGS'):
        from sh_scrapy.env import decode_uri
        return decode_uri(os.environ.get('SHUB_SETTINGS')).get('project_settings', {})
    logging.info("Couldn't find Dash project settings, probably not running in Dash")
    return {}
