"""
Utility for cloning ScrapyCloud jobs
Features tagging of cloned from/to jobs (both source and destination) and avoids to clone source jobs already cloned.
By default cloned jobs are scheduled in the same project as source job. If --project-id is given, target project
is overriden.
"""
import logging
from typing import Optional, List

from scrapinghub.client.jobs import Job
from scrapinghub import DuplicateJobError

from shub_workflow.script import BaseScript, JobKey
from shub_workflow.utils import dash_retry_decorator


_LOG = logging.getLogger(__name__)


def _transform_cmd(job_cmd):
    if isinstance(job_cmd, list):
        return " ".join(["'%s'" % cmd for cmd in job_cmd[1:]])

    return job_cmd


_COPIED_FROM_META = {
    "job_cmd": ("cmd_args", _transform_cmd),
    "units": (None, None),
    "spider_args": ("job_args", None),
    "tags": ("add_tag", None),
    "job_settings": (None, None),
}


class BaseClonner(BaseScript):

    MAX_CLONES = 10

    def is_cloned(self, jobkey: JobKey):
        for tag in self.get_job_tags(jobkey):
            if tag.startswith("ClonedTo="):
                _LOG.warning(f"Job {jobkey} already cloned. Skipped.")
                return True
        return False

    def job_params_hook(self, job_params):
        pass

    def clone_job(
        self, job_key: JobKey, units: Optional[int] = None, extra_tags: Optional[List[str]] = None
    ) -> Optional[Job]:
        extra_tags = extra_tags or []
        job = self.get_job(job_key)

        spider = self.get_metadata_key(job.metadata, "spider")

        job_params = dict()
        for key, (target_key, _) in _COPIED_FROM_META.items():

            if target_key is None:
                target_key = key

            job_params[target_key] = self.get_metadata_key(job.metadata, key)

        clone_number = 0
        add_tag = job_params.setdefault("add_tag", [])

        add_tag = list(filter(lambda x: not x.startswith("ClonedFrom="), add_tag))
        add_tag.append(f"ClonedFrom={job_key}")

        for tag in add_tag:
            if tag.startswith("CloneNumber="):
                clone_number = int(tag.replace("CloneNumber=", ""))
                break

        clone_number += 1

        if clone_number >= self.MAX_CLONES:
            _LOG.warning(f"Already reached max clones allowed for job {job_key}.")
            return None

        add_tag = list(filter(lambda x: not x.startswith("CloneNumber="), add_tag))
        add_tag.append(f"CloneNumber={clone_number}")

        add_tag.extend(extra_tags)
        job_params["add_tag"] = add_tag
        if units is not None:
            job_params["units"] = units

        self.job_params_hook(job_params)

        for key, (target_key, transform) in _COPIED_FROM_META.items():

            target_key = target_key or key

            if transform is None:

                def transform(x):
                    return x

            job_params[target_key] = transform(job_params[target_key])

        project_id, _, _ = job_key.split("/")
        project = self.get_project(self.project_id or project_id)
        new_job = self.schedule_generic(project, spider, **job_params)
        if new_job is not None:
            _LOG.info("Cloned %s to %s", job_key, new_job.key)
            jobtags = self.get_metadata_key(job.metadata, "tags")
            jobtags.append(f"ClonedTo={new_job.key}")
            self._update_metadata(job.metadata, {"tags": jobtags})

        return new_job

    @dash_retry_decorator
    def schedule_generic(self, project, spider, **job_params) -> Optional[Job]:
        try:
            return project.jobs.run(spider, **job_params)
        except DuplicateJobError as e:
            _LOG.error(str(e))
        return None


class CloneJobScript(BaseClonner):

    flow_id_required = False

    @property
    def description(self):
        return __doc__

    def parse_project_id(self, args):
        project_id = super().parse_project_id(args)
        if project_id:
            return project_id
        if args.key:
            return args.key[0].split("/")[0]

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument(
            "key",
            type=str,
            nargs="+",
            default=[],
            help="Target job key. Can be given multiple times. All must be in same project.",
        )
        self.argparser.add_argument("--units", help="Set number of units. Default is the same as cloned job.", type=int)

    def run(self):
        keys = list(filter(lambda x: not self.is_cloned(x), self.args.key))
        for job_key in keys:
            try:
                self.clone_job(job_key, self.args.units, self.args.children_tag)
            except Exception as e:
                _LOG.error("Could not restart job %s: %s", job_key, e)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(name)s [%(levelname)s]: %(message)s",
        level=logging.DEBUG,
    )
    script = CloneJobScript()
    script.run()
