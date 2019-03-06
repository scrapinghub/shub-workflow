"""
Utility for cloning ScrapyCloud jobs
Features tagging of cloned from/to jobs (both source and destination) and avoids to clone source jobs already cloned.
By default cloned jobs are scheduled in the same project as source job. If --project-id is given, target project
is overriden.
"""
import logging
from shub_workflow.script import BaseScript

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.INFO)


def _transform_cmd(job_cmd):
    if isinstance(job_cmd, list):
        return ' '.join(["'%s'" % cmd for cmd in job_cmd[1:]])

    return job_cmd


_COPIED_FROM_META = {'job_cmd':     ('cmd_args', _transform_cmd),
                     'units':       (None,       None),
                     'spider_args': ('job_args', None),
                     'tags':        ('add_tag',  None)}


def is_cloned(jobkey, client):
    job = client.get_job(jobkey)
    for tag in job.metadata.get('tags'):
        if tag.startswith('ClonedTo='):
            _LOG.warning(f'Job {job.key} already cloned. Skipped.')
            return True
    return False


class CloneJobScript(BaseScript):

    flow_id_required = False

    @property
    def description(self):
        return __doc__

    def _clone_job(self, job_key):
        job = self.client.get_job(job_key)

        spider = job.metadata.get('spider')

        job_params = dict()
        for key, (target_key, transform) in _COPIED_FROM_META.items():

            if target_key is None:
                target_key = key

            if transform is None:
                transform = lambda x: x

            job_params[target_key] = transform(job.metadata.get(key))
            job_params.setdefault('add_tag', []).append(f'ClonedFrom={job_key}')
            if self.args.units is not None:
                job_params['units'] = self.args.units

        project_id, spider_id, job_id = job_key.split('/')
        project = self.client.get_project(self.args.project_id or project_id)
        new_job = project.jobs.run(spider, **job_params)
        _LOG.info("Cloned %s to %s", job_key, new_job.key)
        jobtags = job.metadata.get('tags')
        jobtags.append(f'ClonedTo={new_job.key}')
        job.metadata.update({'tags': jobtags})

    def parse_project_id(self, args):
        project_id = super().parse_project_id(args)
        if project_id:
            return project_id
        if args.key:
            return args.key.split('/')[0]
        if args.tag_spider:
            return args.tag_spider.split('/')[0]

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument('--key', help='Target job key. Can be given multiple times', type=str,
                                    action='append', default=[])
        self.argparser.add_argument('--tag-spider', help='In format <project_id>/<tag>/<spider name>,'
                                    'clone given spider from given project id, by tag')
        self.argparser.add_argument('--units', help='Set number of units. Default is the same as cloned job.', type=int)

    def run(self):
        if self.args.key:
            keys = filter(lambda x: not is_cloned(x, self.client), self.args.key)
        elif self.args.tag_spider:
            keys = []
            project_id, tag, spider = self.args.tag_spider.split('/')
            for job in self.client.get_project(project_id).jobs.iter(spider=spider, status=['finished'], has_tag=tag):
                if not is_cloned(job['key'], self.client):
                    keys.append(job['key'])

        for job_key in keys:
            try:
                self._clone_job(job_key)
            except Exception as e:
                _LOG.error('Could not restart job %s: %s', job_key, e)


if __name__ == "__main__":
    script = CloneJobScript()
    script.run()
