"""
The project-base-mixin pattern.

Projects usually centralize shared CLI options and helpers in ONE mixin, then every concrete script
inherits `class X(ProjectMixin, BaseScript)`. The mixin inherits the typing-only Protocol
(BaseScriptProtocol) — NOT BaseScript — so it can call/typecheck base methods without re-inheriting
the implementation (which would duplicate it in the MRO).
"""
import logging

from shub_workflow.script import BaseScript, BaseScriptProtocol

LOG = logging.getLogger(__name__)


class MyProjectScriptMixin(BaseScriptProtocol):
    """Shared options/helpers for every script in this project."""

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("--env", default="prod", choices=["dev", "prod"])

    def shared_helper(self) -> str:
        # self.project_id, self.stats, self.get_jobs(...), etc. are all available (declared on the Protocol)
        return f"project={self.project_id} env={self.args.env}"


class MyScript(MyProjectScriptMixin, BaseScript):  # mixin FIRST, BaseScript LAST

    @property
    def description(self) -> str:
        return "A concrete script built on the project mixin."

    def run(self):
        LOG.info(self.shared_helper())


# A loop variant reuses the same mixin: class MyLoop(MyProjectScriptMixin, BaseLoopScript): ...


if __name__ == "__main__":
    from shub_workflow.utils import get_kumo_loglevel

    logging.basicConfig(format="%(asctime)s %(name)s [%(levelname)s]: %(message)s", level=get_kumo_loglevel())
    script = MyScript()
    script.run()
