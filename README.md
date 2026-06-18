A set of tools for controlling processing workflow with spiders and script running in scrapinghub ScrapyCloud.

# Installation

```
pip install shub-workflow
```

If you want to support s3 tools:

```
pip install shub-workflow[with-s3-tools]
```

For google cloud storage tools support:

```
pip install shub-workflow[with-gcs-tools]
```

# Usage

Check [Project Wiki](https://github.com/scrapinghub/shub-workflow/wiki) for documentation. You can also see code tests for lots of examples of usage.

# Claude Code plugin

shub-workflow ships a [Claude Code](https://claude.com/claude-code) plugin,
**shub-workflow-toolkit**, that gives Claude working knowledge of shub-workflow tooling. It
currently bundles two skills:

- **scanjobs-programs** — authoring and running the `scanjobs` job-scanning + plotting tool and its
  command-line "programs".
- **shub-workflow-scripts** — writing or fixing scripts built on the `shub_workflow.script` base
  classes (`BaseScript` / `BaseLoopScript` / `BaseLoopScriptAsyncMixin`), i.e. any script that runs
  on or operates on Scrapy Cloud.

Install it from this repository's plugin marketplace, from inside Claude Code:

```
/plugin marketplace add scrapinghub/shub-workflow
/plugin install shub-workflow-toolkit@shub-workflow
```

To enable it automatically for a project, add it to that project's `.claude/settings.json`:

```json
{
  "enabledPlugins": ["shub-workflow-toolkit@shub-workflow"]
}
```

The plugin lives in [`plugins/shub-workflow-toolkit/`](plugins/shub-workflow-toolkit); the
marketplace manifest is [`.claude-plugin/marketplace.json`](.claude-plugin/marketplace.json).

# Note

The requirements for this library are defined in setup.py as usual. The Pipfile files in the repository don't define dependencies. It is only used
for setting up a development environment for shub-workflow library development and testing.


# For developers

For installing a development environment for shub-workflow, the package comes with Pipfile and Pipfile.lock files. So, clone or fork the repository and do:

```
> pipenv install --dev
> cp pre-commit .git/hooks/
```

for installing the environment, and:

```
> pipenv shell
```

for initiating it.

There is a script, lint.sh, that you can run everytime you need from the repo root folder, but it is also executed each time you do `git commit` (provided
you installed the pre-commit hook during the installation step described above). It checks code pep8 and typing integrity, via flake8 and mypy.

```
> ./lint.sh
```
