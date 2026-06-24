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
currently bundles five skills:

- **scanjobs-programs** — authoring and running the `scanjobs` job-scanning + plotting tool and its
  command-line "programs".
- **shub-workflow-scripts** — writing or fixing scripts built on the `shub_workflow.script` base
  classes (`BaseScript` / `BaseLoopScript` / `BaseLoopScriptAsyncMixin`), i.e. any script that runs
  on or operates on Scrapy Cloud.
- **shub-workflow-crawl-managers** — building, updating or understanding crawl managers
  (`CrawlManager` / `PeriodicCrawlManager` / `GeneratorCrawlManager` / `AsyncSchedulerCrawlManagerMixin`):
  the `set_parameters_gen()` pattern, outcome/retry hooks, and async scheduling.
- **shub-workflow-graph-managers** — building, updating or understanding graph managers
  (`GraphManager` + `Task` / `SpiderTask`): declaring a DAG of tasks in `configure_workflow()`,
  dependency linking, `on_finish`/retry routing, resources, and parallelization.
- **shub-workflow-monitors** — building, updating or understanding monitors (`BaseMonitor`):
  cross-job stat aggregation over a time window, ratios, reports, custom checks, and threshold
  alerts via Slack / Sentry.

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

## Updates

The plugin is **unversioned** (its `plugin.json` has no `version` field), so each commit pushed to
this repository is a new version. When Claude Code installs the plugin it **copies it into a local
cache** (`~/.claude/plugins/cache/`) and uses that copy — it does **not** read your working tree or
re-pull from GitHub on every session. You choose how new commits reach you:

- **Automatic.** Turn on auto-update for this marketplace: run `/plugin`, open the **Marketplaces**
  tab, and enable auto-update for `shub-workflow` (or set it in settings — see below). With this on,
  Claude Code re-pulls the marketplace from GitHub and updates installed plugins **at startup**, so a
  new session always loads the latest pushed commit. This is the low-friction option for staying
  current.

  ```json
  {
    "extraKnownMarketplaces": {
      "shub-workflow": {
        "source": { "source": "github", "repo": "scrapinghub/shub-workflow" },
        "autoUpdate": true
      }
    },
    "enabledPlugins": ["shub-workflow-toolkit@shub-workflow"]
  }
  ```

- **Manual.** Leave auto-update off (the default for third-party marketplaces). The cached copy stays
  pinned until you explicitly update — nothing changes under you between sessions. To pull the latest
  when you want it:

  ```
  /plugin marketplace update shub-workflow            # refresh the catalog from GitHub
  /plugin update shub-workflow-toolkit@shub-workflow  # update the installed plugin
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
