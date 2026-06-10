# Claude Code skills

[Claude Code](https://claude.com/claude-code) skills shipped with shub-workflow. They give Claude
working knowledge of shub-workflow tooling so it can help you author and run it correctly.

Skills are not loaded automatically — copy the ones you want into your **personal** Claude scope:

```
cp -r skills/scanjobs-programs ~/.claude/skills/
```

(or symlink it, if you'd rather track this checkout). Each skill is a self-contained directory; only
its `name` + `description` stay in context until Claude decides it's relevant, so installing a skill
you don't use costs nothing.

## Available skills

- **scanjobs-programs** — authoring, editing, and running `scanjobs.py` *programs* (the predefined
  `-g <alias>` parameter sets in a project's `scripts/scanjobs.py` `PROGRAMS` dict), including the
  postscript post-processor (`-c`) and the `--plot` mini-language. See
  [scanjobs-programs/SKILL.md](scanjobs-programs/SKILL.md).
