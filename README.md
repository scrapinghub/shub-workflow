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

# Note

The requirements for this library are defined in setup.py as usual. The Pipfile files in the repository don't define dependencies. It is only used
for setting up a development environment for shub-workflow library development and testing.
