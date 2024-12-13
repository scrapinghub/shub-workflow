
# Project Overview
A Set of Tools for Processing Workflows in ScrapyCloud

## Prerequisites
This project is built using Python. Before getting started, ensure you have the following installed:
- Python 
- Pip 

## Installation Steps:
Choose one of the following options to install the project:

### Option 1: Manual Installation
Run the following command to install:
```
pip install shub-workflow
```

To include support for S3 tools:
```
pip install shub-workflow[with-s3-tools]
```

To include support for Google Cloud Storage tools:
```
pip install shub-workflow[with-gcs-tools]
```

### Verification
To verify successful installation, run:
```
python -m shub_workflow --help
```

### For Developers
To set up a development environment:
1. Clone or fork the repository.
2. Install dependencies:
   ```
   pipenv install --dev
   ```
3. Set up the pre-commit hook:
   ```
   cp pre-commit .git/hooks/
   ```

To initiate the environment:
```
pipenv shell
```

To lint the code, use the `lint.sh` script from the repository root:
```
./lint.sh
```

This script runs code checks for PEP8 compliance (via flake8) and typing integrity (via mypy). It is also executed on each `git commit` if the pre-commit hook is installed.

## External Documents
Refer to the [Project Wiki](https://github.com/scrapinghub/shub-workflow/wiki) for comprehensive documentation, including usage examples.

## Notes
The requirements for this library are defined in `setup.py`. The `Pipfile` and `Pipfile.lock` are for setting up the development environment only and do not define runtime dependencies.
