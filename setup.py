# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name="shub-workflow",
    version="1.14.21.4",
    description="Workflow manager for Zyte ScrapyCloud tasks.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="BSD",
    url="https://github.com/scrapinghub/shub-workflow",
    maintainer="Scrapinghub",
    packages=find_packages(),
    install_requires=(
        "pyyaml>=3.12",
        "scrapinghub[msgpack]>=2.3.1",
        "jinja2>=2.7.3",
        "sqlitedict==2.1.0",
        "boto3>=1.9.92",
        "bloom-filter2==2.0.0",
        "collection-scanner",
        "tenacity",
        "typing-extensions",
        "scrapy",
        "prettytable",
        "jmespath",
        "timelength",
    ),
    extras_require = {
        "with-s3-tools": ["s3fs>=0.4.0"],
        "with-gcs-tools": ["google-cloud-storage>=1.38.0"],
    },
    scripts=[],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    package_data={"shub_workflow": ["py.typed"]},
)
