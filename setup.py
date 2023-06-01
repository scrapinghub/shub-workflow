# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name="shub-workflow",
    version="1.11.9",
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
        "sqlitedict==1.6.0",
        "s3fs>=0.4.0",
        "boto3>=1.9.92",
        "google-cloud-storage>=1.38.0",
        "bloom-filter2==2.0.0",
        "collection-scanner",
        "tenacity",
        "typing-extensions",
        "scrapy",
    ),
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
