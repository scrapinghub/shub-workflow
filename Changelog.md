Mayor versions changes
======================

W.X.Y.Z

W - Package version
X - Major version. Introduces backward incompatibilities.
Y - Minor version. Introduces new features.
Z - Micro version. Fixes and improvements.

Fixes and performance improvements are done
along all the way and are not indicated there.

In general you can find most incompatibilities by generous using of typing hints and mypy in your project.

1.9 (From July 2022 to October 2022)
------------------------------------

Backward incompatibility issues:

- Python older than 3.8 is not supported anymore
- name attribute is now required for every subclass of WorkFlowManager (either as hardcoded attribute, or passed via command line required arguments)
- The Delivery script has been refactored. The old code is deprecated. It will be removed in future versions.

Minor version changes:

- 1.9.0 crawlmanager `bad_outcome_hook()`
- 1.9.1 crawlmanager ability to resume
- 1.9.2 implicit crawlmanager resumability via flow id (before a command line option was required)
- 1.9.3 performance improvements
- 1.9.4 name attribute logic fixes
- 1.9.5 delivery script refactor, old delivery code deprecated.
- 1.9.6 diverse fixes and improvements in base script, added `get_jobs_with_tags()` method.
- 1.9.7 refactors and new s3 helper method

1.10 (From October 2022 to April 2023)
--------------------------------------

Backward incompatibility issues:

- Backward incompatibilities may come from the massive introduction of typing hints and fixes in types consistencies, specially when you override some methods.
  Many of the incompatibities you may find can be seen in advance by using mypy in your project and using typing abundantly in the classes that
  use shub-workflow.

Minor version changes:

- 1.10.1 continuation of typing hints massive adoption
- 1.10.2 continuation of typing hints massive adoptio
- 1.10.3 new BaseLoopScript class
- 1.10.4 `script_args()` context manager
- 1.10.5 some refactor and improvements in new delivery script.
- 1.10.6 crawl manager new async schedule mode
- 1.10.7 crawl manager extension of async scheduling
- 1.10.8 max running time for all scripts
- 1.10.9 performance improvements of resume feature
- 1.10.10 new method for async tagging of jobs
- 1.10.11 async tagging on delivery script
- 1.10.12 introduction of script stats
- 1.10.13 crawlmanager stats, delivery stats, AWS email utils.
- 1.10.14 graph manager ability to resume
- 1.10.15 graph manager `bad_outcome_hook()`
- 1.10.16 max running time feature on delivery script
- 1.10.17 spider loader object in base script
- 1.10.18 default implementation of `bad_outcome_hook()` on generator crawl manager
- 1.10.19 some refactoring of base script.
- 1.10.20 base script: resolve project id before parsing options
- 1.10.21 minor improvement in base script spider loader object
- 1.10.22 crawlmanager `finished_ok_hook()` method
- 1.10.23 generator crawlmanager that handle multiple spiders in same process (not good experimient, will probably be deprecated in future)

1.11 (From April 2023 to June 2023)
-----------------------------------

Backward incompatibility issues:

- Definitively removed old legacy delivery class
- Dupefilter classes moved from delivery folder into utils


Minor version changes:

- 1.11.1 replaced `bloom_filter` dependency by `bloom_filter2`
- 1.11.2 typing improvements
- 1.11.3 method for removing jobs tags
- 1.11.4 method for reading log level from kumo and use on script main() function.
- 1.11.5 added methods for working with Google Cloud Storage, with same interface than already existin ones for AWS S3
- 1.11.6 generator crawlmanager: method for computing max jobs per spider
- 1.11.7 GCS additions
- 1.11.8 typing hint updates
- 1.11.9 added support for python 3.11

1.12 (From June 2023 to September 2023)
---------------------------------------

Backward incompatibility issues:

- make installation of s3 and gcs depedencies optional (with shub-workflow[with-s3-tools] and shub-workflow[with-gcs-tools])

Minor version changes:
- 1.12.1 more typing addition and improvements, and related refactoring
- 1.12.2 generator crawlmanager: some methods for conditioning scheduling of new spider jobs
- 1.12.3 reimplementation of `upload_file` s3 util using boto3, for improved performance
- 1.12.4 improvements in job clonner class

1.13 (From September 2023 to June 2024)
---------------------------------------

Backward incompatibility issues:

- Some changes in delivery script interface

Minor version changes:
- 1.13.0 new configuration watchdog script
- 1.13.1 generator crawlmanager: added method for determining retry parameters when a job is retried from default `bad_outcome_hook()`  method
- 1.13.2 generator crawlmanager: additional featuring on multiple spiders handling
- 1.13.3 configurable retry logging via environment variables
- 1.13.4 base script: some handlers for scheduling methods
- 1.13.5 additions in GCS utils
- 1.13.6 additions in GCS utils
- 1.13.7 base script: print stats on close
- 1.13.8 avoid multiple warnings on `kumo_settings()` function. Additions in GCS utils

1.14 (From June 2024 to present)
--------------------------------

Backward incompatibility issues:

- Moved filesystem utils (S3 and GCS utils) from deliver folder into utils folder

Minor version changes:

- 1.14.0 generator crawl manager: acquire all jobs if crawlmanager doesn't have a flow id.
  Added `get_canonical_spidername()` and `get_project_running_spiders()` helper methods on base script.
  Added fshelper base script attribute for readily access to this helper tool
- 1.14.1 Added method for getting alive real time settings from ScrapyCloud
- 1.14.2 Added BaseMonitor class that is able to monitor aggregated stats on entire workflow jobs.
- 1.14.3 Mixin for provision of Sentry alert capabilities in monitor.
- 1.14.4 Monitor ratios
- 1.14.5 Finished job metadata hook
- 1.14.6 Allow to use project entry keyword in scrapinghub.yml as alternative to project numeric id, when passing command line --project-id option.
- 1.14.7 Mixin for provision of Slack alert capabilities in monitor.
- 1.14.8 Allow to load settings from SC when running script on local environment.
- 1.14.9 Added stats aggregation capabilities to crawlmanager
- 1.14.10 AlertSender class to allow both slack and sentry alerts combined.
- 1.14.11 Created SlackSender class for easier reusage of slack messaging
- 1.14.12 SlackSender: allow to send attachments
- 1.14.13 Extended monitor to be able to generate reports
- 1.14.14 Monitor improvements: target_spider_stats can be a regex, and allow to print report tables suitable for copy/paste over spreadsheet
- 1.14.15 Allow to tune how finished and running jobs are acquired, via a couple of new attributes
- 1.14.15 added mixin for allowing scripts to issue items on SC
