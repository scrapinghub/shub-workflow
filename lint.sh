#!/bin/sh
result=0
# flake8 shub_workflow/
# result=$(($result | $?))
flake8 shub_workflow/ --select I100 --application-import-names=shub_workflow --import-order-style=pep8
result=$(($result | $?))
mypy --ignore-missing-imports shub_workflow/
result=$(($result | $?))
exit $result
