#!/bin/sh
result=0
flake8 shub_workflow/ tests/ --application-import-names=shub_workflow --import-order-style=pep8
result=$(($result | $?))
mypy --ignore-missing-imports --disable-error-code=method-assign --check-untyped-defs shub_workflow/ tests/
result=$(($result | $?))
exit $result
