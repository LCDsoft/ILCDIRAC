#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source .gitlab-ci.d/set-reportstyle.sh
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} --cov-append --junit-xml=junit_se.xml"
export XrdSecPROTOCOL=gsi,unix
pytest -n2 $DIR/../Workflow/Modules/Test/Test_SEs.py
