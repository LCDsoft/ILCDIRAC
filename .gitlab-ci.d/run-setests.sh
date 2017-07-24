#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source .gitlab-ci.d/set-reportstyle.sh
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} --cov-append"
export XrdSecPROTOCOL=gsi,unix
py.test $DIR/../Workflow/Modules/Test/Test_SEs.py
