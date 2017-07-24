#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source .gitlab-ci.d/set-reportstyle.sh
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} --cov-append"
py.test $DIR/../Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py
