#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source .gitlab-ci.d/set-reportstyle.sh
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} --cov-append --junitxml=junit_job.xml"
pytest -n 2 $DIR/../Tests/Integration/Test_Jobs.py
