#!/bin/bash
source .gitlab-ci.d/set-reportstyle.sh
export PYTEST_ADDOPTS=$PYTEST_ADDOPTS" -m 'not integration'"
py.test
