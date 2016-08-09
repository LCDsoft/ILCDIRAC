#!/bin/bash
# Script that computes the total coverage of all tests by executing the unittests, then the CVMFS tests and the storage element tests and adds their coverage to the unit test coverage.
# Note that this script requires a proxy to be executed
echo "Please check that a valid dirac proxy is available before executing the cvmfs tests."
source .gitlab-ci.d/set-reportstyle.sh
py.test Workflow/Modules/Test/Test_SEs.py
export PYTEST_ADDOPTS=$PYTEST_ADDOPTS" --cov-append"
py.test Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py
export PYTEST_ADDOPTS=$PYTEST_ADDOPTS" -m 'not integration'"
py.test
