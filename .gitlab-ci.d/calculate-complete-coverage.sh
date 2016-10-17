#!/bin/bash
# Script that computes the total coverage of all tests by executing the unittests, then the CVMFS tests and the storage element tests and adds their coverage to the unit test coverage.
# Note that this script requires a proxy to be executed
echo "Please check that a valid dirac proxy is available before executing the cvmfs tests."
source .gitlab-ci.d/set-reportstyle.sh
py.test Workflow/Modules/Test/Test_SEs.py

SE_RESULT=""
if [ $? -eq 0 ]
then
  SE_RESULT="Storage element tests successful"
else
  SE_RESULT="Storage element tests failed! Check what failed."
fi

export PYTEST_ADDOPTS=$PYTEST_ADDOPTS" --cov-append"
py.test Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py

CVMFS_RESULT=""
if [ $? -eq 0 ]
then
  CVMFS_RESULT="CVMFS system tests successful"
else
  CVMFS_RESULT="CVMFS system failed! Check what failed."
fi

export PYTEST_ADDOPTS=$PYTEST_ADDOPTS" -m 'not integration'"
py.test

UNIT_RESULT=""
if [ $? -eq 0 ]
then
  UNIT_RESULT="Unit tests successful"
else
  UNIT_RESULT="Unit tests failed! Check what failed."
fi

echo "########################################################################"
echo "Reporting results of tests..."
echo "########################################################################"
echo $SE_RESULT
echo $CVMFS_RESULT
echo $UNIT_RESULT
