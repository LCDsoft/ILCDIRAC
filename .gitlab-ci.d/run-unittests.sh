#!/bin/bash
source .gitlab-ci.d/set-reportstyle.sh
export PYTEST_ADDOPTS=$PYTEST_ADDOPTS" --ignore Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py --ignore Workflow/Modules/Test/Test_SEs.py"
py.test
