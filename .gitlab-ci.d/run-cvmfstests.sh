#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTEST_ADDOPTS="-s -v --cov --cov-report html -k test_ --ignore=Interfaces/API/NewInterface/Productions/TestProductionChain.py"
py.test $DIR/../Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py