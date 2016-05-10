#!/bin/bash
export PYTEST_ADDOPTS="-s -v --cov --cov-report html -k test_ --ignore=Interfaces/API/NewInterface/Productions/TestProductionChain.py --ignore Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py --ignore Workflow/Modules/Test/Test_SEs.py"
py.test