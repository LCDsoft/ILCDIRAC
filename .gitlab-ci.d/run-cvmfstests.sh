#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
py.test $DIR/../Interfaces/API/NewInterface/Tests/Test_FullCVMFSTests.py