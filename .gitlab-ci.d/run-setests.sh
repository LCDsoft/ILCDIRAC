#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
py.test $DIR/../Workflow/Modules/Test/Test_SEs.py