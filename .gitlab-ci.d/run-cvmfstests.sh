#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
py.test $DIR/cvmfstests.py
py.test $DIR/ddsimtest.py
py.test $DIR/setests.py