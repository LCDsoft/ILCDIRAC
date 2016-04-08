#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
nosetests-2.7 -v --with-coverage --cover-html --cover-branches $DIR/cvmfstests.py