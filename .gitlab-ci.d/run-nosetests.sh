#!/bin/bash
pip install --upgrade pip
pip install mock
pip install nose
nosetests-2.7 -v --with-coverage --cover-html --cover-branches --cover-package=ILCDIRAC
