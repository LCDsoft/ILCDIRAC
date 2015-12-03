#!/bin/bash
pip install --upgrade pip
pip install pylint
find . -name Examples -prune -o -name .ropeproject -prune -o -name "*.py" -exec pylint -E --rcfile=.gitlab-ci.d/DIRAC.pylint.rc --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" {} +
