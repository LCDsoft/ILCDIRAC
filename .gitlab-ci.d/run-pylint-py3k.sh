#!/bin/bash
find . -name Examples -prune -o -name .ropeproject -prune -o -name "*.py" -exec pylint -E --rcfile=.gitlab-ci.d/DIRAC.pylint.py3k.rc --py3k --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" {} +
