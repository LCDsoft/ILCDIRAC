#!/bin/bash
docker="/root/"
if [ "$HOME" == "$docker" ]
then
  # use default report style, but reset PYTEST_ADDOPTS in case of multiple runs
  export PYTEST_ADDOPTS=""
else
  # use nice html file output in default directory (htmlcov)
  export PYTEST_ADDOPTS="--cov-report html"
fi
