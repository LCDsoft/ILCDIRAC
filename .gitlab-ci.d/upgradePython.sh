#!/bin/bash

pip install -U distribute

pip install --trusted-host pypi.python.org --force-reinstall -U setuptools

pip install --trusted-host pypi.python.org --upgrade pytest mock MySQL-python pylint

easy_install pytest-cov
