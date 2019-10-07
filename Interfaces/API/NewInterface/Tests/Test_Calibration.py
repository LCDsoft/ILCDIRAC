"""Unit tests for the Calibration application."""

from __future__ import print_function
#  import unittest
#  import pytest
#  import os
#  import shutil
#  import time
#  from datetime import datetime
#  from datetime import timedelta
#  from xml.etree import ElementTree as et
#  from shutil import copyfile
#  from DIRAC import S_OK, S_ERROR
#  from mock import patch
#  from mock import MagicMock as Mock
from ILCDIRAC.Interfaces.API.NewInterface.Applications.Calibration import Calibration

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Interfaces.API.NewInterface.Applications.Calibration'


def test_init():
  """Test initialization."""
  calApp = Calibration()
  assert calApp._modulename == "Calibration"
  assert calApp.appname == "marlin"


def test_setCalibrationID():
  """Test setCalibrationID function."""
  calApp = Calibration()

  # correct input
  arg = 3
  assert not calApp._errorDict
  calApp.setCalibrationID(arg)
  assert not calApp._errorDict

  # wrong input
  arg = 'some string'
  calApp.setCalibrationID(arg)
  assert calApp._errorDict


def test_setWorkerID():
  """Test setCalibrationID function."""
  calApp = Calibration()

  # correct input
  arg = 3
  assert not calApp._errorDict
  calApp.setWorkerID(arg)
  assert not calApp._errorDict

  # wrong input
  arg = 'some string'
  calApp.setWorkerID(arg)
  assert calApp._errorDict
