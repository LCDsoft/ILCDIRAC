"""Unit tests for the CalibrationClient."""

import pytest
from mock import MagicMock as Mock
from DIRAC import S_OK
from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationClient

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.CalibrationSystem.Client.CalibrationClient'


@pytest.yield_fixture
def calibClient():
  """Create calibration handler."""
  calibClient = CalibrationClient(1, 1)
  return calibClient


def test_getInputDataDict(mocker):
  """Test getInputDataDict."""
  #  mocker.patch('%s.Client' % MODULE_NAME, new=Mock(return_value=True))
  from DIRAC.Core.Base.Client import Client
  mocker.patch.object(Client, '__init__', new=Mock(return_value=True))

  calibClientWithInputArguments = CalibrationClient(1, 1)

  tmpMock = Mock(name='instance')
  tmpMock.getInputDataDict.return_value = S_OK()
  mocker.patch.object(calibClientWithInputArguments, '_getRPC', return_value=tmpMock)

  res = calibClientWithInputArguments .getInputDataDict()
  assert res['OK']

  calibClientWoInputArguments = CalibrationClient()
  mocker.patch.object(calibClientWoInputArguments, '_getRPC', return_value=tmpMock)

  res = calibClientWoInputArguments .getInputDataDict(1, 1)
  assert res['OK']
