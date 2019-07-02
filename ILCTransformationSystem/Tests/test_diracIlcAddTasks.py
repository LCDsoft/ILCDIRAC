"""Test the dirac-ilc-add-tasks-to-prod."""


import importlib

import pytest

from mock import MagicMock as Mock

from DIRAC import S_OK, S_ERROR


# pylint: disable=protected-access, invalid-name, missing-docstring, redefined-outer-name
THE_SCRIPT = 'ILCDIRAC.ILCTransformationSystem.scripts.dirac-ilc-add-tasks-to-prod'
theScript = importlib.import_module(THE_SCRIPT)

__RCSID__ = '$Id$'


def transInfoDict(maxTasks, groupSize, plugin):
  """Return the Dictionary with some transformation information."""
  return {'MaxNumberOfTasks': maxTasks,
          'GroupSize': groupSize,
          'Plugin': plugin,
          }


@pytest.fixture
def theParams():
  """Return the Params fixture."""
  return theScript._Params()


@pytest.fixture
def tcClient():
  """Return a TransformationClient Mock."""
  tc = Mock(name='TCClient')
  tc.getTransformation = Mock(return_value=S_OK(transInfoDict(33, 1, 'Standard')))
  tc.setTransformationParameter = Mock(return_value=S_OK())
  return tc


def test_params_init(theParams):
  """Test the Params constructor."""
  assert theParams.prod == 0
  assert theParams.tasks == 0
  assert not theParams.total


def test_params_settters(theParams):
  """Test the Params setters."""
  assert theParams.setProd(12345)['OK']
  assert theParams.prod == 12345
  assert theParams.setProd('555')['OK']
  assert theParams.prod == 555
  assert not theParams.setProd('transName')['OK']

  assert theParams.setNbTasks(123)['OK']
  assert theParams.tasks == 123

  assert theParams.setNbTasks('333')['OK']
  assert theParams.tasks == 333

  assert not theParams.setNbTasks('taskThis')['OK']

  assert theParams.setTotal('dummy')['OK']
  assert theParams.total


def test_params_getMaxTasks(theParams):
  theParams.tasks = 10
  theParams.total = False
  assert theParams.getMaxTasks(10) == 20

  theParams.tasks = 20
  theParams.total = True
  assert theParams.getMaxTasks(10) == 20

  theParams.tasks = 10
  theParams.total = True
  with pytest.raises(theScript._Skip):
    theParams.getMaxTasks(10)


def test_extendStandard_10(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = 10
  theParams.total = False
  trans = transInfoDict(20, 1, 'Standard')
  assert theScript._extendStandard(theParams, tcClient, trans) == 0
  tcClient.setTransformationParameter.assert_called_once_with(111, 'MaxNumberOfTasks', 30)


def test_extendStandard_minus1(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = -1
  theParams.total = False
  trans = transInfoDict(20, 1, 'Standard')
  assert theScript._extendStandard(theParams, tcClient, trans) == 1
  tcClient.setTransformationParameter.assert_not_called()


def test_extendStandard_10_fail(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = 10
  theParams.total = False
  trans = transInfoDict(20, 1, 'Standard')
  tcClient.setTransformationParameter = Mock(return_value=S_ERROR('Failed'))
  assert theScript._extendStandard(theParams, tcClient, trans) == 1
  tcClient.setTransformationParameter.assert_called_once_with(111, 'MaxNumberOfTasks', 30)


def test_extendLimited_10(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = 10
  theParams.total = False
  trans = transInfoDict(20, 1, 'Limited')
  assert theScript._extendLimited(theParams, tcClient, trans) == 0
  tcClient.setTransformationParameter.assert_called_once_with(111, 'MaxNumberOfTasks', 30)


def test_extendLimited_minus1(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = -1
  theParams.total = False
  trans = transInfoDict(20, 1, 'Limited')
  assert theScript._extendLimited(theParams, tcClient, trans) == 0
  tcClient.setTransformationParameter.assert_called_once_with(111, 'MaxNumberOfTasks', -1)


def test_extendLimited_0(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = 0
  theParams.total = False
  trans = transInfoDict(20, 1, 'Limited')
  assert theScript._extendLimited(theParams, tcClient, trans) == 1
  tcClient.setTransformationParameter.assert_not_called()


def test_extendLimited_10_fail(theParams, tcClient):
  theParams.prod = 111
  theParams.tasks = 10
  theParams.total = False
  trans = transInfoDict(20, 1, 'Limited')
  tcClient.setTransformationParameter = Mock(return_value=S_ERROR('Failed'))
  assert theScript._extendLimited(theParams, tcClient, trans) == 1
  tcClient.setTransformationParameter.assert_called_once_with(111, 'MaxNumberOfTasks', 30)


def test_extend_fail(theParams, tcClient):
  posArgs = []
  theScript._getTransformationClient = Mock(return_value=tcClient)
  assert theScript._extend(theParams, posArgs) == 1


def test_extend_fail_pos(theParams, tcClient):
  posArgs = [111, 222, 333]
  theScript._getTransformationClient = Mock(return_value=tcClient)
  assert theScript._extend(theParams, posArgs) == 1


def test_extend_fail_mix(theParams, tcClient):
  posArgs = [111, 222]
  theParams.prod = 444
  theParams.tasks = 555
  theScript._getTransformationClient = Mock(return_value=tcClient)
  assert theScript._extend(theParams, posArgs) == 1


def test_extend_fail_zero(theParams, tcClient):
  posArgs = [0, 0]
  theParams.prod = 0
  theParams.tasks = 0
  theScript._getTransformationClient = Mock(return_value=tcClient)
  assert theScript._extend(theParams, posArgs) == 1


def test_extend_posArgs(theParams, tcClient):
  posArgs = [123, 456]
  theParams.prod = 0
  theParams.tasks = 0
  theScript._getTransformationClient = Mock(return_value=tcClient)
  assert theScript._extend(theParams, posArgs) == 0


def test_extend_standard(theParams, tcClient):
  theParams.prod = 123
  theParams.tasks = 22
  posArgs = []
  theScript._getTransformationClient = Mock(return_value=tcClient)
  assert theScript._extend(theParams, posArgs) == 0
  tcClient.getTransformation.assert_called_once_with(123)


def test_extend_limited(theParams, tcClient):
  theParams.prod = 123
  theParams.tasks = 22
  posArgs = []
  theScript._getTransformationClient = Mock(return_value=tcClient)
  tcClient.getTransformation = Mock(return_value=S_OK(transInfoDict(33, 1, 'Limited')))

  assert theScript._extend(theParams, posArgs) == 0
  tcClient.getTransformation.assert_called_once_with(123)


def test_extend_broad(theParams, tcClient):
  theParams.prod = 123
  theParams.tasks = 22
  posArgs = []
  theScript._getTransformationClient = Mock(return_value=tcClient)
  tcClient.getTransformation = Mock(return_value=S_OK(transInfoDict(33, 1, 'BroadCast')))
  assert theScript._extend(theParams, posArgs) == 1
  tcClient.getTransformation.assert_called_once_with(123)
