"""Test for the SoftwareVersions Executor."""

import pytest
from mock import MagicMock, patch

from DIRAC import gLogger, S_OK


@pytest.fixture
def svExecutor(mocker):
  """Mock the SoftwareVersions executor."""
  class FakeExecutorModule(object):
    log = gLogger

    def __init__(self, *args, **kwargs):
      self.log = gLogger
      self.ex_optimizerName = lambda: "optimus prime"

    @classmethod
    def ex_getOption(cls, *args, **kwargs):
      return {'BanLists': ['1', '2'],
              '1Reason': 'BadSoftware',
              '1Sites': ['Site1', 'Site2'],
              '2Reason': 'BadVersion',
              '2Sites': ['Site2', 'Site3'],
              }[args[0]]

    def setNextOptimizer(self, *args, **kwargs):
      return S_OK()

  mocker.patch('ILCDIRAC.WorkloadManagementSystem.Executor.SoftwareVersions.OptimizerExecutor',
               new=MagicMock(spec="DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor.OptimizerExecutor"))

  from ILCDIRAC.WorkloadManagementSystem.Executor import SoftwareVersions
  executorClass = SoftwareVersions.SoftwareVersions
  patchBase = patch.object(executorClass, '__bases__', (FakeExecutorModule,))
  with patchBase:
    patchBase.is_local = True
    SoftwareVersions.SoftwareVersions.initializeOptimizer()
    theExecutor = SoftwareVersions.SoftwareVersions()

  theExecutor.setNextOptimizer = MagicMock()
  return theExecutor


@pytest.fixture
def aJobState():
  """Return a jobState mock."""
  js = MagicMock(name="JobState")
  jm = MagicMock(name="JobManifest")

  def _jsOptions(*args, **kwargs):
    return {'SoftwarePackages': ['BadSoftware', 'BadVersion'],
            'BannedSites': [],
            'BannedSite': [],
            }[args[0]]
  jm.getOption = _jsOptions
  jm.setOption = MagicMock(name="setOption")

  js.getManifest.return_value = S_OK(jm)
  js.JM = jm  # for fast access
  return js


def test_optimizeJob(svExecutor, aJobState):
  """Test the optimizeJob function."""
  gLogger.setLevel('DEBUG')
  assert 'BadSoftware' in svExecutor._SoftwareVersions__softToBanned
  res = svExecutor.optimizeJob(1234, aJobState)
  assert res['OK']
  aJobState.JM.setOption.assert_called_with('BannedSites', 'Site2, Site3, Site1')
