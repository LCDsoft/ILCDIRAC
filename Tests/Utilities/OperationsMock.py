""" module to mock Operations access in tests"""

from mock import MagicMock as Mock
from DIRAC import S_OK


def getOptionsDictMock(path, defValue=None):
  """ mock the getOptionsDict access for the operations helper """

  options = {'Production/ExperimentBasePaths': S_OK({'CLIC': '/ilc/prod/clic',
                                                     'ILC_ILD': '/ilc/prod/ilc/mc-dbd, /ilc/prod/ilc/mc-opt',
                                                     'ILC_SID': '/ilc/prod/ilc/sid',
                                                    }),
             }

  return options.get(path.strip('/'), defValue)


def createOperationsMock():
  """ create an Mock for the Operations Helper """
  opModuleMock = Mock()
  opModuleMock.name = "OperationsModule Mock"
  opMock = Mock()
  opModuleMock.return_value = opMock
  opMock.name = "OperationsClass Mock"

  opMock.getOptionsDict.side_effect = getOptionsDictMock
  return opModuleMock
