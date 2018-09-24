"""Mock objects for tests."""

from mock import MagicMock as Mock


def clientMock(ret):
  """Return an Client which returns **ret**."""
  clientModuleMock = Mock(name="Client Module")
  clientClassMock = Mock(name="Client Class")
  clientClassMock.ping.return_value = ret
  clientModuleMock.return_value = clientClassMock
  return clientModuleMock
