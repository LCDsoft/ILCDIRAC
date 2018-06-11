"""Mock objects for tests."""

from mock import MagicMock as Mock


def rpcMock(ret):
  """Return an RPCClient which returns **ret**."""
  rpcModuleMock = Mock(name="RPCClient Module")
  rpcClientMock = Mock(name="RPCClient Class")
  rpcClientMock.ping.return_value = ret
  rpcModuleMock.return_value = rpcClientMock
  return rpcModuleMock
