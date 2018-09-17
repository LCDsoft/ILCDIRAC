"""Print version of current iLCDirac installation.

Mandatory when submitting support requests

.. seealso::

  :doc:`AdministratorGuide/CommandReference/dirac-version`, :doc:`UserGuide/CommandReference/Others/dirac-info` and how
  to submit a :ref:`support-request`.

.. versionadded:: 23.0

:author: A. Sailer
"""

import pprint
import os

import DIRAC
from DIRAC import gConfig
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.PrettyPrint import printTable

import ILCDIRAC

__RCSID__ = "$Id$"


def _printVersions():
  """Print the ILCDIRAC and DIRAC versions and other information."""
  fields, records = getInfo()
  records.insert(0, ("ILCDirac Version:", ILCDIRAC.version))
  records.insert(1, ("DIRAC version:", DIRAC.version))

  printTable(fields, records, numbering=False)


def getInfo():
  """Retrieve information about setup, etc."""
  records = []

  records.append(('Setup', gConfig.getValue('/DIRAC/Setup', 'Unknown')))
  records.append(('ConfigurationServer', gConfig.getValue('/DIRAC/Configuration/Servers', [])))
  records.append(('Installation path', DIRAC.rootPath))

  if os.path.exists(os.path.join(DIRAC.rootPath, DIRAC.getPlatform(), 'bin', 'mysql')):
    records.append(('Installation type', 'server'))
  else:
    records.append(('Installation type', 'client'))

  records.append(('Platform', DIRAC.getPlatform()))

  ret = getProxyInfo(disableVOMS=False)
  if ret['OK']:
    print pprint.pformat(ret)
    if 'group' in ret['Value']:
      vo = getVOForGroup(ret['Value']['group'])
    else:
      vo = getVOForGroup('')
    if not vo:
      vo = "None"
    records.append(('VirtualOrganization', vo))
    if 'identity' in ret['Value']:
      records.append(('User DN', ret['Value']['identity']))
    if 'secondsLeft' in ret['Value']:
      records.append(('Proxy validity, secs', {'Value': str(ret['Value']['secondsLeft']), 'Just': 'L'}))

  if gConfig.getValue('/DIRAC/Security/UseServerCertificate', True):
    records.append(('Use Server Certificate', 'Yes'))
  else:
    records.append(('Use Server Certificate', 'No'))
  if gConfig.getValue('/DIRAC/Security/SkipCAChecks', False):
    records.append(('Skip CA Checks', 'Yes'))
  else:
    records.append(('Skip CA Checks', 'No'))

  try:
    import gfalthr  # pylint: disable=import-error
    records.append(('gfal version', gfalthr.gfal_version()))
  except BaseException:
    pass

  fields = ['Option', 'Value']

  return fields, records

if __name__ == "__main__":
  _printVersions()
