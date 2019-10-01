#!/bin/env python
"""Script to call the DataRecoveryAgent functionality by hand."""
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base import Script


__RCSID__ = '$Id$'


class Params(object):
  """Collection of Parameters set via CLI switches."""

  def __init__(self):
    self.enabled = False
    self.prodID = 0

  def setEnabled(self, _):
    self.enabled = True
    return S_OK()

  def setProdID(self, prodID):
    self.prodID = int(prodID)
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch('P:', 'ProdID=', 'ProdID to Check/Fix', self.setProdID)
    Script.registerSwitch('X', 'Enabled', 'Enable the changes', self.setEnabled)
    Script.setUsageMessage('\n'.join([__doc__,
                                      '\nUsage:',
                                      '  %s [option|cfgfile] ...\n' % Script.scriptName]))


if __name__ == '__main__':
  PARAMS = Params()
  PARAMS.registerSwitches()
  Script.parseCommandLine(ignoreErrors=False)

  # Create Data Recovery Agent and run over single production.
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent import DataRecoveryAgent
  DRA = DataRecoveryAgent('ILCTransformation/DataRecoveryAgent', 'ILCTransformation/DataRecoveryAgent')
  DRA.jobStatus = ['Done', 'Failed']
  DRA.enabled = PARAMS.enabled
  TRANSFORMATION = TransformationClient().getTransformations(condDict={'TransformationID': PARAMS.prodID})
  if not TRANSFORMATION['OK']:
    gLogger.error('Failed to find transformation: %s' % TRANSFORMATION['Message'])
    exit(1)
  if not TRANSFORMATION['Value']:
    gLogger.error('Did not find any transformations')
    exit(1)
  TRANS_INFO_DICT = TRANSFORMATION['Value'][0]
  TRANS_INFO_DICT.pop('Body', None)
  gLogger.notice('Found transformation: %s' % TRANS_INFO_DICT)
  DRA.treatProduction(PARAMS.prodID, TRANS_INFO_DICT)
  exit(0)
