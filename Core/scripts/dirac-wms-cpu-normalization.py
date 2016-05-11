#!/usr/bin/env python
"""
  Determine Normalization for current CPU

  Forked from DIRAC.WorkloadManagementSystem.scripts

  :author:  Ricardo Graciani
"""
import DIRAC
from DIRAC.Core.Base import Script

__RCSID__ = "$Id$"

Script.registerSwitch( "U", "Update", "Update dirac.cfg with the resulting value" )
Script.registerSwitch( "R:", "Reconfig=", "Update given configuration file with the resulting value" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = True )

update = False
configFile = None

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "U", "Update" ):
    update = True
  elif unprocSw[0] in ( "R", "Reconfig" ):
    configFile = unprocSw[1]

def checkFunction():
  """ gets CPU normalisation from MFJ or calculate itself """
  from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getPowerFromMJF
  from ILCDIRAC.Core.Utilities.CPUNormalization import getCPUNormalization
  from DIRAC import gLogger, gConfig

  result = getCPUNormalization()

  if not result['OK']:
    gLogger.error( result['Message'] )

  norm = round( result['Value']['NORM'], 1 )

  gLogger.notice( 'Estimated CPU power is %.1f %s' % ( norm, result['Value']['UNIT'] ) )

  mjfPower = getPowerFromMJF()
  if mjfPower:
    gLogger.notice( 'CPU power from MJF is %.1f HS06' % mjfPower )
  else:
    gLogger.notice( 'MJF not available on this node' )

  if update and not configFile:
    gConfig.setOptionValue( '/LocalSite/CPUScalingFactor', mjfPower if mjfPower else norm )
    gConfig.setOptionValue( '/LocalSite/CPUNormalizationFactor', norm )

    gConfig.dumpLocalCFGToFile( gConfig.diracConfigFilePath )
  if configFile:
    from DIRAC.Core.Utilities.CFG import CFG
    cfg = CFG()
    try:
      # Attempt to open the given file
      cfg.loadFromFile( configFile )
    except:
      pass
    # Create the section if it does not exist
    if not cfg.existsKey( 'LocalSite' ):
      cfg.createNewSection( 'LocalSite' )
    cfg.setOption( '/LocalSite/CPUScalingFactor', mjfPower if mjfPower else norm )
    cfg.setOption( '/LocalSite/CPUNormalizationFactor', norm )

    cfg.writeToFile( configFile )


  DIRAC.exit()

if __name__ == "__main__":
  checkFunction()
