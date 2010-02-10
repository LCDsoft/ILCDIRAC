# $HeadURL$
# $Id$
#!/usr/bin/env python
########################################################################
# File :   lcd-proxy-init.py
# Author : Adrian Casajus, adapted for LCD by S POSS
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1. $"

import sys
import os
import getpass
import imp
import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.FrameworkSystem.Client.ProxyGeneration import CLIParams, generateProxy
from DIRAC.FrameworkSystem.Client.ProxyUpload import uploadProxy

cliParams = CLIParams()
cliParams.registerCLISwitches()

Script.disableCS()
Script.parseCommandLine()

diracGroup = cliParams.getDIRACGroup()
time = cliParams.getProxyLifeTime()

retVal = generateProxy( cliParams )
if not retVal[ 'OK' ]:
  print "Can't create a proxy: %s" % retVal[ 'Message' ]
  sys.exit(1)

from DIRAC import gConfig
from DIRAC.Core.Security import CS, Properties
from DIRAC.Core.Security.Misc import getProxyInfo
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.VOMS import VOMS

def uploadProxyToMyProxy( params, DNAsUsername ):
  myProxy = MyProxy()
  if DNAsUsername:
    params.debugMsg( "Uploading pilot proxy with group %s to %s..." % ( params.getDIRACGroup(), myProxy.getMyProxyServer() ) )
  else:
    params.debugMsg(  "Uploading user proxy with group %s to %s..." % ( params.getDIRACGroup(), myProxy.getMyProxyServer() ) )
  retVal =  myProxy.getInfo( proxyInfo[ 'path' ], useDNAsUserName = DNAsUsername )
  if retVal[ 'OK' ]:
    remainingSecs = ( int( params.getProxyRemainingSecs() / 3600 ) * 3600 ) - 7200
    myProxyInfo = retVal[ 'Value' ]
    if 'timeLeft' in myProxyInfo and remainingSecs < myProxyInfo[ 'timeLeft' ]:
      params.debugMsg(  " Already uploaded" )
      return True
  retVal = generateProxy( params )
  if not retVal[ 'OK' ]:
    print " There was a problem generating proxy to be uploaded to myproxy: %s" % retVal[ 'Message' ]
    return False
  retVal = getProxyInfo( retVal[ 'Value' ] )
  if not retVal[ 'OK' ]:
    print " There was a problem generating proxy to be uploaded to myproxy: %s" % retVal[ 'Message' ]
    return False
  generatedProxyInfo = retVal[ 'Value' ]
  retVal = myProxy.uploadProxy( generatedProxyInfo[ 'path' ], useDNAsUserName = DNAsUsername )
  if not retVal[ 'OK' ]:
    print " Can't upload to myproxy: %s" % retVal[ 'Message' ]
    return False
  params.debugMsg( " Uploaded" )
  return True

def uploadProxyToDIRACProxyManager( params ):
  params.debugMsg(  "Uploading user pilot proxy with group %s..." % ( params.getDIRACGroup() ) )
  params.onTheFly = True
  retVal = uploadProxy( params )
  if not retVal[ 'OK' ]:
    print " There was a problem generating proxy to be uploaded proxy manager: %s" % retVal[ 'Message' ]
    return False
  return True

Script.enableCS()

retVal = getProxyInfo( retVal[ 'Value' ] )
if not retVal[ 'OK' ]:
  print "Can't create a proxy: %s" % retVal[ 'Message' ]
  sys.exit(1)

proxyInfo = retVal[ 'Value' ]
if 'username' not in proxyInfo:
  print "Not authorized in DIRAC"
  sys.exit(1)

retVal = CS.getGroupsForUser( proxyInfo[ 'username' ] )
if not retVal[ 'OK' ]:
  print "No groups defined for user %s" % proxyInfo[ 'username' ]
  sys.exit(1)
availableGroups = retVal[ 'Value' ]

pilotGroup = False
for group in availableGroups:
  groupProps = CS.getPropertiesForGroup( group )
  if Properties.PILOT in groupProps or Properties.GENERIC_PILOT in groupProps:
    pilotGroup = group
    break
  
issuerCert = proxyInfo[ 'chain' ].getIssuerCert()[ 'Value' ]
remainingSecs = issuerCert.getRemainingSecs()[ 'Value' ]
cliParams.setProxyRemainingSecs( remainingSecs - 300 )  

if not pilotGroup:
  print "WARN: No pilot group defined for user %s" % proxyInfo[ 'username' ]
  if cliParams.strict:
    sys.exit(1)
else:
  cliParams.setDIRACGroup( pilotGroup )
  #uploadProxyToMyProxy( cliParams, True )
  success = uploadProxyToDIRACProxyManager( cliParams )
  if not success and cliParams.strict:
    sys.exit(1)

cliParams.setDIRACGroup( proxyInfo[ 'group' ] )
#uploadProxyToMyProxy( cliParams, False )
success = uploadProxyToDIRACProxyManager( cliParams )
if not success and cliParams.strict:
  sys.exit(1)

finalChain = proxyInfo[ 'chain' ]

vomsMapping = CS.getVOMSAttributeForGroup( proxyInfo[ 'group' ] )
if vomsMapping:
  voms = VOMS()
  retVal = voms.setVOMSAttributes( finalChain, vomsMapping )
  if not retVal[ 'OK' ]:
    #print "Cannot add voms attribute %s to proxy %s: %s" % ( attr, proxyInfo[ 'path' ], retVal[ 'Message' ] )
    print "Warning : Cannot add voms attribute %s to proxy" % ( vomsMapping )
    print "          Accessing data in the grid storage from the user interface will not be possible."
    print "          The grid jobs will not be affected."
    if cliParams.strict:
      sys.exit(1)
  else:
    finalChain = retVal[ 'Value' ]

retVal = finalChain.dumpAllToFile( proxyInfo[ 'path' ] )
if not retVal[ 'OK' ]:
  print "Cannot write proxy to file %s" % proxyInfo[ 'path' ]
  sys.exit(1)
cliParams.debugMsg(  "done" )
sys.exit(0)








