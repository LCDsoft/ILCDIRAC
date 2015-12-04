"""
Checks and potentially provides production proxy, called from :mod:`~ILCDIRAC.Interfaces.API.NewInterface.ProductionJob`

:since: Feb 10, 2011
:author: sposs
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from subprocess import call
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

def getNewProxy(): 
  """ Get a new production proxy

  :returns: statuscode of the dirac-proxy-init call: 0 success, otherwise error!
  """
  print 'Getting production proxy ...'
  return call( [ 'dirac-proxy-init', '-g', 'ilc_prod' ] )
def checkAndGetProdProxy():
  """ Check if current proxy is a production one, and if not call the :any:`getNewProxy` method.

  :returns: :func:`S_OK() <DIRAC:DIRAC.Core.Utilities.ReturnValues.S_OK>`, :func:`~DIRAC:DIRAC.Core.Utilities.ReturnValues.S_ERROR`
  """
  result = getProxyInfo()

  if result['OK'] and 'group' in result['Value']:
    if result['Value']['group'] == "ilc_prod":
      return S_OK()
    else:
      print "You don't have an ilc_prod proxy, trying to get one..."
  else:
    print "Error to get proxy information, trying to get proxy"

  newProxyRetVal = getNewProxy()
  if newProxyRetVal: ## != 0
    return S_ERROR("dirac-proxy-init failed")

  result = getProxyInfo()
  if result['OK'] and 'group' in result['Value']:
    if result['Value']['group'] == "ilc_prod":
      return S_OK()
    else:
      print 'You do not have a valid group'
      return S_ERROR( "Could not obtain valid group" )
  elif result['OK'] and 'group' not in result['Value']:
    return S_ERROR( "Could not obtain group information from proxy" )

  print 'Could not obtain proxy information: %s' % result['Message']
  return result
