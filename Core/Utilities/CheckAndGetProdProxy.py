'''
Checks and potentially provides production proxy, called from L{ProductionJob}

Created on Feb 10, 2011

@author: sposs
'''

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from subprocess import call
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

def getNewProxy(): 
  """ Get a new production proxy
  """
  print 'Getting production proxy ...'
  return call( [ 'dirac-proxy-init', '-g', 'ilc_prod' ] )
def checkAndGetProdProxy():
  """ Check if current proxy is a production one, and if not call the L{getNewProxy} method.
  """
  result = getProxyInfo()
  if not result['OK']:
    print 'Could not obtain proxy information: %s' % result['Message']
    if getNewProxy():
      return S_ERROR("dirac-proxy-init failed")
  elif not result['Value'].has_key('group'):
    print 'Could not get group from proxy'
    getNewProxy()
    if getNewProxy():
      return S_ERROR("dirac-proxy-init failed")
  group = result['Value']['group']

  if not group == 'ilc_prod':
    print 'You do not have a valid group'
    getNewProxy()  
    if getNewProxy():
      return S_ERROR("dirac-proxy-init failed")
  return S_OK()
