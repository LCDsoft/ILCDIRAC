'''
Created on Feb 10, 2011

@author: sposs
'''
from DIRAC import S_OK,S_ERROR
from subprocess import call
from DIRAC.Core.Security.Misc import getProxyInfo

def getNewProxy(): 
  print 'Getting production proxy ...'
  call( [ 'proxy-init' , '-g', 'ilc_prod' ] )

def CheckAndGetProdProxy():
  result = getProxyInfo()
  if not result['OK']:
    print 'Could not obtain proxy information: %s'%result['Message']
    getNewProxy()
  elif not result['Value'].has_key('group'):
    print 'Could not get group from proxy'
    getNewProxy()
  group = result['Value']['group']

  if not group=='ilc_prod':
    print 'You do not have a valid group'
    getNewProxy()  
  return S_OK()