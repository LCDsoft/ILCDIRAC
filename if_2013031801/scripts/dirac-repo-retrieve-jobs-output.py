'''
Retrieve the output sandboxes of jobs created using the API, stored in the repository file

Created on Mar 24, 2010

@author: sposs
'''
from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, exit as dexit

class Params(object):
  def __init__(self):
    self.outputdata = False
    self.repo = ''
    
  def setOuputData(self, opt):
    self.outputdata = True
    return S_OK()
  
  def setRepo(self, opt):
    self.repo = opt
    return S_OK()
  
  def registerSwitches(self):
    Script.registerSwitch( 'O', 'Outputdata', 'retrieve also the outputdata', self.setOuputData)
    Script.registerSwitch( 'r:', 'Repository=', 'repository file to use', self.setRepo)
    Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] )  )
    
if __name__=="__main__":
  repoLocation = ''
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  repoLocation = clip.repo
  if not repoLocation:
    Script.showHelp()
    dexit(1)
  from DIRAC import gLogger
  from DIRAC.Interfaces.API.Dirac import Dirac

  dirac = Dirac(True, repoLocation)
  
  exitCode = 0
  res = dirac.monitorRepository(False)
  if not res['OK']:
    gLogger.error("Failed because %s" % res['Message'])
    dexit(1)
    
  res = dirac.retrieveRepositorySandboxes()
  if not res['OK']:
    gLogger.error("Failed because %s" % res['Message'])
    dexit(1)
  if clip.outputdata:
    res = dirac.retrieveRepositoryData()
    if not res['OK']:
      gLogger.error("Failed because %s" % res['Message'])
      exit(1)
  dexit(exitCode)