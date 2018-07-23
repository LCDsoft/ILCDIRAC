"""
Retrieve the output sandboxes of jobs created using the API, stored in the repository file

Options:
  -r repoLocation             Path to repository file
  -O, --Outputdata            retrieve also the outputdata

:since: Mar 24, 2010
:author: sposs
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import S_OK, exit as dexit
from DIRAC import gLogger

LOG = gLogger.getSubLogger(__name__)

class _Params(object):
  def __init__(self):
    self.outputdata = False
    self.repo = ''
    
  def setOuputData(self, dummy_opt):
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
    
def _getOutputs():
  repoLocation = ''
  clip = _Params()
  clip.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  repoLocation = clip.repo
  if not repoLocation:
    Script.showHelp()
    dexit(1)
  from DIRAC.Interfaces.API.Dirac import Dirac

  dirac = Dirac(True, repoLocation)
  
  exitCode = 0
  res = dirac.monitorRepository(False)
  if not res['OK']:
    LOG.error("Failed because %s" % res['Message'])
    dexit(1)
    
  res = dirac.retrieveRepositorySandboxes()
  if not res['OK']:
    LOG.error("Failed because %s" % res['Message'])
    dexit(1)
  if clip.outputdata:
    res = dirac.retrieveRepositoryData()
    if not res['OK']:
      LOG.error("Failed because %s" % res['Message'])
      exit(1)
  dexit(exitCode)

if __name__=="__main__":
  _getOutputs()
