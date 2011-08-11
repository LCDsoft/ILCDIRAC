'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

__RCSID__ = "$Id: "

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from DIRAC.Resources.Catalog.Client.FileCatalogClient import FileCatalogClient
from DIRAC import S_OK, S_ERROR, gConfig


class ProductionJob(Job):
  def __init__(self, script = None):
    Job.__init__(self , script)
    self.prodVersion = __RCSID__
    self.csSection = '/Operations/Production/Defaults'
    self.fc = FileCatalogClient()
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection), 'x86_64-slc5-gcc43-opt')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'

    self.prodTypes = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction', 'MCReconstruction_Overlay']
    
    is_prod = "IS_PROD"
    self._addParameter(self.workflow, is_prod, 'JDL', True, "This job is a production job")
    if not script:
      self.__setDefaults()
      
  #############################################################################
  def __setDefaults(self):
    """Sets some default parameters.
    """
    self.setSystemConfig(self.systemConfig)
    self.setCPUTime('300000')
    self.setLogLevel('verbose')
    self.setJobGroup('@{PRODUCTION_ID}')
    self.setFileMask('')

    #version control
    self._setParameter('productionVersion', 'string', self.prodVersion, 'ProdAPIVersion')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID',     'string', self.defaultProdID.zfill(8), 'ProductionID')
    self._setParameter('JOB_ID',            'string', self.defaultProdJobID.zfill(8), 'ProductionJobID')
    self._setParameter('Priority',             'JDL',                     '1', 'Priority')
    self._setParameter('emailAddress',      'string', 'stephane.poss@cern.ch', 'CrashEmailAddress')

  def _setParameter(self, name, parameterType, parameterValue, description):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    if gConfig.getValue('%s/%s' % (self.csSection, name), ''):
      self.log.debug('Setting %s from CS defaults = %s' % (name, gConfig.getValue('%s/%s' % (self.csSection, name))))
      self._addParameter(self.workflow, name, parameterType, gConfig.getValue('%s/%s' % (self.csSection, name), 'default'), description)
    else:
      self.log.debug('Setting parameter %s = %s' % (name, parameterValue))
      self._addParameter(self.workflow, name, parameterType, parameterValue, description)
     
    
  def setInputDataQuery(self,metadict):
    """ Define the input data query needed
    """
    res = self.fc.findFilesByMetadata(metadict)
    if not res['OK']:
      return res
    """ Also get the compatible metadata such as energy, evttype, etc, populate dictionary
    """
    return S_OK()
  
  def createProduction(self):
    """ Create production.
    """
    return S_OK()
  
  def finalizeProd(self):
    """ Finalize definition: submit to Transformation service
    """
    return S_OK()  
  
  def _jobSpecificParams(self,application):
    """ For production additional checks are needed: ask the user
    """
    if not application.logfile:
      logf = application.appname+"_"+application.version+"_Step_"+str(self.stepnumber)+".log"
      application.setLogFile(logf)
      #in fact a bit more tricky as the log files have the prodID and jobID in them
    return S_OK()

  def _jobSpecificModules(self,application,step):
    return application._prodjobmodules(step)
  