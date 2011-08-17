'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

__RCSID__ = "$Id: "

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Client.Transformation import Transformation

from DIRAC.Resources.Catalog.Client.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Security.Misc import getProxyInfo

from DIRAC import S_OK, S_ERROR, gConfig


class ProductionJob(Job):
  def __init__(self, script = None):
    Job.__init__(self , script)
    self.prodVersion = __RCSID__
    self.csSection = '/Operations/Production/Defaults'
    self.fc = FileCatalogClient()
    self.trc = TransformationClient()
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection), 'x86_64-slc5-gcc43-opt')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'

    self.proxyinfo = getProxyInfo()

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
  
  def createProduction(self,name = None):
    """ Create production.
    """
    
    if not self.proxyinfo['OK']:
      return S_ERROR("Not allowed to create production, you need a ilc_prod proxy.")
    if self.proxyinfo['Value'].has_key('group'):
      group = self.proxyinfo['Value']['group']
      if not group=="ilc_prod":
        return S_ERROR("Not allowed to create production, you need a ilc_prod proxy.")
    else:
      return S_ERROR("Could not determine group, you do not have the right proxy.")

    workflowName = self.workflow.getName()
    fileName = '%s.xml' %workflowName
    self.log.verbose('Workflow XML file name is: %s' %fileName)
    try:
      self.createWorkflow()
    except Exception,x:
      self.log.error(x)
      return S_ERROR('Could not create workflow')
    oFile = open(fileName,'r')
    workflowXML = oFile.read()
    oFile.close()
    if not name:
      name = workflowName

    res = self.trc.getTransformationStats(name)
    if res['OK']:
      return self._reportError("Transformation with name %s already exists! Cannot proceed."%name)
    
    ###Create Tranformation
    Trans = Transformation()
    Trans.setTransformationName(name)
    Trans.setDescription(self.workflow.getDescrShort())
    Trans.setLongDescription(self.workflow.getDescription())
    Trans.setType(self.type)
    self.prodparameters['JobType']=self.type
    Trans.setPlugin('Standard')
    Trans.setGroupSize(self.jobFileGroupSize)
    Trans.setTransformationGroup(self.prodGroup)
    Trans.setBody(workflowXML)
    Trans.setEventsPerTask(self.prodparameters['nbevts']*self.prodparameters['NbInputFiles'])
    res = Trans.addTransformation()
    if not res['OK']:
      print res['Message']
      return res
    self.currtrans = Trans
    self.currtrans.setStatus("Active")
    self.currtrans.setAgentType("Automatic")
    
    
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
      res = application.setLogFile(logf)
      if not res['OK']:
        return res
      
      #in fact a bit more tricky as the log files have the prodID and jobID in them
    
    ### Retrieve from the application the essential info to build the prod info.
    if not self.nbevts:
      self.nbevts = application.nbevts
    elif not application.nbevts:
      res = application.setNbEvts(self.nbevts)
      if not res['OK']:
        return res
      
    if application.nbevts > 0 and self.nbevts > application.nbevts:
      self.nbevts = application.nbevts
    
    if not self.energy:
      self.energy = application.energy
      if not self.energy:
        return S_ERROR("Could not find the energy defined, it is needed for the production definition.")
    elif not application.energy:
      res = application.setEnergy(self.energy)
      if not res['OK']:
        return res
      
    return S_OK()

  def _jobSpecificModules(self,application,step):
    return application._prodjobmodules(step)
  