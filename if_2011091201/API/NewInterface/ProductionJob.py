'''
Production Job class. Used to define new productions. 

Mostly similar to L{UserJob}, but cannot be (and should not be) used like the UserJob class.

@author: Stephane Poss
@author: Remi Ete
@author: Ching Bon Lam
'''

__RCSID__ = "$Id: "
from DIRAC.Core.Workflow.Module                             import ModuleDefinition
from DIRAC.Core.Workflow.Step                               import StepDefinition
from ILCDIRAC.Interfaces.API.NewInterface.Job               import Job
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Client.Transformation       import Transformation

from DIRAC.Resources.Catalog.FileCatalogClient              import FileCatalogClient
from DIRAC.Core.Security.Misc                               import getProxyInfo

from math                                                   import modf

from DIRAC                                                  import S_OK, S_ERROR, gConfig

import string, os, shutil


class ProductionJob(Job):
  def __init__(self, script = None):
    Job.__init__(self , script)
    self.prodVersion = __RCSID__
    self.created = False
    self.type = 'Production'
    self.csSection = '/Operations/Production/Defaults'
    self.fc = FileCatalogClient()
    self.trc = TransformationClient()
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection), 'x86_64-slc5-gcc43-opt')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.basename = ''
    self.basepath = "/ilc/prod/"
    self.evttype = ''
    self.machine = 'clic'

    self.description = ''

    self.outputStorage = ''

    self.proxyinfo = getProxyInfo()

    self.inputdataquery = False
    self.plugin = 'Standard'

    self.prodTypes = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction', 'MCReconstruction_Overlay']
    self.prodparameters = {}
    self._addParameter(self.workflow, "IS_PROD", 'JDL', True, "This job is a production job")
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
      
  #############################################################################
  def setProdGroup(self,group):
    """ Sets a user defined tag for the production as appears on the monitoring page
    """
    self.prodGroup = group
  #############################################################################
  def setProdPlugin(self,plugin):
    """ Sets the plugin to be used to creating the production jobs
    """
    self.plugin = plugin
    
  #############################################################################
  def setJobFileGroupSize(self,files):
    """ Sets the number of files to be input to each job created.
    """
    self.jobFileGroupSize = files
    self.prodparameters['NbInputFiles'] = files
  #############################################################################
  def setProdType(self,prodType):
    """Set prod type.
    """
    if not prodType in self.prodTypes:
      raise TypeError,'Prod must be one of %s' %(string.join(self.prodTypes,', '))
    self.setType(prodType)
  #############################################################################
  def setWorkflowName(self,name):
    """Set workflow name.
    """
    self.workflow.setName(name)
    self.name = name

  #############################################################################
  def setWorkflowDescription(self,desc):
    """Set workflow name.
    """
    self.workflow.setDescription(desc)
             
  #############################################################################
  def createWorkflow(self):
    """ Create XML for local testing.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' %name)
    self.workflow.toXMLFile(name)
    
  #############################################################################
  def setOutputSE(self,outputse):
    """ Define where the output file(s) will go. 
    """
    self.outputStorage = outputse
    return S_OK()
  
  #############################################################################
  def setInputDataQuery(self,metadict):
    """ Define the input data query needed
    """
    res = self.fc.findFilesByMetadata(metadict)
    if not res['OK']:
      return res
    lfns = res['Value']
    if not len(lfns):
      return S_ERROR("No files found")
    """ Also get the compatible metadata such as energy, evttype, etc, populate dictionary
    Beware of energy: need to convert to gev (3tev -> 3000, 500gev -> 500)
    """
    
    self.inputdataquery = True
    return S_OK()

  def setMachine(self,machine):
    self.machine = machine

  def setDescription(self,desc):
    """ Set the production's description
    
    @param desc: Description
    """
    self.description = desc
    return S_OK()

  def getBasePath(self):
    """ Return the base path. Updated by L{setInputDataQuery}.
    """
    return self.basepath
  
  def addFinalization(self, uploadData=False, registerData=False, uploadLog = False, sendFailover=False):
    """ Add finalization step

    @param uploadData: Upload or not the data to the storage
    @param uploadLog: Upload log file to storage (currently only available for admins, thus add them to OutputSandbox)
    @param sendFailover: Send Failover requests, and declare files as processed or unused in transfDB
    @param registerData: Register data in the file catalog
    @todo: Do the registration only once, instead of once for each job

    """
    self.importLine = 'from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>'
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'Enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest,'Enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('RegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata,'Enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','RegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload,'Enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadLogFile')
    logUpload.setBody(body)

    finalization = StepDefinition('Job_Finalization')
    finalization.addModule(dataUpload)
    up = finalization.createModuleInstance('UploadOutputData','dataUpload')
    up.setValue("Enable",uploadData)

    finalization.addModule(registerdata)
    ro = finalization.createModuleInstance('RegisterOutputData','RegisterOutputData')
    ro.setValue("Enable",registerData)

    finalization.addModule(logUpload)
    ul  = finalization.createModuleInstance('UploadLogFile','logUpload')
    ul.setValue("Enable",uploadLog)

    finalization.addModule(failoverRequest)
    fr = finalization.createModuleInstance('FailoverRequest','failoverRequest')
    fr.setValue("Enable",sendFailover)
    
    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')

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

    if self.created:
      return S_ERROR("Production already created.")

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
    Trans.setDescription(self.description)
    Trans.setLongDescription(self.description)
    Trans.setType(self.type)
    self.prodparameters['JobType']=self.type
    Trans.setPlugin(self.plugin)
    if self.inputdataquery:
      Trans.setGroupSize(self.jobFileGroupSize)
    Trans.setTransformationGroup(self.prodGroup)
    Trans.setBody(workflowXML)
    Trans.setEventsPerTask(self.nbevts)
    res = Trans.addTransformation()
    if not res['OK']:
      print res['Message']
      return res
    self.currtrans = Trans
    self.currtrans.setStatus("Active")
    self.currtrans.setAgentType("Automatic")
    
    self.created = True
    return S_OK()

  def setNbOfTasks(self,nbtasks):
    """ Define the number of tasks you want. Useful for generation jobs.
    """
    if not self.currtrans:
      self.log.error("Not transformation defined earlier")
      return S_ERROR("No transformation defined")
    if self.inputBKSelection:
      self.log.error("Meta data selection activated, should not specify the number of jobs")
      return S_ERROR()
    self.nbtasks = nbtasks
    self.currtrans.setMaxNumberOfTasks(self.nbtasks)
    return S_OK()
  
  def finalizeProd(self):
    """ Finalize definition: submit to Transformation service
    """
    return S_OK()  
  
  def _jobSpecificParams(self,application):
    """ For production additional checks are needed: ask the user
    """

    if self.created:
      return S_ERROR("The production was created, you cannot add new applications to the job.")

    if not application.logfile:
      logf = application.appname+"_"+application.version+"_Step_"+str(self.stepnumber)+".log"
      res = application.setLogFile(logf)
      if not res['OK']:
        return res
      
      #in fact a bit more tricky as the log files have the prodID and jobID in them
    
    ### Retrieve from the application the essential info to build the prod info.
    if not self.nbevts:
      self.nbevts = application.nbevts
      if not self.nbevts:
        return S_ERROR("Number of events to process is not defined.")
    elif not application.nbevts:
      res = application.setNbEvts(self.nbevts)
      if not res['OK']:
        return res
      
    if application.nbevts > 0 and self.nbevts > application.nbevts:
      self.nbevts = application.nbevts
    
    if not self.energy:
      if application.energy:
        self.energy = application.energy
      else:
        return S_ERROR("Could not find the energy defined, it is needed for the production definition.")
    elif not application.energy:
      res = application.setEnergy(self.energy)
      if not res['OK']:
        return res
    if self.energy:
      self._setParameter( "Energy", "int", int(self.energy), "Energy used")      
      
    if not self.evttype:
      if hasattr(application,'evttype'):
        self.evttype = application.evttype
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      
    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
    
    res = application.setOutputSE(self.outputStorage)
    if not res['OK']:
      return res
    
    energypath = ''
    fracappen = modf(self.energy/1000.)
    if fracappen[1]>0:
      energypath = "%s"%int(fracappen[1])
      if fracappen[0]>0:
        energypath =  "%s"%(self.energy/1000.)
      energypath += 'tev/'  
    else:
      energypath =  "%sgev/"%(self.energy/1000.)

    if not self.basename:
      self.basename = self.evttype
    
    if not self.machine[-1]=='/':
      self.machine += "/"
    if not self.evttype[-1]=='/':
      self.evttype += '/'  
    
      
    ###Need to resolve file names and paths
    if hasattr(application,"setOutputRecFile"):
      path = self.basepath+self.machine+energypath+self.evttype+application.detectortype+"/REC/"
      fname = self.basename+"_rec.slcio"
      application.setOutputRecFile(fname,path)  
      path = self.basepath+self.machine+energypath+self.evttype+application.detectortype+"/DST/"
      fname = self.basename+"_dst.slcio"
      application.setOutputDstFile(fname,path)  
    elif hasattr(application,"outputFile") and hasattr(application,'datatype') and not application.outputFile:
      path = self.basepath+self.machine+energypath+self.evttype
      if hasattr(application,"detectortype"):
        if application.detectortype:
          path += application.detectortype+"/"
      path += application.datatype+"/"
      self.log.info("Will store the files under %s"%path)
      extension = 'stdhep'
      if application.datatype=='SIM':
        extension = 'slcio'
      fname = self.basename+"_%s"%(application.datatype.lower())+"."+extension
      application.setOutputFile(fname,path)  
      
    return S_OK()

  def _jobSpecificModules(self,application,step):
    return application._prodjobmodules(step)
  