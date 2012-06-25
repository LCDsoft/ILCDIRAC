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
from DIRAC.Core.DISET.RPCClient                             import RPCClient

from DIRAC.Resources.Catalog.FileCatalogClient              import FileCatalogClient
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo

from math                                                   import modf

from DIRAC                                                  import S_OK, S_ERROR, gConfig

import string, os, shutil, types
from decimal import *


class ProductionJob(Job):
  """ Production job class. Suitable for CLIC studies. Need to sub class and overload for other clients.
  """
  def __init__(self, script = None):
    Job.__init__(self , script)
    self.prodVersion = __RCSID__
    self.created = False
    self.checked = False
    self.call_finalization = False
    self.finalsdict = {}
    self.transfid = 0
    self.type = 'Production'
    self.csSection = '/Operations/Production/Defaults'
    self.fc = FileCatalogClient()
    self.trc = TransformationClient()
    self.systemConfig = gConfig.getValue('%s/SystemConfig' % (self.csSection), 'x86_64-slc5-gcc43-opt')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.jobFileGroupSize = 1
    self.nbtasks = 1
    self.basename = ''
    self.basepath = "/ilc/prod/"
    self.evttype = ''
    self.datatype = ''
    self.machine = 'clic'
    self.detector = ''
    self.currtrans = None
    self.description = ''

    self.finalpaths = []
    self.finalMetaDict = {}
    self.finalMetaDictNonSearch = {}

    self.outputStorage = ''

    self.proxyinfo = getProxyInfo()

    self.inputdataquery = False
    self.inputBKSelection = {}
    self.plugin = 'Standard'

    self.prodTypes = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction', 
                      'MCReconstruction_Overlay', 'Merge', 'Split']
    self.prodparameters = {}
    self.prodparameters['NbInputFiles'] = 1
    self.prodparameters['nbevts']  = 0 
    self.prodparameters["SWPackages"] = ''
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
  def setProdGroup(self, group):
    """ Sets a user defined tag for the production as appears on the monitoring page
    """
    self.prodGroup = group
  #############################################################################
  def setProdPlugin(self, plugin):
    """ Sets the plugin to be used to creating the production jobs
    """
    self.plugin = plugin
    
  #############################################################################
  def setJobFileGroupSize(self, files):
    """ Sets the number of files to be input to each job created.
    """
    if self.checked:
      return self._reportError("This input is needed at the beginning of the production definition: it's needed for total number of evts.")
    self.jobFileGroupSize = files
    self.prodparameters['NbInputFiles'] = files
    
  #############################################################################
  def setProdType(self, prodType):
    """Set prod type.
    """
    if not prodType in self.prodTypes:
      raise TypeError,'Prod must be one of %s' % (string.join(self.prodTypes,', '))
    self.setType(prodType)
  #############################################################################
  def setWorkflowName(self, name):
    """Set workflow name.
    """
    self.workflow.setName(name)
    self.name = name

  #############################################################################
  def setWorkflowDescription(self, desc):
    """Set workflow name.
    """
    self.workflow.setDescription(desc)
             
  #############################################################################
  def createWorkflow(self):
    """ Create XML for local testing.
    """
    name = '%s.xml' % self.name
    if os.path.exists(name):
      shutil.move(name,'%s.backup' % name)
    self.workflow.toXMLFile(name)
    
  #############################################################################
  def setOutputSE(self, outputse):
    """ Define where the output file(s) will go. 
    """
    self.outputStorage = outputse
    return S_OK()
  
  #############################################################################
  def setInputDataQuery(self, metadata):
    """ Define the input data query needed
    """
    res = self.fc.findDirectoriesByMetadata(metadata)
    if not res['OK']:
      return res
    dirs = res['Value']
    if not len(dirs):
      return S_ERROR("No directories found")

    metakeys = metadata.keys()
    res = self.fc.getMetadataFields()
    if not res['OK']:
      print "Could not contact File Catalog"
      return S_ERROR("Could not contact File Catalog")
    metaFCkeys = res['Value'].keys()
    for key in metakeys:
      for meta in metaFCkeys:
        if meta != key:
          if meta.lower() == key.lower():
            return self._reportError("Key syntax error %s, should be %s" % (key, meta))
      if not metaFCkeys.count(key):
        return self._reportError("Key %s not found in metadata keys, allowed are %s" % (key, metaFCkeys))

    if not   metadata.has_key("ProdID"):
      return self._reportError("Input metadata dictionary must contain at least a key 'ProdID' as reference")
    
    res = self.fc.findDirectoriesByMetadata(metadata)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for available directories")
    elif len(res['Value']) < 1:
      return self._reportError('Could not find any directories corresponding to the query issued')
    dirs = res['Value'].values()
    for mdir in dirs:
      res = self.fc.getDirectoryMetadata(mdir)
      if not res['OK']:
        return self._reportError("Error looking up the catalog for directory metadata")
    #res =   client.getCompatibleMetadata(metadata)
    #if not res['OK']:
    #  return self._reportError("Error looking up the catalog for compatible metadata")
      compatmeta = res['Value']
      compatmeta.update(metadata)
    if compatmeta.has_key('EvtType'):
      if type(compatmeta['EvtType']) in types.StringTypes:
        self.evttype  = compatmeta['EvtType']
      if type(compatmeta['EvtType']) == type([]):
        self.evttype = compatmeta['EvtType'][0]
    else:
      return self._reportError("EvtType is not in the metadata, it has to be!")
    if compatmeta.has_key('NumberOfEvents'):
      if type(compatmeta['NumberOfEvents']) == type([]):
        self.nbevts = int(compatmeta['NumberOfEvents'][0])
      else:
        #type(compatmeta['NumberOfEvents']) in types.StringTypes:
        self.nbevts = int(compatmeta['NumberOfEvents'])
      #else:
      #  return self._reportError('Nb of events does not have any type recognised')

    self.basename = self.evttype
    self.basepath = "/ilc/prod/"
    if compatmeta.has_key("Machine"):
      if type(compatmeta["Machine"]) in types.StringTypes:
        self.machine = compatmeta["Machine"] + "/"
      if type(compatmeta["Machine"]) == type([]):
        self.machine = compatmeta["Machine"][0] + "/"
    if compatmeta.has_key("Energy"):
      if type(compatmeta["Energy"]) in types.StringTypes:
        self.energycat = compatmeta["Energy"]
      if type(compatmeta["Energy"]) == type([]):
        self.energycat = compatmeta["Energy"][0]
        
    if self.energycat.count("tev"):
      self.energy = Decimal("1000.") * Decimal(self.energycat.split("tev")[0])
    elif self.energycat.count("gev"):
      self.energy = Decimal("1.") * Decimal(self.energycat.split("gev")[0])
    else:
      self.energy = Decimal("1.") * Decimal(self.energycat)
    gendata = False
    if compatmeta.has_key('Datatype'):
      if type(compatmeta['Datatype']) in types.StringTypes:
        self.datatype = compatmeta['Datatype']
        if compatmeta['Datatype'] == 'gen':
          gendata = True
      if type(compatmeta['Datatype']) == type([]):
        self.datatype = compatmeta['Datatype'][0]
        if compatmeta['Datatype'][0] == 'gen':
          gendata = True
    if compatmeta.has_key("DetectorType") and not gendata:
      if type(compatmeta["DetectorType"]) in types.StringTypes:
        self.detector = compatmeta["DetectorType"]
      if type(compatmeta["DetectorType"]) == type([]):
        self.detector = compatmeta["DetectorType"][0]    
    self.inputBKSelection = metadata
    self.inputdataquery = True
    
    self.prodparameters['nbevts'] = self.nbevts 
    self.prodparameters["FCInputQuery"] = self.inputBKSelection

    return S_OK()

  def setMachine(self, machine):
    """ Define the machine type: clic or ilc
    """
    self.machine = machine

  def setDescription(self, desc):
    """ Set the production's description
    
    @param desc: Description
    """
    self.description = desc
    return S_OK()

  def getBasePath(self):
    """ Return the base path. Updated by L{setInputDataQuery}.
    """
    return self.basepath
  
  def addFinalization(self, uploadData = False, registerData = False, uploadLog = False, sendFailover=False):
    """ Add finalization step

    @param uploadData: Upload or not the data to the storage
    @param uploadLog: Upload log file to storage (currently only available for admins, thus add them to OutputSandbox)
    @param sendFailover: Send Failover requests, and declare files as processed or unused in transfDB
    @param registerData: Register data in the file catalog
    @todo: Do the registration only once, instead of once for each job

    """
    
    self.call_finalization = True
    self.finalsdict['uploadData'] = uploadData
    self.finalsdict['registerData'] = registerData
    self.finalsdict['uploadLog'] = uploadLog
    self.finalsdict['sendFailover'] = sendFailover

  def _addRealFinalization(self):  
    """ This is called at creation: now that the workflow is created at the last minute,
    we need to add this also at the last minute
    """
    self.importLine = 'from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>'
    
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(self.importLine, '<MODULE>', 'UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(self.importLine, '<MODULE>', 'FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('RegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(self.importLine, '<MODULE>', 'RegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(self.importLine, '<MODULE>', 'UploadLogFile')
    logUpload.setBody(body)

    finalization = StepDefinition('Job_Finalization')
    finalization.addModule(dataUpload)
    up = finalization.createModuleInstance('UploadOutputData', 'dataUpload')
    up.setValue("enable", self.finalsdict['uploadData'])

    finalization.addModule(registerdata)
    ro = finalization.createModuleInstance('RegisterOutputData', 'RegisterOutputData')
    ro.setValue("enable", self.finalsdict['registerData'])

    finalization.addModule(logUpload)
    ul  = finalization.createModuleInstance('UploadLogFile', 'logUpload')
    ul.setValue("enable", self.finalsdict['uploadLog'])

    finalization.addModule(failoverRequest)
    fr = finalization.createModuleInstance('FailoverRequest', 'failoverRequest')
    fr.setValue("enable", self.finalsdict['sendFailover'])
    
    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')

    return S_OK()
  
  def createProduction(self, name = None):
    """ Create production.
    """
    
    if not self.proxyinfo['OK']:
      return S_ERROR("Not allowed to create production, you need a ilc_prod proxy.")
    if self.proxyinfo['Value'].has_key('group'):
      group = self.proxyinfo['Value']['group']
      if not group == "ilc_prod":
        return S_ERROR("Not allowed to create production, you need a ilc_prod proxy.")
    else:
      return S_ERROR("Could not determine group, you do not have the right proxy.")

    if self.created:
      return S_ERROR("Production already created.")

    ###We need to add the applications to the workflow
    res = self._addToWorkflow()
    if not res['OK']:
      return res
    if self.call_finalization:
      self._addRealFinalization()
    
    workflowName = self.workflow.getName()
    fileName = '%s.xml' % workflowName
    self.log.verbose('Workflow XML file name is: %s' % fileName)
    try:
      self.createWorkflow()
    except Exception, x:
      self.log.error(x)
      return S_ERROR('Could not create workflow')
    oFile = open(fileName, 'r')
    workflowXML = oFile.read()
    oFile.close()
    if not name:
      name = workflowName

    res = self.trc.getTransformationStats(name)
    if res['OK']:
      return self._reportError("Transformation with name %s already exists! Cannot proceed." % name)
    
    ###Create Tranformation
    Trans = Transformation()
    Trans.setTransformationName(name)
    Trans.setDescription(self.description)
    Trans.setLongDescription(self.description)
    Trans.setType(self.type)
    self.prodparameters['JobType'] = self.type
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
    self.transfid = Trans.getTransformationID()['Value']

    if self.inputBKSelection:
      res = self.applyInputDataQuery()
    Trans.setAgentType("Automatic")  
    Trans.setStatus("Active")
    
    finals = []
    for finalpaths in self.finalpaths:
      finalpaths = finalpaths.rstrip("/")
      finalpaths += "/"+str(self.transfid).zfill(8)
      finals.append(finalpaths)
      self.finalMetaDict[finalpaths] = {'NumberOfEvents' : self.nbevts, "ProdID" : self.transfid}
    self.finalpaths = finals
    self.created = True
    return S_OK()

  def setNbOfTasks(self, nbtasks):
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

  def applyInputDataQuery(self, metadata = None, prodid = None):
    """ Tell the production to update itself using the metadata query specified, i.e. submit new jobs if new files are added corresponding to same query.
    """
    if not self.transfid and self.currtrans:
      self.transfid = self.currtrans.getTransformationID()['Value']
    elif prodid:
      self.transfid = prodid
    if not self.transfid:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if metadata:
      self.inputBKSelection = metadata

    client = TransformationClient()
    res = client.createTransformationInputDataQuery(self.transfid, self.inputBKSelection)
    if not res['OK']:
      return res
    return S_OK()
  
  def addMetadataToFinalFiles(self, metadict):
    """ Add additionnal non-query metadata 
    """
    for key in metadict.keys():
      if key in self.finalMetaDictNonSearch[self.basepath].keys():
        self.log.error("Not allowed to overwrite existing metadata: ", key)
    self.finalMetaDictNonSearch[self.basepath].update(metadict)
    return S_OK()
  
  def finalizeProd(self, prodid = None, prodinfo = None):
    """ Finalize definition: submit to Transformation service and register metadata
    """
    currtrans = 0
    if self.currtrans:
      currtrans = self.currtrans.getTransformationID()['Value']
    if prodid:
      currtrans = prodid
    if not currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if prodinfo:
      self.prodparameters = prodinfo
      
    info = []
    info.append('%s Production %s has following parameters:\n' % (self.prodparameters['JobType'], currtrans))
    if self.prodparameters.has_key("Process"):
      info.append('- Process %s' % self.prodparameters['Process'])
    if self.prodparameters.has_key("Energy"):
      info.append('- Energy %s GeV' % self.prodparameters["Energy"])
    info.append("- %s events per job" % (self.prodparameters['nbevts'] * self.prodparameters['NbInputFiles']))
    if self.prodparameters.has_key('lumi'):
      if self.prodparameters['lumi']:
        info.append('    corresponding to a luminosity %s fb' % (self.prodparameters['lumi'] * self.prodparameters['NbInputFiles']))
    if self.prodparameters.has_key('FCInputQuery'):
      info.append('Using InputDataQuery :')
      for k, v in self.prodparameters['FCInputQuery'].items():
        info.append('    %s = %s' % (k, v))
        
    info.append('- SW packages %s' % self.prodparameters["SWPackages"])
    # as this is the very last call all applications are registered, so all software packages are known
    #add them the the metadata registration
    for finalpath in self.finalpaths:
      if not self.finalMetaDictNonSearch.has_key(finalpath):
        self.finalMetaDictNonSearch[finalpath] = {}
      self.finalMetaDictNonSearch[finalpath]["SWPackages"] = self.prodparameters["SWPackages"]
    
    info.append('- Registered metadata: ')
    for k, v in self.finalMetaDict.items():
      info.append('    %s = %s' % (k, v))
    info.append('- Registered non searchable metadata: ')
    for k, v in self.finalMetaDictNonSearch.items():
      info.append('    %s = %s' % (k, v))
      
    infoString = string.join(info,'\n')
    self.prodparameters['DetailedInfo'] = infoString
    for n, v in self.prodparameters.items():
      result = self._setProdParameter(currtrans, n, v)
      if not result['OK']:
        self.log.error(result['Message'])

    res = self._registerMetadata()
    if not res['OK']:
      self.log.error("Could not register the following directories :", res['Failed'])
    return S_OK()  
  #############################################################################
  
  def _registerMetadata(self):
    """ Private method
      
      Register path and metadata before the production actually runs. This allows for the definition of the full chain in 1 go. 
    """
    

    failed = []
    for path, meta in self.finalMetaDict.items():
      result = self.fc.createDirectory(path)
      if result['OK']:
        if result['Value']['Successful']:
          if result['Value']['Successful'].has_key(path):
            self.log.verbose("Successfully created directory:", path)
        elif result['Value']['Failed']:
          if result['Value']['Failed'].has_key(path):  
            self.log.error('Failed to create directory:', result['Value']['Failed'][path])
            failed.append(path)
      else:
        self.log.error('Failed to create directory:', result['Message'])
        failed.append(path)
      result = self.fc.setMetadata(path.rstrip("/"), meta)
      if not result['OK']:
        self.log.error("Could not preset metadata", meta)

    for path, meta in self.finalMetaDictNonSearch.items():
      result = self.fc.createDirectory(path)
      if result['OK']:
        if result['Value']['Successful']:
          if result['Value']['Successful'].has_key(path):
            self.log.verbose("Successfully created directory:", path)
        elif result['Value']['Failed']:
          if result['Value']['Failed'].has_key(path):  
            self.log.error('Failed to create directory:', result['Value']['Failed'][path])
            failed.append(path)
      else:
        self.log.error('Failed to create directory:', result['Message'])
        failed.append(path)
      result = self.fc.setMetadata(path.rstrip("/"), meta)
      if not result['OK']:
        self.log.error("Could not preset metadata", meta)        

    if len(failed):
      return  { 'OK' : False, 'Failed': failed}
    return S_OK()
  
  def getMetadata(self):
    """ Return the corresponding metadata of the last step
    """
    metadict = {}
    for meta in self.finalMetaDict.values():
      metadict.update(meta)
    return metadict
  
  def _setProdParameter(self, prodID, pname, pvalue):
    """ Set a production parameter.
    """
    if type(pvalue) == type([]):
      pvalue = string.join(pvalue, '\n')

    prodClient = RPCClient('Transformation/TransformationManager', timeout=120)
    if type(pvalue) == type(2):
      pvalue = str(pvalue)
    result = prodClient.setTransformationParameter(int(prodID), str(pname), str(pvalue))
    if not result['OK']:
      self.log.error('Problem setting parameter %s for production %s and value:\n%s' % (prodID, pname, pvalue))
    return result
  
  def _jobSpecificParams(self, application):
    """ For production additional checks are needed: ask the user
    """

    if self.created:
      return S_ERROR("The production was created, you cannot add new applications to the job.")

    if not application.logfile:
      logf = application.appname + "_" + application.version + "_@{STEP_ID}.log"
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
      self.nbevts = self.jobFileGroupSize * self.nbevts
      res = application.setNbEvts(self.nbevts)
      if not res['OK']:
        return res
    
    if application.nbevts > 0 and self.nbevts > application.nbevts:
      self.nbevts = application.nbevts 

    self.prodparameters['nbevts'] = self.nbevts
    
    if not self.energy:
      if application.energy:
        self.energy = Decimal(str(application.energy))
      else:
        return S_ERROR("Could not find the energy defined, it is needed for the production definition.")
    elif not application.energy:
      res = application.setEnergy(float(self.energy))
      if not res['OK']:
        return res
    if self.energy:
      self._setParameter( "Energy", "float", float(self.energy), "Energy used")      
      self.prodparameters["Energy"] = float(self.energy)
      
    if not self.evttype:
      if hasattr(application, 'evttype'):
        self.evttype = application.evttype
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      self.prodparameters['Process'] = self.evttype
      
    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
    
    if self.prodparameters["SWPackages"]:
      self.prodparameters["SWPackages"] += ";%s.%s" % (application.appname, application.version)
    else :
      self.prodparameters["SWPackages"] = "%s.%s" % (application.appname, application.version)
    
    if not application.accountInProduction:
      res = self._updateProdParameters(application)
      if not res['OK']:
        return res  
      self.checked = True

      return S_OK()
    
    res = application.setOutputSE(self.outputStorage)
    if not res['OK']:
      return res
    
    energypath = ''
    fracappen = modf(float(self.energy)/1000.)
    if fracappen[1] > 0:
      energypath = "%stev/" % (self.energy/Decimal("1000."))
      if energypath == '3.0tev/':
        energypath = '3tev/'
    else:
      energypath =  "%sgev/" % (self.energy)
      if energypath == '500.0gev/':
        energypath = '500gev/'
      elif energypath == '350.0gev/':
        energypath = '350gev/'  

    if not self.basename:
      self.basename = self.evttype
    
    if not self.machine[-1] == '/':
      self.machine += "/"
    if not self.evttype[-1] == '/':
      self.evttypepath = self.evttype + '/'  
    
    path = self.basepath  
    ###Need to resolve file names and paths
    if hasattr(application, "setOutputRecFile") and not application.willBeCut:
      path = self.basepath + self.machine + energypath + self.evttypepath + application.detectortype + "/REC"
      self.finalMetaDict[self.basepath + self.machine + energypath + self.evttypepath] = {"EvtType":self.evttype}
      self.finalMetaDict[self.basepath + self.machine + energypath + self.evttypepath + application.detectortype] = {"DetectorType" : application.detectortype}
      self.finalMetaDict[self.basepath + self.machine + energypath + self.evttypepath + application.detectortype + "/REC"] = {'Datatype':"REC"}
      fname = self.basename+"_rec.slcio"
      application.setOutputRecFile(fname, path)  
      self.log.info("Will store the files under %s" % path)
      self.finalpaths.append(path)
      path = self.basepath + self.machine + energypath + self.evttypepath + application.detectortype + "/DST"
      self.finalMetaDict[self.basepath + self.machine + energypath + self.evttypepath + application.detectortype + "/DST"] = {'Datatype':"DST"}
      fname = self.basename + "_dst.slcio"
      application.setOutputDstFile(fname, path)  
      self.log.info("Will store the files under %s" % path)
      self.finalpaths.append(path)
    elif hasattr(application, "outputFile") and hasattr(application, 'datatype') and not application.outputFile and not application.willBeCut:
      path = self.basepath + self.machine + energypath + self.evttypepath
      self.finalMetaDict[path] = {"EvtType" : self.evttype}      
      if hasattr(application, "detectortype"):
        if application.detectortype:
          path += application.detectortype
          self.finalMetaDict[path] = {"DetectorType" : application.detectortype}
          path += '/'
        elif self.detector:
          path += self.detector
          self.finalMetaDict[path] = {"DetectorType" : self.detector}
          path += '/'
      if not application.datatype and self.datatype:
        application.datatype = self.datatype
      path += application.datatype
      self.finalMetaDict[path] = {'Datatype' : application.datatype}
      self.log.info("Will store the files under %s" % path)
      self.finalpaths.append(path)
      extension = 'stdhep'
      if application.datatype in ['SIM', 'REC']:
        extension = 'slcio'
      fname = self.basename + "_%s" % (application.datatype.lower()) + "." + extension
      application.setOutputFile(fname, path)  
      
    self.basepath = path

    res = self._updateProdParameters(application)
    if not res['OK']:
      return res
    
    self.checked = True
      
    return S_OK()

  def _updateProdParameters(self, application):
    """ Update the prod parameters stored in the production parameters visible from the web
    """
    try:
      self.prodparameters.update(application.prodparameters)
    except Exception, x:
      return S_ERROR("%s: %s" % (Exception, str(x)))
    return S_OK()

  def _jobSpecificModules(self, application, step):
    return application._prodjobmodules(step)
  