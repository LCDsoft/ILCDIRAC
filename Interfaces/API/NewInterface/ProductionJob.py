'''
Production Job class. Used to define new productions.

Mostly similar to :mod:`~ILCDIRAC.Interfaces.API.NewInterface.UserJob`, but
cannot be (and should not be) used like the
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.UserJob` class.

:author: Stephane Poss
:author: Remi Ete
:author: Ching Bon Lam

'''

import os
import shutil
import pprint

from collections import defaultdict
from decimal import Decimal

from DIRAC                                                  import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.Core.Workflow.Module                             import ModuleDefinition
from DIRAC.Core.Workflow.Step                               import StepDefinition
from DIRAC.Resources.Catalog.FileCatalogClient              import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from ILCDIRAC.ILCTransformationSystem.Client.Transformation import Transformation
from ILCDIRAC.Interfaces.API.NewInterface.Job               import Job
from ILCDIRAC.Interfaces.Utilities import JobHelpers

__RCSID__ = "$Id$"

class ProductionJob(Job): #pylint: disable=too-many-public-methods, too-many-instance-attributes
  """ Production job class. Suitable for CLIC studies. Need to sub class and overload for other clients.
  """
  def __init__(self, script = None):
    super(ProductionJob, self).__init__( script )
    self.prodVersion = __RCSID__
    self.dryrun = False
    self.created = False
    self.checked = False
    self.call_finalization = False
    self.finalsdict = {}
    self.transfid = 0
    self.type = 'Production'
    self.csSection = '/Production/Defaults'
    self.ops = Operations()
    self.fc = FileCatalogClient()
    self.trc = TransformationClient()
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.jobFileGroupSize = 1
    self.nbtasks = 1
    self.slicesize =0
    self.basename = ''
    self.basepath = self.ops.getValue('/Production/CLIC/BasePath','/ilc/prod/clic/')
    self.evttype = ''
    self.datatype = ''
    self.energycat = ''
    self.detector = ''
    self.currtrans = None
    self.description = ''

    self.finalpaths = []
    self.finalMetaDict = defaultdict( dict )
    self.prodMetaDict = {}
    self.finalMetaDictNonSearch = {}
    self.metadict_external = {}
    self.outputStorage = ''

    self.proxyinfo = getProxyInfo()

    self.inputdataquery = False
    self.inputBKSelection = {}
    self.plugin = 'Standard'
    self.prodGroup = ''

    self.prodTypes = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction',
                      'MCReconstruction_Overlay', 'Merge', 'Split',
                      'MCGeneration_ILD',
                      'MCSimulation_ILD',
                      'MCReconstruction_ILD',
                      'MCReconstruction_Overlay_ILD',
                      'Split_ILD'
                     ]
    self.prodparameters = {}
    self.prodparameters['NbInputFiles'] = 1
    self.prodparameters['nbevts']  = 0 
    #self.prodparameters["SWPackages"] = ''
    self._addParameter(self.workflow, "IS_PROD", 'JDL', True, "This job is a production job")
    if not script:
      self.__setDefaults()

    self._recBasePaths = {}
      
  #############################################################################
  def __setDefaults(self):
    """Sets some default parameters.
    """
    self.setPlatform(self.ops.getValue('%s/Platform' % (self.csSection), 'x86_64-slc5-gcc43-opt'))
    self.setCPUTime('300000')
    self.setLogLevel('verbose')
    self.setJobGroup('@{PRODUCTION_ID}')

    #version control
    self._setParameter('productionVersion', 'string', self.prodVersion, 'ProdAPIVersion')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID',     'string', self.defaultProdID.zfill(8), 'ProductionID')
    self._setParameter('JOB_ID',            'string', self.defaultProdJobID.zfill(8), 'ProductionJobID')
    self._setParameter('Priority',             'JDL',                     '1', 'Priority')
    self._setParameter('emailAddress',      'string', 'ilcdirac-support@cern.ch', 'CrashEmailAddress')

  def _setParameter(self, name, parameterType, parameterValue, description):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    if self.ops.getValue('%s/%s' % (self.csSection, name), ''):
      self.log.debug('Setting %s from CS defaults = %s' % (name, self.ops.getValue('%s/%s' % (self.csSection, name))))
      self._addParameter(self.workflow, name, parameterType, self.ops.getValue('%s/%s' % (self.csSection, name), 
                                                                               'default'), description)
    else:
      self.log.debug('Setting parameter %s = %s' % (name, parameterValue))
      self._addParameter(self.workflow, name, parameterType, parameterValue, description)
  
  def setConfig(self,version):
    """ Define the Configuration package to obtain
    """
    appName = 'ILDConfig'
    self._addSoftware(appName.lower(), version)
    self.prodparameters['ILDConfigVersion'] = version
    self._addParameter( self.workflow, 'ILDConfigPackage', 'JDL', appName+version, 'ILDConfig package' )
    return S_OK()  

  def setClicConfig(self,version):
    """ Define the ClicConfig package to obtain
    """
    appName = 'ClicConfig'
    self._addSoftware(appName.lower(), version)
    self._addParameter( self.workflow, 'ClicConfigPackage', 'JDL', appName+version, 'ClicConfig package' )
    return S_OK()


  def setDryRun(self, run):
    """ In case one wants to get all the info as if the prod was being submitted
    """
    self.dryrun = run
      
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
      return self._reportError("This input is needed at the beginning of the production definition: it's \
      needed for total number of evts.")
    self.jobFileGroupSize = files
    self.prodparameters['NbInputFiles'] = files
    
  def setNbEvtsPerSlice(self,nbevts):
    """ Define the number of events in a slice.
    """
    self.slicesize = nbevts
    
  #############################################################################
  def setProdType(self, prodType):
    """Set prod type.
    """
    if prodType not in self.prodTypes:
      raise TypeError('Prod must be one of %s' % (', '.join(self.prodTypes)))
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

    retMetaKey = self._checkMetaKeys( metadata.keys() )
    if not retMetaKey['OK']:
      return retMetaKey

    if "ProdID" not in metadata:
      return self._reportError("Input metadata dictionary must contain at least a key 'ProdID' as reference")
    
    retDirs = self._checkFindDirectories( metadata )
    if not retDirs['OK']:
      return retDirs
    dirs = retDirs['Value'].values()
    for mdir in dirs:
      gLogger.notice("Directory: %s" % mdir)
      res = self.fc.getDirectoryUserMetadata(mdir)
      if not res['OK']:
        return self._reportError("Error looking up the catalog for directory metadata")
      compatmeta = res['Value']
      compatmeta.update(metadata)

    if 'EvtType' in compatmeta:
      self.evttype = JobHelpers.getValue( compatmeta['EvtType'], str, basestring )
    else:
      return self._reportError("EvtType is not in the metadata, it has to be!")

    if 'NumberOfEvents' in compatmeta:
      self.nbevts = JobHelpers.getValue( compatmeta['NumberOfEvents'], int, None )

    self.basename = self.evttype
    gLogger.notice("MetaData: %s" % compatmeta)
    gLogger.notice("MetaData: %s" % metadata)
    if "Energy" in compatmeta:
      self.energycat = JobHelpers.getValue( compatmeta["Energy"], str, (int, long, basestring) )
        
    if self.energycat.count("tev"):
      self.energy = Decimal("1000.") * Decimal(self.energycat.split("tev")[0])
    elif self.energycat.count("gev"):
      self.energy = Decimal("1.") * Decimal(self.energycat.split("gev")[0])
    else:
      self.energy = Decimal("1.") * Decimal(self.energycat)
    gendata = False
    if 'Datatype' in compatmeta:
      self.datatype = JobHelpers.getValue( compatmeta['Datatype'], str, basestring )
      if self.datatype == 'gen':
        gendata = True
    if "DetectorType" in compatmeta and not gendata:
      self.detector = JobHelpers.getValue( compatmeta["DetectorType"], str, basestring )
    self.inputBKSelection = metadata
    self.inputdataquery = True
    
    self.prodparameters['nbevts'] = self.nbevts 
    self.prodparameters["FCInputQuery"] = self.inputBKSelection

    return S_OK()

  def setDescription(self, desc):
    """ Set the production's description
    
    :param string desc: Description
    """
    self.description = desc
    return S_OK()

  def getBasePath(self):
    """ Return the base path. Updated by :any:`setInputDataQuery`.
    """
    return self.basepath
  
  def addFinalization(self, uploadData = False, registerData = False, uploadLog = False, sendFailover=False):
    """ Add finalization step

    :param bool uploadData: Upload or not the data to the storage
    :param bool uploadLog: Upload log file to storage (currently only available for admins, thus add them to OutputSandbox)
    :param bool sendFailover: Send Failover requests, and declare files as processed or unused in transfDB
    :param bool registerData: Register data in the file catalog
    """
    #TODO: Do the registration only once, instead of once for each job
    
    self.call_finalization = True
    self.finalsdict['uploadData'] = uploadData
    self.finalsdict['registerData'] = registerData
    self.finalsdict['uploadLog'] = uploadLog
    self.finalsdict['sendFailover'] = sendFailover

  def _addRealFinalization(self):  
    """ This is called at creation: now that the workflow is created at the last minute,
    we need to add this also at the last minute
    """
    importLine = 'from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>'
    
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload, 'enable', 'bool', False, 'EnableFlag')
    body = importLine.replace('<MODULE>', 'UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest, 'enable', 'bool', False, 'EnableFlag')
    body = importLine.replace('<MODULE>', 'FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('RegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata, 'enable', 'bool', False, 'EnableFlag')
    body = importLine.replace('<MODULE>', 'RegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload, 'enable', 'bool', False, 'EnableFlag')
    body = importLine.replace('<MODULE>', 'UploadLogFile')
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
    self.workflow.createStepInstance('Job_Finalization', 'finalization')

    return S_OK()
  
  def createProduction(self, name = None):
    """ Create production.
    """
    
    if not self.proxyinfo['OK']:
      return S_ERROR("Not allowed to create production, you need a ilc_prod proxy.")
    if 'group' in self.proxyinfo['Value']:
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
    self.log.verbose('Workflow XML file name is:', '%s' % fileName)
    try:
      self.createWorkflow()
    except Exception as x:
      self.log.error( "Exception creating workflow", repr(x) )
      return S_ERROR('Could not create workflow')
    with open(fileName, 'r') as oFile:
      workflowXML = oFile.read()
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
    if not self.slicesize:
      Trans.setEventsPerTask(self.jobFileGroupSize * self.nbevts)
    else:
      Trans.setEventsPerTask(self.slicesize)
    self.currtrans = Trans
    if self.dryrun:
      self.log.notice('Would create prod called',name)
      self.transfid = 12345
    else: 
      res = Trans.addTransformation()
      if not res['OK']:
        print res['Message']
        return res
      self.transfid = Trans.getTransformationID()['Value']

    if self.inputBKSelection:
      res = self.applyInputDataQuery()
    if not self.dryrun:
      Trans.setAgentType("Automatic")  
      Trans.setStatus("Active")
    
    finals = []
    for finalpaths in self.finalpaths:
      finalpaths = finalpaths.rstrip("/")
      finalpaths += "/"+str(self.transfid).zfill(8)
      finals.append(finalpaths)
      self.finalMetaDict[finalpaths].update( { "ProdID": self.transfid } )
      self.finalMetaDict[finalpaths].update( self.prodMetaDict )
      # if 'ILDConfigVersion' in self.prodparameters:
      #   self.finalMetaDict[finalpaths].update({"ILDConfig":self.prodparameters['ILDConfigVersion']})
        
      if self.nbevts:
        self.finalMetaDict[finalpaths].update({'NumberOfEvents' : self.jobFileGroupSize * self.nbevts})
    self.finalpaths = finals
    self.created = True
    
    return S_OK()

  def setNbOfTasks(self, nbtasks):
    """ Define the number of tasks you want. Useful for generation jobs.
    """
    if not self.currtrans:
      self.log.error("Not transformation defined earlier")
      return S_ERROR("No transformation defined")
    if self.inputBKSelection and self.plugin not in  ['Limited', 'SlicedLimited']:
      self.log.error("Meta data selection activated, should not specify the number of jobs")
      return S_ERROR()
    self.nbtasks = nbtasks
    self.currtrans.setMaxNumberOfTasks(self.nbtasks) #pylint: disable=E1101
    return S_OK()

  def applyInputDataQuery(self, metadata = None, prodid = None):
    """ Tell the production to update itself using the metadata query specified, i.e. submit new jobs if new files 
    are added corresponding to same query.
    """
    if not self.transfid and self.currtrans:
      self.transfid = self.currtrans.getTransformationID()['Value'] #pylint: disable=E1101
    elif prodid:
      self.transfid = prodid
    if not self.transfid:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if metadata:
      self.inputBKSelection = metadata

    client = TransformationClient()
    if not self.dryrun:
      res = client.createTransformationInputDataQuery(self.transfid, self.inputBKSelection)
      if not res['OK']:
        return res
    else:
      self.log.notice("Would use %s as metadata query for production" % str(self.inputBKSelection))
    return S_OK()
  
  def addMetadataToFinalFiles(self, metadict):
    """ Add additionnal non-query metadata 
    """
    self.metadict_external = metadict
    
    return S_OK()
  
  def finalizeProd(self, prodid = None, prodinfo = None):
    """ Finalize definition: submit to Transformation service and register metadata
    """
    currtrans = 0
    if self.currtrans:
      if not self.dryrun:
        currtrans = self.currtrans.getTransformationID()['Value'] #pylint: disable=E1101
      else:
        currtrans = 12345
    if prodid:
      currtrans = prodid
    if not currtrans:
      print "Not transformation defined earlier"
      return S_ERROR("No transformation defined")
    if prodinfo:
      self.prodparameters = prodinfo
      
    info = []
    info.append('%s Production %s has following parameters:\n' % (self.prodparameters['JobType'], currtrans))
    if "Process" in self.prodparameters:
      info.append('- Process %s' % self.prodparameters['Process'])
    if "Energy" in self.prodparameters:
      info.append('- Energy %s GeV' % self.prodparameters["Energy"])

    if not self.slicesize:
      self.prodparameters['nbevts'] = self.jobFileGroupSize * self.nbevts
    else:
      self.prodparameters['nbevts'] = self.slicesize
    if self.prodparameters['nbevts']:
      info.append("- %s events per job" % (self.prodparameters['nbevts']))
    if self.prodparameters.get('lumi', False):
      info.append('    corresponding to a luminosity %s fb' % (self.prodparameters['lumi'] * \
                                                               self.prodparameters['NbInputFiles']))
    if 'FCInputQuery' in self.prodparameters:
      info.append('Using InputDataQuery :')
      for key, val in self.prodparameters['FCInputQuery'].iteritems():
        info.append('    %s = %s' % (key, val))
    if "SWPackages" in self.prodparameters:
      info.append('- SW packages %s' % self.prodparameters["SWPackages"])
    if "SoftwareTag" in self.prodparameters:
      info.append('- SW tags %s' % self.prodparameters["SoftwareTag"])
    if "ILDConfigVersion" in self.prodparameters:
      info.append('- ILDConfig %s' % self.prodparameters['ILDConfigVersion'])  
    # as this is the very last call all applications are registered, so all software packages are known
    #add them the the metadata registration
    for finalpath in self.finalpaths:
      if finalpath not in self.finalMetaDictNonSearch:
        self.finalMetaDictNonSearch[finalpath] = {}
      if "SWPackages" in self.prodparameters:
        self.finalMetaDictNonSearch[finalpath]["SWPackages"] = self.prodparameters["SWPackages"]
        
      if self.metadict_external:
        self.finalMetaDictNonSearch[finalpath].update(self.metadict_external)  
    
    info.append('- Registered metadata: ')
    for path, metadata in sorted( self.finalMetaDict.iteritems() ):
      info.append('    %s = %s' % (path, metadata))
    info.append('- Registered non searchable metadata: ')
    for path, metadata in sorted( self.finalMetaDictNonSearch.iteritems() ):
      info.append('    %s = %s' % (path, metadata))

    pprint.pprint( info )

    infoString = '\n'.join(info)
    self.prodparameters['DetailedInfo'] = infoString
    
    for name, val in self.prodparameters.iteritems():
      result = self._setProdParameter(currtrans, name, val)
      if not result['OK']:
        self.log.error(result['Message'])

    res = self._registerMetadata()
    if not res['OK']:
      self.log.error("Could not register the following directories :", "%s" % str(res))
    return S_OK()  
  #############################################################################
  
  def _registerMetadata(self):
    """ Private method
      
      Register path and metadata before the production actually runs. This allows for the definition of the full 
      chain in 1 go. 
    """
    
    prevent_registration = self.ops.getValue("Production/PreventMetadataRegistration", False)
    
    if self.dryrun or prevent_registration:
      self.log.notice("Would have created and registered the following\n",
                      "\n ".join( [ " * %s: %s" %( par, val ) for par,val in self.finalMetaDict.iteritems() ] ) )
      self.log.notice("Would have set this as non searchable metadata", str(self.finalMetaDictNonSearch))
      return S_OK()
    
    failed = []
    for path, meta in self.finalMetaDict.items():
      result = self.fc.createDirectory(path)
      if result['OK']:
        if result['Value']['Successful']:
          if path in result['Value']['Successful']:
            self.log.verbose("Successfully created directory:", "%s" % path)
            res = self.fc.changePathMode({ path : 0o775 }, False)
            if not res['OK']:
              self.log.error(res['Message'])
              failed.append(path)
        elif result['Value']['Failed']:
          if path in result['Value']['Failed']:
            self.log.error('Failed to create directory:', "%s" % str(result['Value']['Failed'][path]))
            failed.append(path)
      else:
        self.log.error('Failed to create directory:', result['Message'])
        failed.append(path)

      ## Get existing metadata, if it is the same don't set it again, otherwise throw error
      existingMetadata = self.fc.getDirectoryUserMetadata( path.rstrip("/") )
      metaCopy = dict(meta)
      if existingMetadata['OK']:
        failure = False
        for key, value in existingMetadata['Value'].iteritems():
          if key in meta and meta[key] != value:
            self.log.error( "Metadata values for folder %s disagree for key %s: Existing(%r), new(%r)" % ( path, key, value, meta[key] ) )
            failure = True
          elif key in meta and meta[key] == value:
            metaCopy.pop(key, None)
        if failure:
          return S_ERROR( "Error when setting new metadata, already existing metadata disagrees!" )

      result = self.fc.setMetadata(path.rstrip("/"), metaCopy)
      if not result['OK']:
        self.log.error("Could not preset metadata", "%s" % str(metaCopy))
        self.log.error("Could not preset metadata", "%s" % result['Message'] )

    for path, meta in self.finalMetaDictNonSearch.items():
      result = self.fc.createDirectory(path)
      if result['OK']:
        if result['Value']['Successful']:
          if path in result['Value']['Successful']:
            self.log.verbose("Successfully created directory:", "%s" % path)
            res = self.fc.changePathMode({ path: 0o775}, False)
            if not res['OK']:
              self.log.error(res['Message'])
              failed.append(path)
        elif result['Value']['Failed']:
          if path in result['Value']['Failed']:
            self.log.error('Failed to create directory:', "%s" % str(result['Value']['Failed'][path]))
            failed.append(path)
      else:
        self.log.error('Failed to create directory:', result['Message'])
        failed.append(path)
      result = self.fc.setMetadata(path.rstrip("/"), meta)
      if not result['OK']:
        self.log.error("Could not preset metadata", "%s" % str(meta))        

    if len(failed):
      return  { 'OK' : False, 'Failed': failed}
    return S_OK()
  
  def getMetadata(self):
    """ Return the corresponding metadata of the last step
    """
    metadict = {}
    for meta in self.finalMetaDict.values():
      metadict.update(meta)
    if 'NumberOfEvents' in metadict:
      del metadict['NumberOfEvents'] #As this is not supposed to be a searchable thing
    return metadict
  
  def _setProdParameter(self, prodID, pname, pvalue):
    """ Set a production parameter.
    """
    if isinstance( pvalue, list ):
      pvalue = '\n'.join(pvalue)

    prodClient = RPCClient('Transformation/TransformationManager', timeout=120)
    if isinstance( pvalue, (int, long) ):
      pvalue = str(pvalue)
    if not self.dryrun:  
      result = prodClient.setTransformationParameter(int(prodID), str(pname), str(pvalue))
      if not result['OK']:
        self.log.error('Problem setting parameter %s for production %s and value:\n%s' % (prodID, pname, pvalue))
    else:
      self.log.notice("Adding %s=%s to transformation" % (str(pname), str(pvalue)))
      result = S_OK()
    return result
  
  def _jobSpecificParams(self, application):
    """ For production additional checks are needed: ask the user
    """

    if self.created:
      return S_ERROR("The production was created, you cannot add new applications to the job.")

    if not application.logFile:
      logf = application.appname + "_" + application.version + "_@{STEP_ID}.log"
      res = application.setLogFile(logf)
      if not res['OK']:
        return res
      
      #in fact a bit more tricky as the log files have the prodID and jobID in them
    
    ### Retrieve from the application the essential info to build the prod info.
    if not self.nbevts and not self.slicesize:
      self.nbevts = application.numberOfEvents
      if not self.nbevts:
        return S_ERROR("Number of events to process is not defined.")
    elif not application.numberOfEvents:
      if not self.slicesize:
        res = application.setNumberOfEvents(self.jobFileGroupSize * self.nbevts)
      else:
        res = application.setNumberOfEvents(self.slicesize)
      if not res['OK']:
        return res
    
    if application.numberOfEvents > 0 and (self.jobFileGroupSize * self.nbevts > application.numberOfEvents or self.slicesize > application.numberOfEvents):
      self.nbevts = application.numberOfEvents

    
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
      if hasattr(application, 'eventType'):
        self.evttype = application.eventType
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      self.prodparameters['Process'] = self.evttype
      
    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
    
    curpackage = "%s.%s" % (application.appname, application.version)
    if "SWPackages" in self.prodparameters:      
      if not self.prodparameters["SWPackages"].count(curpackage):
        self.prodparameters["SWPackages"] += ";%s" % ( curpackage )    
    else :
      self.prodparameters["SWPackages"] = "%s" % (curpackage)
    
    if not application.accountInProduction:
      res = self._updateProdParameters(application)
      if not res['OK']:
        return res  
      self.checked = True

      return S_OK()
    
    res = application.setOutputSE(self.outputStorage)
    if not res['OK']:
      return res
    
    energypath = self.getEnergyPath()

    if not self.basename:
      self.basename = self.evttype

    evttypepath = ''
    if not self.evttype[-1] == '/':
      evttypepath = self.evttype + '/'
    
    path = self.basepath  
    ###Need to resolve file names and paths
    if self.energy:
      self.finalMetaDict[self.basepath + energypath] = {"Energy":str(self.energy)}
    if hasattr(application, "setOutputRecFile") and not application.willBeCut:
      path = self.basepath + energypath + evttypepath + application.detectortype + "/REC"
      self.finalMetaDict[self.basepath + energypath + evttypepath] = {"EvtType":self.evttype}
      self.finalMetaDict[self.basepath + energypath + evttypepath + application.detectortype] = {"DetectorType" : application.detectortype}
      self.finalMetaDict[self.basepath + energypath + evttypepath + application.detectortype + "/REC"] = {'Datatype':"REC"}
      fname = self.basename+"_rec.slcio"
      application.setOutputRecFile(fname, path)  
      self.log.info("Will store the files under", "%s" % path)
      self.finalpaths.append(path)
      path = self.basepath + energypath + evttypepath + application.detectortype + "/DST"
      self.finalMetaDict[self.basepath + energypath + evttypepath + application.detectortype + "/DST"] = {'Datatype':"DST"}
      fname = self.basename + "_dst.slcio"
      application.setOutputDstFile(fname, path)  
      self.log.info("Will store the files under", "%s" % path)
      self.finalpaths.append(path)
    elif hasattr(application, "outputFile") and hasattr(application, 'datatype') and not application.outputFile and not application.willBeCut:
      path = self.basepath + energypath + evttypepath
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
      self.log.info("Will store the files under", "%s" % path)
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
    except Exception as x:
      return S_ERROR("Exception: %r" % x )
    return S_OK()

  def _jobSpecificModules(self, application, step):
    return application._prodjobmodules(step)

  def getEnergyPath(self):
    """returns the energy path 250gev or 3tev or 1.4tev etc."""
    energy = Decimal(str(self.energy))
    tD = Decimal('1000.0')
    unit = 'gev' if energy < tD else 'tev'
    energy = energy if energy < tD else energy/tD

    if float(energy).is_integer():
      energyPath = str(int(energy))
    else:
      energyPath = "%1.1f" % energy

    energyPath = energyPath+unit+'/'

    self.log.info ("Energy path is: ", energyPath)
    return energyPath


  def _checkMetaKeys( self, metakeys, extendFileMeta=False ):
    """ check if metadata keys are allowed to be metadata

    :param list metakeys: metadata keys for production metadata
    :param bool extendFileMeta: also use FileMetaFields for checking meta keys
    :returns: S_OK, S_ERROR
    """

    res = self.fc.getMetadataFields()
    if not res['OK']:
      print "Could not contact File Catalog"
      return S_ERROR("Could not contact File Catalog")
    metaFCkeys = res['Value']['DirectoryMetaFields'].keys()
    if extendFileMeta:
      metaFCkeys.extend( res['Value']['FileMetaFields'].keys() )

    for key in metakeys:
      for meta in metaFCkeys:
        if meta != key and meta.lower() == key.lower():
          return self._reportError("Key syntax error %r, should be %r" % (key, meta), name = self.__class__.__name__)
      if key not in metaFCkeys:
        return self._reportError("Key %r not found in metadata keys, allowed are %r" % (key, metaFCkeys))

    return S_OK()

  def _checkFindDirectories( self, metadata ):
    """ find directories by metadata and check that there are directories found

    :param dict metadata: metadata dictionary
    :returns: S_OK, S_ERROR
    """

    res = self.fc.findDirectoriesByMetadata(metadata)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for available directories")
    elif len(res['Value']) < 1:
      return self._reportError('Could not find any directories corresponding to the query issued')
    return res

  def setReconstructionBasePaths( self, recPath, dstPath ):
    """ set the output Base paths for the reconstruction REC and DST files """
    self._recBasePaths['REC'] = recPath
    self._recBasePaths['DST'] = dstPath
