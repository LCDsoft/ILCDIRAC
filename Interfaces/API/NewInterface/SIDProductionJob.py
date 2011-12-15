from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import *

class SIDProductionJob(ProductionJob):
  def __init__(self):
    ProductionJob.__init__(self)
    self.machine = 'ilc'
    
  def setInputDataQuery(self,metadata):
    """ Define the input data query needed, also get from the data the meta info requested to build the path
    """
    res = self.fc.findFilesByMetadata(metadata)

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
    self._addParameter(dataUpload,'enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest,'enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('SIDRegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata,'enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','SIDRegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload,'enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadLogFile')
    logUpload.setBody(body)

    finalization = StepDefinition('Job_Finalization')
    finalization.addModule(dataUpload)
    up = finalization.createModuleInstance('UploadOutputData','dataUpload')
    up.setValue("enable",uploadData)

    finalization.addModule(registerdata)
    ro = finalization.createModuleInstance('SIDRegisterOutputData','SIDRegisterOutputData')
    ro.setValue("enable",registerData)

    finalization.addModule(logUpload)
    ul  = finalization.createModuleInstance('UploadLogFile','logUpload')
    ul.setValue("enable",uploadLog)

    finalization.addModule(failoverRequest)
    fr = finalization.createModuleInstance('FailoverRequest','failoverRequest')
    fr.setValue("enable",sendFailover)
    
    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')

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
      logf = application.appname+"_"+application.version+"_@{STEP_ID}.log"
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
      self.nbevts = self.jobFileGroupSize*self.nbevts
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
      self._setParameter( "Energy", "float", self.energy, "Energy used")      
      
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
    
    
    ###Below modify according to SID conventions
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
        elif self.detector:
          path += self.detector+"/"
      if not application.datatype and self.datatype:
        application.datatype = self.datatype
      path += application.datatype
      self.log.info("Will store the files under %s"%path)
      extension = 'stdhep'
      if application.datatype=='SIM' or application.datatype=='REC':
        extension = 'slcio'
      fname = self.basename+"_%s"%(application.datatype.lower())+"."+extension
      application.setOutputFile(fname,path)  
      
    self.basepath = path
    self.checked = True
      
    return S_OK()
 