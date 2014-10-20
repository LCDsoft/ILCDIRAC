'''
DBD Whizard generation, not completed, never used

@since: Jan 26, 2012

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC                                              import S_OK, S_ERROR
from DIRAC.Core.Workflow.Module                         import ModuleDefinition
from DIRAC.Core.Workflow.Step                           import StepDefinition

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from decimal import Decimal
import string

class DBDGeneration(ProductionJob):
  def __init__(self, script = None):
    super(DBDGeneration, self).__init__(script)
    self.machine = 'ilc'
    self.basepath = '/ilc/prod/ilc/mc-dbd/generated'
    self._addParameter(self.workflow, "IS_DBD_GENPROD", 'JDL', True, "This job is a production job")
    
  def addCatalogEntry(self,catalogdict,prodid = None):
    if not self.created:
      return S_ERROR("You need to have created the production before calling this")
    
    allowedkeys = ['process_id',
                   #'process_names',
                   'process_type',
                   #'CM_energy_in_GeV',
                   'program_name_version',
                   'pythia_version',
                   'stdhep_version',
                   #'OS_version_build=2.6.18-238.5.1.el5;x86_64;GNU/Linux',
                   #'OS_version_run=2.6.18-194.32.1.el5;x86_64;GNU/Linux'
                   #'libc_version=glibc-2.5-49.el5_5.7.x86_64',
                   #'fortran_version',
                   'hadronisation_tune',
                   'tau_decays',
                   #'beam_particle1',
                   #'beam_particle2',
                   'polarization1',
                   'polarization2',
                   'luminosity',
                   'cross_section_in_fb',
                   'cross_section_error_in_fb',
                   #'lumi_file',
                   'file_type',
                   #'total_number_of_events',
                   #'number_of_files',
                   #'file_names',
                   #'number_of_events_in_files',
                   'fileurl',
                   #'logurl',
                   'comment']
    
    inputdict = {}
    for key in catalogdict.keys():
      if not key in allowedkeys:
        return S_ERROR("No allowed to use this key '%s', please check"%key)
      else:
        inputdict[key]=catalogdict[key]
      
    whizardparams = self.prodparameters['whizardparams']
    processes = whizardparams['process_input']['process_id']
    energy = whizardparams['process_input']['sqrts']
    beam_particle1 = whizardparams['beam_input_1']['particle_name']
    beam_particle2 = whizardparams['beam_input_2']['particle_name']
    currtrans = 0
    if self.currtrans:
      currtrans = self.currtrans.getTransformationID()['Value']
    if prodid:
      currtrans = prodid
    
    self.basepath += "/"+str(currtrans).zfill(8) 
    
    fc = FileCatalogClient()
    
    result = fc.createDirectory(self.basepath)
    if result['OK']:
      if result['Value']['Successful']:
        if result['Value']['Successful'].has_key(self.basepath):
          print "Successfully created directory:", self.basepath
      elif result['Value']['Failed']:
        if result['Value']['Failed'].has_key(self.basepath):  
          print 'Failed to create directory:',result['Value']['Failed'][self.basepath]
    else:
      print 'Failed to create directory:',result['Message']
    
    metadict = {}
    
    metadict['process_names']=processes
    metadict['CM_energy_in_GeV']=energy
    metadict['beam_particle1']=beam_particle1
    metadict['beam_particle2']=beam_particle2
    metadict.update(inputdict)
    
    res = fc.setMetadata(self.basepath,metadict)
    if not res['OK']:
      self.log.error("Could not preset metadata")        
    return S_OK()  
    
  def addFinalization(self, uploadData=False, registerData=False, uploadLog = False, sendFailover=False):
    """ Add finalization step

    @param uploadData: Upload or not the data to the storage
    @param uploadLog: Upload log file to storage (currently only available for admins, thus add them to OutputSandbox)
    @param sendFailover: Send Failover requests, and declare files as processed or unused in transfDB
    @param registerData: Register data in the file catalog
    @todo: Do the registration only once, instead of once for each job

    """
    importLine = 'from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>'
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'enable','bool',False,'EnableFlag')
    body = string.replace(importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest,'enable','bool',False,'EnableFlag')
    body = string.replace(importLine,'<MODULE>','FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('DBDGenRegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata,'enable','bool',False,'EnableFlag')
    body = string.replace(importLine,'<MODULE>','DBDGenRegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload,'enable','bool',False,'EnableFlag')
    body = string.replace(importLine,'<MODULE>','UploadLogFile')
    logUpload.setBody(body)

    finalization = StepDefinition('Job_Finalization')
    finalization.addModule(dataUpload)
    up = finalization.createModuleInstance('UploadOutputData','dataUpload')
    up.setValue("enable",uploadData)

    finalization.addModule(registerdata)
    ro = finalization.createModuleInstance('DBDGenRegisterOutputData','DBDGenRegisterOutputData')
    ro.setValue("enable",registerData)

    finalization.addModule(logUpload)
    ul  = finalization.createModuleInstance('UploadLogFile','logUpload')
    ul.setValue("enable",uploadLog)

    finalization.addModule(failoverRequest)
    fr = finalization.createModuleInstance('FailoverRequest','failoverRequest')
    fr.setValue("enable",sendFailover)
    
    self.workflow.addStep(finalization)
    self.workflow.createStepInstance('Job_Finalization', 'finalization')

    return S_OK()
    
  def _jobSpecificParams(self,application):  
    if self.created:
      return S_ERROR("The production was created, you cannot add new applications to the job.")
    
    if not application.LogFile:
      logf = "SomeLog.log"
      res = application.setLogFile(logf)
      if not res['OK']:
        return res
    if not self.nbevts:
      self.nbevts = application.NbEvts
      if not self.nbevts:
        return S_ERROR("Number of events to process is not defined.")

    if not self.energy:
      if application.energy:
        self.energy = Decimal(str(application.energy))
      else:
        return S_ERROR("Could not find the energy defined, it is needed for the production definition.")    
    if self.energy:
      self._setParameter( "Energy", "float", float(self.energy), "Energy used")      
      self.prodparameters["Energy"] = float(self.energy)
      
    if not self.evttype:
      if hasattr(application,'EvtType'):
        self.evttype = application.EvtType
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      self.prodparameters['Process'] = self.evttype

    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
        
    if self.prodparameters["SWPackages"]:
      self.prodparameters["SWPackages"] +=";%s.%s"%(application.appname,application.Version)
    else :
      self.prodparameters["SWPackages"] ="%s.%s"%(application.appname,application.Version)

    res = self._updateProdParameters(application)
    if not res['OK']:
      return res

    #Mandatory
    self.checked = True
             
    return S_OK()