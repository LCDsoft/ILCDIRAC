'''
Created on Jan 26, 2012

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC                                                  import S_OK, S_ERROR, gConfig

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from decimal import Decimal
import string

class DBDGeneration(ProductionJob):
  def __init__(self, script = None):
    ProductionJob.__init__(self, script)
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
    for key in catalogdict.keys():
      if not key in allowedkeys:
        return S_ERROR("No allowed to use this key '%s', please check"%key)
      
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
    
    fc = FileCatalogClient()
    
    
            
    return S_OK()  
    
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

    registerdata = ModuleDefinition('DBDGenRegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata,'enable','bool',False,'EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','DBDGenRegisterOutputData')
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
    ro = finalization.createModuleInstance('DBDGenRegisterOutputData','DBDGenRegisterOutputData')
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
    
  def _jobSpecificParams(self,application):  
    if self.created:
      return S_ERROR("The production was created, you cannot add new applications to the job.")
    
    if not application.logfile:
      logf = "SomeLog.log"
      res = application.setLogFile(logf)
      if not res['OK']:
        return res
    if not self.nbevts:
      self.nbevts = application.nbevts
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
      if hasattr(application,'evttype'):
        self.evttype = application.evttype
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      self.prodparameters['Process'] = self.evttype

    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
        
    if self.prodparameters["SWPackages"]:
      self.prodparameters["SWPackages"] +=";%s.%s"%(application.appname,application.version)
    else :
      self.prodparameters["SWPackages"] ="%s.%s"%(application.appname,application.version)

    res = self._updateProdParameters(application)
    if not res['OK']:
      return res

    #Mandatory
    self.checked = True
             
    return S_OK()