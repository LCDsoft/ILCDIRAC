from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC import S_OK, S_ERROR

import os,types,string
from math import modf
from decimal import Decimal


class SIDProductionJob(ProductionJob):
  def __init__(self):
    ProductionJob.__init__(self)
    self.machine = 'ilc'
    self.basepath = '/ilc/prod/ilc/mc-dbd/sid'
    
  def setInputDataQuery(self, metadata):
    """ Define the input data query needed, also get from the data the meta info requested to build the path
    """
    metakeys = metadata.keys()
    client = FileCatalogClient()
    res = client.getMetadataFields()
    if not res['OK']:
      print "Could not contact File Catalog"
      return S_ERROR()
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
    
    res = client.findFilesByMetadata(metadata)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for available files")
    elif len(res['Value']) < 1:
      return self._reportError('Could not find any files corresponding to the query issued')
    directory = os.path.dirname(res['Value'][0])
    res = client.getDirectoryMetadata(directory)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for directory metadata")
    
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
        self.nbevts = int(compatmeta['NumberOfEvents'])


    self.basename = "" #TO BE DEFINED
    
    if compatmeta.has_key("Energy"):
      if type(compatmeta["Energy"]) in types.StringTypes:
        self.energycat = compatmeta["Energy"]
      if type(compatmeta["Energy"]) == type([]):
        self.energycat = compatmeta["Energy"][0]
        
    if self.energycat.count("tev"):
      self.energy = 1000.*Decimal(self.energycat.split("tev")[0])
    elif self.energycat.count("gev"):
      self.energy = 1.*Decimal(self.energycat.split("gev")[0])
    else:
      self.energy = 1.*Decimal(self.energycat)  
    
    self.inputBKSelection = metadata
    self.inputdataquery = True
    return S_OK()    
    
  def _addRealFinalization(self):
    """ See L{ProductionJob} for definition
    """
    importLine = 'from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>'
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(importLine, '<MODULE>', 'UploadOutputData')
    dataUpload.setBody(body)

    failoverRequest = ModuleDefinition('FailoverRequest')
    failoverRequest.setDescription('Sends any failover requests')
    self._addParameter(failoverRequest, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(importLine, '<MODULE>', 'FailoverRequest')
    failoverRequest.setBody(body)

    registerdata = ModuleDefinition('SIDRegisterOutputData')
    registerdata.setDescription('Module to add in the metadata catalog the relevant info about the files')
    self._addParameter(registerdata, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(importLine, '<MODULE>', 'SIDRegisterOutputData')
    registerdata.setBody(body)

    logUpload = ModuleDefinition('UploadLogFile')
    logUpload.setDescription('Uploads the output log files')
    self._addParameter(logUpload, 'enable', 'bool', False, 'EnableFlag')
    body = string.replace(importLine, '<MODULE>', 'UploadLogFile')
    logUpload.setBody(body)

    finalization = StepDefinition('Job_Finalization')
    finalization.addModule(dataUpload)
    up = finalization.createModuleInstance('UploadOutputData', 'dataUpload')
    up.setValue("enable",self.finalsdict['uploadData'])

    finalization.addModule(registerdata)
    ro = finalization.createModuleInstance('SIDRegisterOutputData', 'SIDRegisterOutputData')
    ro.setValue("enable",self.finalsdict['registerData'])

    finalization.addModule(logUpload)
    ul  = finalization.createModuleInstance('UploadLogFile', 'logUpload')
    ul.setValue("enable",self.finalsdict['uploadLog'])

    finalization.addModule(failoverRequest)
    fr = finalization.createModuleInstance('FailoverRequest', 'failoverRequest')
    fr.setValue("enable",self.finalsdict['sendFailover'])
    
    self.workflow.addStep(finalization)
    self.workflow.createStepInstance('Job_Finalization', 'finalization')

    return S_OK() 
  
  def _jobSpecificParams(self, application):
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
        self.energy = Decimal(str(application.energy))
      else:
        return S_ERROR("Could not find the energy defined, it is needed for the production definition.")
    elif not application.energy:
      res = application.setEnergy(float(self.energy))
      if not res['OK']:
        return res
    if self.energy:
      self._setParameter( "Energy", "float", float(self.energy), "Energy used")      
      
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
    fracappen = modf(float(self.energy)/1000.)
    if fracappen[1] > 0:
      energypath = "%stev/" % (self.energy/Decimal("1000."))
    else:
      energypath =  "%sgev/" % (self.energy/Decimal("1000."))
    self.finalMetaDict[self.basepath+energypath] = {'Energy' : energypath.rstrip("/")}  
    
    if not self.basename:
      self.basename = self.evttype
    
    if not self.machine[-1] == '/':
      self.machine += "/"
    if not self.evttype[-1] == '/':
      self.evttype += '/'  
    
      
    ###Need to resolve file names and paths
    if hasattr(application,"setOutputRecFile"):
      path = self.basepath+energypath+self.evttype+"/REC/"
      self.finalMetaDict[self.basepath+energypath+self.evttype] = {"EvtType" : self.evttype}
      self.finalMetaDict[self.basepath+energypath+self.evttype+"/REC"] = {'Datatype' : "REC"}
      fname = self.basename+"_rec.slcio"
      application.setOutputRecFile(fname, path)  
      path = self.basepath+energypath+self.evttype+"/DST/"
      self.finalMetaDict[self.basepath+energypath+self.evttype+"/DST"] = {'Datatype':"DST"}
      fname = self.basename+"_dst.slcio"
      application.setOutputDstFile(fname, path)  
    elif hasattr(application,"outputFile") and hasattr(application,'datatype') and not application.outputFile:
      path = self.basepath+energypath+self.evttype
      self.finalMetaDict[path]= {"EvtType" : self.evttype}      
      if not application.datatype and self.datatype:
        application.datatype = self.datatype
      path += application.datatype
      self.finalMetaDict[path] = {"Datatype":application.datatype}      
      self.log.info("Will store the files under %s" % path)
      fname = self.basename+"_%s"%(application.datatype.lower())+".slcio"
      application.setOutputFile(fname,path)  

    self.basepath = path
    
    res = self._updateProdParameters(application)
    if not res['OK']:
      return res      
    
    self.checked = True
      
    return S_OK()
