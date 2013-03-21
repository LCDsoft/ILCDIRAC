"""
SID DBD specific production job utility

@author: S. Poss
@since: Jul 01, 2012
"""

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC import S_OK, S_ERROR

import types,string
from decimal import Decimal


class ILDProductionJob(ProductionJob):
  def __init__(self):
    super(ILDProductionJob, self).__init__()
    self.machine = 'ilc'
    self.basepath = '/ilc/prod/ilc/mc-dbd/ild/'
    self.polarization = ""
    self.machineparams = ''
    self.detector = ''
    self.ildconfigvers = ''
    self.compatmeta = {}
    
  def setILDConfig(self,Version):
    """ Define the Configuration package to obtain
    """
    appName = 'ILDConfig'
    self._addSoftware(appName, Version)
    self.ildconfigvers = Version
    self.prodparameters['ILDConfigVersion'] = Version
    self._addParameter( self.workflow, 'ILDConfigPackage', 'JDL', appName+Version, 'ILDConfig package' )
    return S_OK()  
  
  def setInputDataQuery(self, metadata):
    """ Define the input data query needed, also get from the data the meta info requested to build the path
    """
    metakeys = metadata.keys()
    res = self.fc.getMetadataFields()
    if not res['OK']:
      print "Could not contact File Catalog"
      return S_ERROR()
    metaFCkeys = res['Value']['DirectoryMetaFields'].keys()
    for key in metakeys:
      for meta in metaFCkeys:
        if meta != key:
          if meta.lower() == key.lower():
            return self._reportError("Key syntax error %s, should be %s" % (key, meta), name = 'SIDProduction')
      if not metaFCkeys.count(key):
        return self._reportError("Key %s not found in metadata keys, allowed are %s" % (key, metaFCkeys))
    #if not metadata.has_key("ProdID"):
    #  return self._reportError("Input metadata dictionary must contain at least a key 'ProdID' as reference")
    res = self.fc.findDirectoriesByMetadata(metadata)
    if not res['OK']:
      return self._reportError("Error looking up the catalog for available directories")
    elif len(res['Value']) < 1:
      return self._reportError('Could not find any directory corresponding to the query issued')
    dirs = res['Value'].values()
    for mdir in dirs:
      res = self.fc.getDirectoryMetadata(mdir)
      if not res['OK']:
        return self._reportError("Error looking up the catalog for directory metadata")
      compatmeta = res['Value']
      compatmeta.update(metadata)
    
    #get all the files available
    res = self.fc.findFilesByMetadata(metadata, self.basepath)
    if not res['OK']:
      return self._reportError("Could not find the files with this metadata")
    my_lfn= res['Value'][0]
    ##Get the meta data of the first one as it should be enough is the registration was 
    ## done right
    res = self.fc.getFileUserMetadata(my_lfn)
    if not res['OK']:
      return self._reportError('Failed to get file metadata, cannot build filename')
    compatmeta.update(res['Value'])
    
    if compatmeta.has_key('EvtClass'):
      if type(compatmeta['EvtClass']) in types.StringTypes:
        self.evttype  = compatmeta['EvtClass']
      if type(compatmeta['EvtClass']) == type([]):
        self.evttype = compatmeta['EvtClass'][0]
    #elif compatmeta.has_key('GenProcessID'):
      
    else:
      return self._reportError("EvtClass is not in the metadata, it has to be!")

    if compatmeta.has_key("Energy"):
      if type(compatmeta["Energy"]) in types.StringTypes:
        self.energycat = compatmeta["Energy"]
      if type(compatmeta["Energy"]) == type([]):
        self.energycat = compatmeta["Energy"][0]

    if compatmeta.has_key("MachineParams"):
      if type(compatmeta["MachineParams"]) in types.StringTypes:
        self.machineTuning = compatmeta["MachineParams"]
      if type(compatmeta["MachineParams"]) == type([]):
        self.machineparams = compatmeta["MachineParams"][0]
    gendata = False    
    if compatmeta.has_key('Datatype'):
      if type(compatmeta['Datatype']) in types.StringTypes:
        self.datatype = compatmeta['Datatype']
        if compatmeta['Datatype'].lower() == 'gen':
          gendata = True
      if type(compatmeta['Datatype']) == type([]):
        self.datatype = compatmeta['Datatype'][0]
        if compatmeta['Datatype'][0].lower() == 'gen':
          gendata = True

    if compatmeta.has_key("DetectorModel") and not gendata:
      if type(compatmeta["DetectorModel"]) in types.StringTypes:
        self.detector = compatmeta["DetectorModel"]
      if type(compatmeta["DetectorModel"]) == type([]):
        self.detector = compatmeta["DetectorModel"][0]

    self.compatmeta = compatmeta
    #TODO: fix base name to ILD conventions, maybe let it partly free.
    self.basename = ''

    self.energy = Decimal(self.energycat)  
    
    self.inputBKSelection = metadata
    self.prodparameters["FCInputQuery"] = self.inputBKSelection

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
    #TODO: create ILDRegisterOutputData
    ro = finalization.createModuleInstance('ILDRegisterOutputData', 'ILDRegisterOutputData')
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
    
    if self.prodparameters["SWPackages"]:
      curpackage = "%s.%s" % (application.appname, application.version)
      if not self.prodparameters["SWPackages"].count(curpackage):
        self.prodparameters["SWPackages"] += ";%s" % ( curpackage )
    else :
      self.prodparameters["SWPackages"] = "%s.%s" % (application.appname, application.version)
    softwarepath = application.version
    
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
      if hasattr(application,'evttype'):
        self.evttype = application.evttype
      else:
        return S_ERROR("Event type not found nor specified, it's mandatory for the production paths.")  
      
    if not application.accountInProduction:
      #needed for the stupid overlay
      res = self._updateProdParameters(application)
      if not res['OK']:
        return res  
      self.checked = True
      return S_OK()  
    
    if not self.outputStorage:
      return S_ERROR("You need to specify the Output storage element")
    
    res = application.setOutputSE(self.outputStorage)
    if not res['OK']:
      return res
    
    if not self.detector:
      if hasattr(application,"detectorModel"):
        self.detector = application.detectorModel
        if not self.detector:
          return S_ERROR("Application does not know which model to use, so the production does not either.")
      #else:
      #  return S_ERROR("Application does not know which model to use, so the production does not either.")
    
    
    energypath =  "%s-%s/" % (self.energy, self.machineparams)# 1000-B1s_ws
 
    
    #TODO: Make sure basename is correct. Maybe allow for setting basename prefix
    # Final name being e.g. NAME_rec.slcio, need to define NAME, maybe based on meta data (include 
    # EvtClass automatically)
    if not self.basename:
      self.basename = 's'+self.ildconfigvers
    if 'DetectorModel'  in self.compatmeta:
      self.basename += '.m'+self.compatmeta['DetectorModel']
    if self.energy:
      self.basename += '.E'+str(self.energy)
    if 'MachineParams' in self.compatmeta:
      self.basename += '-'+self.compatmeta['MachineParams']
      
    if 'GenProcessID' in self.compatmeta:
      self.basename += '.I'+ self.compatmeta['GenProcessID']
    if 'ProcessType' in self.compatmeta:
      self.basename += '.P'+self.compatmeta['ProcessType']
    if 'BeamParticle1' in self.compatmeta:
      self.basename += '.'
      if self.compatmeta['BeamParticle1'] == 'e1':
        self.basename += 'e'
      elif self.compatmeta['BeamParticle1'] == 'E1':
        self.basename += 'p'
      else:
        self.basename += self.compatmeta['BeamParticle1']
    if 'PolarizationB1' in self.compatmeta:
      self.basename += self.compatmeta['PolarizationB1']
    if 'BeamParticle2' in self.compatmeta:
      self.basename += '.'
      if self.compatmeta['BeamParticle2'] == 'E1':
        self.basename += 'p'
      elif self.compatmeta['BeamParticle2'] == 'e1':
        self.basename += 'e'
      else:
        self.basename += self.compatmeta['BeamParticle2']
    if 'PolarizationB2' in self.compatmeta:
      self.basename += self.compatmeta['PolarizationB2']

    
    if not self.machine[-1] == '/':
      self.machine += "/"
    if not self.evttype[-1] == '/':
      evttypemeta = self.evttype
      self.evttype += '/'  
    else:
      evttypemeta = self.evttype.rstrip("/")

    if self.detector:
      if not self.detector[-1] == "/":
        detectormeta = self.detector
        self.detector += "/"
      else:
        detectormeta = self.detector.rstrip("/")
      
    path = self.basepath    
    ###Need to resolve file names and paths
    #TODO: change basepath for ILD Don't forget LOG PATH in ProductionOutpuData module
    if hasattr(application,"setOutputRecFile") and not application.willBeCut:
      path = self.basepath+'rec/'+energypath+self.evttype+self.detector+softwarepath+'/'
      self.finalMetaDict[self.basepath+'dst/'+energypath+self.evttype] = {"EvtType" : evttypemeta}
      self.finalMetaDict[self.basepath+'dst/'+energypath+self.evttype+self.detector] = {"DetectorModel" : detectormeta}
      self.finalMetaDict[self.basepath+'dst/'+energypath+self.evttype+self.detector+softwarepath] = {"SoftwareTag" : softwarepath}
      fname = self.basename+"_rec.slcio"
      application.setOutputRecFile(fname, path)  
      self.finalpaths.append(path)
      path = self.basepath+'dst/'+energypath+self.evttype+self.detector+"/"+softwarepath
      self.finalMetaDict[self.basepath+'dst/'+energypath+self.evttype+self.detector] = {"DetectorModel" : detectormeta}
      self.finalMetaDict[self.basepath+'dst/'+energypath+self.evttype+self.detector+softwarepath] = {"SoftwareTag" : softwarepath}
      fname = self.basename+"_dst.slcio"
      application.setOutputDstFile(fname, path)  
      self.finalpaths.append(path)
    elif hasattr(application,"outputFile") and hasattr(application,'datatype') and (not application.outputFile) and (not application.willBeCut):
      if (not application.datatype) and self.datatype:
        application.datatype = self.datatype
      path = self.basepath + application.datatype
      path += energypath + self.evttype
      self.finalMetaDict[path] = {"EvtType" : evttypemeta}      
      if hasattr(application, "detectorModel"):
        if application.detectorModel:
          path += application.detectorModel
          self.finalMetaDict[path] = {"DetectorModel" : application.detectorModel}
          path += '/'
        elif self.detector:
          path += self.detector
          self.finalMetaDict[path] = {"DetectorModel" : self.detector}
          path += '/'
      path += softwarepath     
      self.finalMetaDict[path] = {"SoftwareTag" : softwarepath}
      path += '/'
      
      self.log.info("Will store the files under", "%s" % path)
      self.finalpaths.append(path)

      extension = 'stdhep'
      if application.datatype in ['SIM', 'REC']:
        extension = 'slcio'
      fname = self.basename+"_%s"%(application.datatype.lower())+ "." + extension
      application.setOutputFile(fname,path)  

    self.basepath = path
    
    res = self._updateProdParameters(application)
    if not res['OK']:
      return res      
    
    self.checked = True
      
    return S_OK()
