'''
Created on Jun 24, 2010

@author: sposs
'''
__RCSID__ = "$Id: Production.py 24/06/2010 sposs $"

from DIRAC.Core.Workflow.Workflow                     import *
from ILCDIRAC.Interfaces.API.DiracILC                       import DiracILC
from DIRAC.Core.Utilities.List                        import removeEmptyElements

from ILCDIRAC.Interfaces.API.ILCJob                           import *
from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR
import string

 
class Production(ILCJob): 
  ############################################################################# 
  def __init__(self,script=None):
    """Instantiates the Workflow object and some default parameters.
    """
    Job.__init__(self,script) 
    self.prodVersion=__RCSID__
    self.csSection = '/Production/Defaults'
    self.StepCount = 0
    self.currentStepPrefix = ''
    self.inputDataType = 'STDHEP' #Default
    #self.tier1s=gConfig.getValue('%s/Tier1s' %(self.csSection),['LCG.CERN.ch','LCG.CNAF.it','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.GRIDKA.de','LCG.IN2P3.fr','LCG.SARA.nl'])
    #self.histogramName =gConfig.getValue('%s/HistogramName' %(self.csSection),'@{applicationName}_@{STEP_ID}_Hist.root')
    #self.histogramSE =gConfig.getValue('%s/HistogramSE' %(self.csSection),'CERN-HIST')
    self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'x86_64-slc5-gcc43-opt')
    #self.systemConfig = gConfig.getValue('%s/SystemConfig' %(self.csSection),'slc4_ia32_gcc34')
    self.inputDataDefault = gConfig.getValue('%s/InputDataDefault' %(self.csSection),'/ilc/prod/clic/3tev/gen/bb/0/BS_01.stdhep')
    self.defaultProdID = '12345'
    self.defaultProdJobID = '12345'
    self.ioDict = {}
    #self.gaussList = []
    self.prodTypes = ['MCSimulation','Test']
    self.pluginsTriggeringStreamTypes = ['ByFileTypeSize','ByRunFileTypeSize','ByRun','AtomicRun']
    self.name='unspecifiedWorkflow'
    self.firstEventType = ''
    #self.bkSteps = {}
    self.prodGroup = ''
    self.plugin = ''
    self.inputFileMask = ''
    self.inputBKSelection = {}
    self.jobFileGroupSize = 0
    self.ancestorProduction = ''
    self.importLine = """
from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
    if not script:
      self.__setDefaults()

  #############################################################################
  def __setDefaults(self):
    """Sets some default parameters.
    """
    self.setType('MCSimulation')
    self.setSystemConfig(self.systemConfig)
    self.setCPUTime('600000')
    self.setLogLevel('verbose')
    self.setJobGroup('@{PRODUCTION_ID}')
    self.setFileMask('dummy')

    #version control
    self._setParameter('productionVersion','string',self.prodVersion,'ProdAPIVersion')

    #General workflow parameters
    self._setParameter('PRODUCTION_ID','string',self.defaultProdID.zfill(8),'ProductionID')
    self._setParameter('JOB_ID','string',self.defaultProdJobID.zfill(8),'ProductionJobID')
    #self._setParameter('poolXMLCatName','string','pool_xml_catalog.xml','POOLXMLCatalogName')
    self._setParameter('Priority','JDL','1','Priority')
    self._setParameter('emailAddress','string','stephane.poss@cern.ch','CrashEmailAddress')
    self._setParameter('DataType','string','MC','Priority') #MC or DATA
    self._setParameter('outputMode','string','Local','SEResolutionPolicy')

    #Options related parameters
    self._setParameter('EventMaxDefault','string','-1','DefaultNumberOfEvents')
    #BK related parameters
    self._setParameter('lfnprefix','string','prod','LFNprefix')
    self._setParameter('lfnpostfix','string','2009','LFNpostfix')
    #self._setParameter('conditions','string','','SimOrDataTakingCondsString')
  #############################################################################
  def _setParameter(self,name,parameterType,parameterValue,description):
    """Set parameters checking in CS in case some defaults need to be changed.
    """
    if gConfig.getValue('%s/%s' %(self.csSection,name),''):
      self.log.debug('Setting %s from CS defaults = %s' %(name,gConfig.getValue('%s/%s' %(self.csSection,name))))
      self._addParameter(self.workflow,name,parameterType,gConfig.getValue('%s/%s' %(self.csSection,name),'default'),description)
    else:
      self.log.debug('Setting parameter %s = %s' %(name,parameterValue))
      self._addParameter(self.workflow,name,parameterType,parameterValue,description)

  def addMokkaStep(self,appvers,steeringfile,detectormodel,numberofevents,outputfile,outputpath,outputSE):
    self.StepCount +=1
    mokkaStep = ModuleDefinition('MokkaStep')
    mokkaStep.setDescription('Mokka step: simulation in ILD-like geometries context')
    body = string.replace(self.importLine,'<MODULE>','MokkaAnalysis')
    mokkaStep.setBody(body)
    
    createoutputlist = ModuleDefinition('CreateOutputList')
    createoutputlist.setDescription('Compute the outputList parameter, needed by outputdataPolicy')
    body = string.replace(self.importLine,'<MODULE>','ComputeOutputDataList')
    createoutputlist.setBody(body)
     
    MokkaAppDefn = StepDefinition('Mokka_App_Step')
    MokkaAppDefn.addModule(mokkaStep)
    MokkaAppDefn.createModuleInstance('MokkaStep', 'MokkaApp')
    MokkaAppDefn.addModule(createoutputlist)
    MokkaAppDefn.createModuleInstance('CreateOutputList','compOutputDataList')
    self._addParameter(MokkaAppDefn,'applicationVersion','string','','ApplicationVersion')
    self._addParameter(MokkaAppDefn,"steeringFile","string","","Name of the steering file")
    self._addParameter(MokkaAppDefn,"detectorModel","string","",'Detector Model')
    self._addParameter(MokkaAppDefn,"numberOfEvents","int",0,"Number of events to generate")
    self._addParameter(MokkaAppDefn,"applicationLog","string","","Application log file")
    self._addParameter(MokkaAppDefn,"outputPath","string","","Output data path")
    self._addParameter(MokkaAppDefn,"outputFile","string","","output file name")
    self._addParameter(MokkaAppDefn,'listoutput',"list",[],"list of output file name")
    mstep = self.workflow.createStepInstance('Mokka_App_Step','Mokka')
    mstep.setValue('applicationVersion',appvers)
    mstep.setValue('steeringFile',steeringfile)
    mstep.setValue("detectorModel",detectormodel)
    mstep.setValue("numberOfEvents",numberofevents)
    mstep.setValue('applicationLog', '@{applicationName}_@{STEP_ID}.log')
    mstep.setValue("outputFile",outputfile)
    mstep.setValue("outputPath",outputpath)
    outputList=[]
    outputList.append({"outputFile":"@{outputFile}","outputPath":"@{outputPath}","outputDataSE":outputSE})
    mstep.setValue('listoutput',(outputList))

    self.__addSoftwarePackages('mokka.%s' %(appvers))
    self.ioDict["MokkaStep"]=mstep.getName()
    return S_OK()
  
  def addMarlinStep(self):
    return

  def addSLICStep(self):
    return

  def addLCSIMStep(self):
    return
  
  def addFinalizationStep(self,uploadData=False):
    dataUpload = ModuleDefinition('UploadOutputData')
    dataUpload.setDescription('Uploads the output data')
    self._addParameter(dataUpload,'Enable','bool','True','EnableFlag')
    body = string.replace(self.importLine,'<MODULE>','UploadOutputData')
    dataUpload.setBody(body)


    finalization = StepDefinition('Job_Finalization')
    dataUpload.setLink('Enable','self','UploadEnable')
    finalization.addModule(dataUpload)
    finalization.createModuleInstance('UploadOutputData','dataUpload')
    self._addParameter(finalization,'UploadEnable','bool',str(uploadData),'EnableFlag')
    self.workflow.addStep(finalization)
    finalizeStep = self.workflow.createStepInstance('Job_Finalization', 'finalization')
    finalizeStep.setValue('UploadEnable',uploadData)

    return
  #############################################################################
  def __addSoftwarePackages(self,nameVersion):
    """ Internal method to accumulate software packages.
    """
    swPackages = 'SoftwarePackages'
    description='ILCSoftwarePackages'
    if not self.workflow.findParameter(swPackages):
      self._addParameter(self.workflow,swPackages,'JDL',nameVersion,description)
    else:
      apps = self.workflow.findParameter(swPackages).getValue()
      apps = apps.split(';')
      apps.append(nameVersion)
      apps = removeEmptyElements(apps)
      apps = string.join(apps,';')
      self._addParameter(self.workflow,swPackages,'JDL',apps,description)
