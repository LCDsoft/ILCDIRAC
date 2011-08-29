'''
Production Job class. Used to define new productions. 

Mostly similar to L{UserJob}, but cannot be (and should not be) used like the UserJob class.

@author: Stephane Poss
@author: Remi Ete
@author: Ching Bon Lam
'''

__RCSID__ = "$Id: "

from ILCDIRAC.Interfaces.API.NewInterface.Job               import Job
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Client.Transformation       import Transformation

from DIRAC.Resources.Catalog.FileCatalogClient              import FileCatalogClient
from DIRAC.Core.Security.Misc                               import getProxyInfo

from math                                                   import modf

from DIRAC                                                  import S_OK, S_ERROR, gConfig


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

    self.outputStorage = ''

    self.proxyinfo = getProxyInfo()

    self.prodTypes = ['MCGeneration', 'MCSimulation', 'Test', 'MCReconstruction', 'MCReconstruction_Overlay']
    
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
     
  def setOutputSE(self,outputse):
    """ Define where the output file(s) will go. 
    """
    self.outputStorage = outputse
    return S_OK()
    
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
    return S_OK()

  def setMachine(self,machine):
    self.machine = machine

  def getBasePath(self):
    """ Return the base path. Updated by L{setInputDataQuery}.
    """
    return self.basepath
  
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
      self._setParameter( "Energy", "int", self.energy, "Energy used")      
      
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
    
    if not self.machine[-1]=='/':
      self.machine += "/"
    if not self.evttype[-1]=='/':
      self.evttype += '/'  
      
    ###Need to resolve file names and paths
    if hasattr(application,"outputRecFile"):
      path = self.basepath+self.machine+energypath+self.evttype+application.detectortype+"/REC/"
      fname = self.basename+"_rec.slcio"
      application.OutputRecFile(fname,path)  
      path = self.basepath+self.machine+energypath+self.evttype+application.detectortype+"/DST/"
      fname = self.basename+"_dst.slcio"
      application.OutputDstFile(fname,path)  
    elif hasattr(application,"outputFile") and hasattr(application,'datatype') and not application.outputFile:
      path = self.basepath+self.machine+energypath+self.evttype+application.detectortype+"/"+application.datatype+"/"
      extension = 'stdhep'
      if application.datatype=='SIM':
        extension = 'slcio'
      fname = self.basename+"_%s"%(application.datatype.lower())+"."+extension
      application.setOutputFile(fname,path)  
      
    return S_OK()

  def _jobSpecificModules(self,application,step):
    return application._prodjobmodules(step)
  