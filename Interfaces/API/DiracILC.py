"""
DiracILC is the API to use to submit jobs in the ILC VO

@since:  Apr 13, 2010

@author: Stephane Poss
"""
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.Interfaces.API.Job                       import Job
from DIRAC.Core.Utilities.List                      import breakListIntoChunks, sortList
from ILCDIRAC.Core.Utilities.ProcessList            import ProcessList
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

from DIRAC import gConfig, S_ERROR, S_OK, gLogger
import string,os


COMPONENT_NAME='DiracILC'

class DiracILC(Dirac):
  """DiracILC is VO specific API Dirac
  
  Adding specific ILC functionalities to the Dirac class, and implement the preSubmissionChecks method
  """
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the ILCDIRAC API.
    """
    #self.dirac = Dirac(WithRepo=WithRepo, RepoLocation=RepoLocation)
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)
    self.log = gLogger
    self.software_versions = {}
    self.pl = None
    
  def getProcessList(self):    
    processlistpath = gConfig.getOption("/LocalSite/ProcessListPath", "")
    if not processlistpath['Value']:
      gLogger.info('Will download the process list locally. To gain time, please put it somewhere and add to your dirac.cfg \
                   the entry /LocalSite/ProcessListPath pointing to the file')
      pathtofile = gConfig.getOption("/Operations/ProcessList/Location","")
      if not pathtofile['Value']:
        gLogger.error("Could not get path to process list")
        processlist = ""
      else:
        rm = ReplicaManager()
        rm.getFile(pathtofile['Value'])
        processlist=os.path.basename(pathtofile['Value'])   
    else:
      processlist = processlistpath['Value']
    self.pl = ProcessList(processlist)
    return self.pl
    
  def preSubmissionChecks(self,job,mode):
    """Overridden method from DIRAC.Interfaces.API.Dirac
    
    Checks from CS that required software packages are available.
    @param job: job definition.
    @param mode: submission mode, not used here. 
    
    @return: S_OK() or S_ERROR()
    """
    return self._do_check(job)
    
  def checkparams(self,job):
    """Helper method
    
    Method used for stand alone checks of job integrity. Calls the formulation error checking of the job
    
    Actually checks that all input are available and checks that the required software packages are available in the CS
    @param job: job object
    @return: S_OK() or S_ERROR()  
    """
    try:
      formulationErrors = job._getErrors()
    except Exception, x:
      self.log.verbose( 'Could not obtain job errors:%s' % ( x ) )
      formulationErrors = {}

    if formulationErrors:
      for method, errorList in formulationErrors.items():
        self.log.error( '>>>> Error in %s() <<<<\n%s' % ( method, string.join( errorList, '\n' ) ) )
      return S_ERROR( formulationErrors )
    return self._do_check(job)

  def giveProcessList(self):
    """ Returns the list of Processes
    """
    return self.pl
  
  def retrieveRepositoryOutputDataLFNs(self,requestedStates = ['Done']):
    """Helper function
    
    Get the list of uploaded output data for a set of jobs in a repository
    
    @param requestedStates: List of states requested for filtering the list
    @type requestedStates: list of strings
    @return: list
    """
    list = []
    if not self.jobRepo:
      gLogger.warn( "No repository is initialised" )
      return S_OK()
    jobs = self.jobRepo.readRepository()['Value']
    for jobID in sortList( jobs.keys() ):
      jobDict = jobs[jobID]
      if jobDict.has_key( 'State' ) and ( jobDict['State'] in requestedStates ):
        if ( jobDict.has_key( 'UserOutputData' ) and ( not int( jobDict['UserOutputData'] ) ) ) or ( not jobDict.has_key( 'UserOutputData' ) ):
          params = self.parameters(int(jobID))
          if params['OK']:
            if params['Value'].has_key('UploadedOutputData'):
              lfn = params['Value']['UploadedOutputData']
              list.append(lfn)
    return list
  
  def _do_check(self,job):
    sysconf = job.systemConfig
    apps = job.workflow.findParameter("SoftwarePackages")
    if apps:
      apps = apps.getValue()
      for appver in apps.split(";"):
        app = appver.split(".")[0].lower()#first element
        vers = appver.split(".")[1:]#all the others
        vers = string.join(vers,".")
        res = self._checkapp(sysconf,app,vers)
        if not res['OK']:
          return res
    outputpathparam = job.workflow.findParameter("UserOutputPath")
    if outputpathparam:
      outputpath = outputpathparam.getValue()
      res = self._checkoutputpath(outputpath)
      if not res['OK']:
        return res
    useroutputdata = job.workflow.findParameter("UserOutputData")
    useroutputsandbox = job.addToOutputSandbox
    if useroutputdata:
      res = self._checkdataconsistency(useroutputdata.getValue(), useroutputsandbox)
      if not res['OK']: 
        return res

    return S_OK()
  
  def _checkapp(self,config,appName,appVersion):
    app_version= gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(config,appName,appVersion),'')
    if not app_version:
      self.log.error("Could not find the specified software %s_%s for %s, check in CS"%(appName,appVersion,config))
      return S_ERROR("Could not find the specified software %s_%s for %s, check in CS"%(appName,appVersion,config))
    return S_OK()
  
  def _checkoutputpath(self,path):
    if path.find("//")>-1 or path.find("/./")>-1 or path.find("/../")>-1:
      self.log.error("OutputPath of setOutputData() contains invalid characters, please remove any //, /./, or /../")
      return S_ERROR("Invalid path")
    path = path.rstrip()
    if path[-1]=="/":
      self.log.error("Please strip trailing / from outputPath in setOutputData()")
      return S_ERROR("Invalid path")
    return S_OK()
  
  def _checkdataconsistency(self,useroutputdata,useroutputsandbox):
    useroutputdata = useroutputdata.split(";")
    for data in useroutputdata:
      for item in useroutputsandbox:
        if data==item:
          self.log.error("Output data and sandbox should not contain the same things.")
          return S_ERROR("Output data and sandbox should not contain the same things.")
      if data.find("*")>-1:
        self.log.error("Remove wildcard characters from output data definition: must be exact files")
        return S_ERROR("Wildcard character in OutputData definition")
    return S_OK()

  def checkInputSandboxLFNs(self,job):
    lfns = []
    inputsb = job.workflow.findParameter("InputSandbox")
    if inputsb:
      list = inputsb.getValue()
      if list:
        list = list.split(';')
        for f in list:
          if f.lower().count('lfn:'):
            lfns.append(f.replace('LFN:','').replace('lfn:',''))
    if len(lfns):
      res = self.getReplicas(lfns)
      if not res["OK"]:
        return S_ERROR('Could not get replicas')
      failed = res['Value']['Failed']
      if failed:
        self.log.error('Failed to find replicas for the following files %s'%string.join(failed, ', '))
        return S_ERROR('Failed to find replicas')
      else:
        self.log.info('All LFN files have replicas available')
    return S_OK()
  