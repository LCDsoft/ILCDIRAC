'''
@since: Mar 12, 2013

@author: sposs
'''

from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from ILCDIRAC.Core.Utilities.resolveOFnames                import getProdFilename
from ILCDIRAC.Core.Utilities.InputFilesUtilities           import getNumberOfevents
from ILCDIRAC.Core.Utilities.resolveIFpaths                import resolveIFpaths

from DIRAC import gLogger, S_OK, S_ERROR
import os, shutil

class MoveInFC(ModuleBase):
  '''
  classdocs
  '''
  def __init__(self):
    '''
    Constructor
    '''
    super(MoveInFC, self).__init__()
    self.enable = False
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "MoveInFC" )
    self.applicationName = 'MoveInFC'
    self.rm = ReplicaManager()
    self.listoutput = {}
    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
      
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        #self.OutputFile = getProdFilename(self.outputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
      
    if len(self.InputData):
      if not self.workflow_commons.has_key("Luminosity") or not self.workflow_commons.has_key("NbOfEvents"):
        res = getNumberOfevents(self.InputData)
        if res["nbevts"] and not self.workflow_commons.has_key("Luminosity") :
          self.workflow_commons["NbOfEvents"] = res["nbevts"]
          self.workflow_commons["NbOfEvts"] = res["nbevts"]
          if self.NumberOfEvents > res["nbevts"]:
            self.NumberOfEvents = res["nbevts"]
        if res["lumi"] and not self.workflow_commons.has_key("NbOfEvents"):
          self.workflow_commons["Luminosity"] = res["lumi"]
        if res.has_key('EvtType') and not self.processID:
          self.processID = res['EvtType']

    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        self.InputFile.append(files)
    
    if self.step_commons.has_key('listoutput'):
      self.listoutput = self.step_commons['listoutput'][0]
    
    return S_OK()
  
  def execute(self):
    """ Run the module
    """
    result = self.resolveInputVariables()
    if not result['OK']:
      return result
    self.result = S_OK()

    if not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )

    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('%s should not proceed as previous step did not end properly'% self.applicationName)

    ### Now remove the files in the FC
    lfns = self.InputFile
    
    ##Check that all the files are here:
    res = resolveIFpaths(lfns)
    if not res['OK']:
      self.log.error(res['Message'])
      return S_ERROR("Failed to find a file locally")
    
    #All files are here and available
    paths = res['Value']
    localpaths = []
    for inputfile in paths:
      basename = os.path.basename(inputfile)
      locname = os.path.join(os.getcwd(),basename)
      if not locname == inputfile:
        try:
          shutil.copy(inputfile, locname)
        except:
          self.log.error("Failed to copy file locally, will have to stop")
          return S_ERROR("Failed copy to local directory")
      localpaths.append(locname)
      try:
        os.unlink(inputfile)
      except OSError:
        self.log.warn("Failed to remove intiial file, increased disk space usage")
        
    #all the files are in the run directory 
    
    #Update the listoutput
    if self.listoutput:
      outputlist = []
      for f in localpaths:
        item = {}
        item['outputFile'] = f
        item['outputPath'] = self.listoutput['outputPath']
        item['outputDataSE'] = self.listoutput['outputDataSE']
        outputlist.append(item)
      self.step_commons['listoutput'] = outputlist
    #Now remove them
    if self.enable:
      res = self.rm.removeFile(lfns, force=True)
      if not res['OK']:
        self.log.error("Failed to remove the files")
        self.setApplicationStatus("Failed to remove the file")
        #return S_ERROR("Failed to remove the files")
    else:
      self.log.info("Would have removed: ","%s" % str(lfns))
    
    ## Now the files are not on the storage anymore, they exist only locally. We can hope 
    ## that the job will not be killed between now and the time the UploadOutputData module
    ## is called
    
    return S_OK()