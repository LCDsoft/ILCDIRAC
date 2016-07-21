'''
:since: Mar 12, 2013
:author: sposs
'''
__RCSID__ = "$Id$"
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.DataManagementSystem.Client.DataManager         import DataManager
from ILCDIRAC.Core.Utilities.resolvePathsAndNames          import resolveIFpaths

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
    self.repMan = DataManager()
    self.listoutput = {}
    self.outputpath = ''
    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """
    
    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        self.InputFile.append(files)
    
    if 'listoutput' in self.step_commons:
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
        except shutil.Error:
          self.log.error("Failed to copy file locally, will have to stop")
          return S_ERROR("Failed copy to local directory")
      localpaths.append(locname)
      try:
        os.unlink(inputfile)
      except OSError:
        self.log.warn("Failed to remove initial file, increased \
        disk space usage")
        
    #all the files are in the run directory 
    
    #get all metadata, ancestor/daughter relations, etc. for all the files
    
    #Update the listoutput
    if self.listoutput:
      outputlist = []
      for localFile in localpaths:
        item = {}
        item['outputFile'] = localFile
        item['outputPath'] = self.listoutput['outputPath']
        item['outputDataSE'] = self.listoutput['outputDataSE']
        outputlist.append(item)
      if self.enable:
        self.step_commons['listoutput'] = outputlist
      else:
        self.log.info("listoutput would have been ",outputlist)

    ## Make sure the path contains / at the end as we are going to 
    ## concatenate final path and local files
    if not self.outputpath[-1]=='/':
      self.outputpath += "/"

    if 'ProductionOutputData' in self.workflow_commons:
      file_list = ";".join([self.outputpath+name for name in [os.path.basename(fin) for fin in localpaths]])
      if self.enable:
        self.workflow_commons['ProductionOutputData'] = file_list
      else:
        self.log.info("ProductionOutputData would have been", file_list)
      
    #Now remove them
    if self.enable:
      res = self.repMan.removeFile(lfns, force=True)
      if not res['OK']:
        self.log.error("Failed to remove the files")
        self.setApplicationStatus("Failed to remove the file")
        #return S_ERROR("Failed to remove the files")
    else:
      self.log.info("Would have removed: ","%s" % str(lfns))
    
    ## Now the files are not on the storage anymore, they exist only locally. We can hope 
    ## that the job will not be killed between now and the time the UploadOutputData module
    ## is called
    
    return self.finalStatusReport(0)
