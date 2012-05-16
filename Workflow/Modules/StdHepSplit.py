#####################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/ILCDIRAC/trunk/ILCDIRAC/Workflow/Modules/LCIOConcatenate.py $
#####################################################
"""
Module to concatenate LCIO files
"""

__RCSID__ = "$Id: LCIOConcatenate.py 48402 2012-03-09 09:33:09Z sposs $"

from DIRAC.Core.Utilities.Subprocess                      import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC                                                import S_OK, S_ERROR, gLogger
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc
from ILCDIRAC.Core.Utilities.resolveOFnames               import getProdFilename
from ILCDIRAC.Core.Utilities.resolveIFpaths               import resolveIFpaths
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import GetNewLDLibs
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder

from DIRAC import gConfig, S_OK, S_ERROR

import os

class StdHepSplit(ModuleBase):
  """ StdHep split module
  """
  def __init__(self):

    ModuleBase.__init__(self)

    self.STEP_NUMBER = ''
    self.log         = gLogger.getSubLogger( "StdHepSplit" )
    self.result      = S_ERROR()
    self.nbEventsPerSlice = 0
    self.InputFile = []
    # Step parameters
    self.prod_outputdata = []
    #
    self.listoutput = {}
    self.OutputFile = []
    self.log.info("%s initialized" % ( self.__str__() ))

  def applicationSpecificInputs(self):
    """ Resolve LCIO concatenate specific parameters, called from ModuleBase
    """

    if not self.OutputFile:
      return S_ERROR( 'No output file defined' )
    
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        if self.workflow_commons.has_key('ProductionOutputData'):
          self.prod_outputdata = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in self.prod_outputdata:
            if obj.lower().count("_gen_"):
              self.OutputFile = os.path.basename(obj)
        else:
          self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
                                              int(self.workflow_commons["JOB_ID"]))
          
    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".stdhep")>-1:
          self.InputFile.append(files)
      
    if self.step_commons.has_key('listoutput'):
      self.listoutput = self.step_commons['listoutput'][0]
      
    return S_OK('Parameters resolved')

  def execute(self):
    """ Execute the module, called by JobAgent
    """
    # Get input variables

    self.result = self.resolveInputVariables()
    # Checks

    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )

    if not self.result['OK']:
      return self.result

    
    if len(self.InputFile):
      res = resolveIFpaths(self.InputFile)
      if not res['OK']:
        self.setApplicationStatus('StdHepSplit: missing input stdhep file')
        return S_ERROR('Missing stdhep file!')
      runonstdhep = res['Value'][0]
    else:
      return S_OK("No files found to process")
    # removeLibc

    #removeLibc( os.path.join( os.environ["LCIO"], "lib" ) )

    prefix = ''
    if self.OutputFile:
      prefix = self.OutputFile.split('.stdhep')[0]
    else:
      prefix = "this_split"
    self.log.info("Will rename all files using '%s' as base."%prefix)

    # Setting up script
    splitDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"stdhepsplit",self.applicationVersion),'')
    splitDir = splitDir.replace(".tgz","").replace(".tar.gz","")
    res = getSoftwareFolder(splitDir)
    if not res['OK']:
      self.setApplicationStatus('StdHepSplit: Could not find neither local area not shared area install')
      return res
    
    mysplitDir = res['Value']
    new_ld_lib = GetNewLDLibs(self.systemConfig,"stdhepsplit",self.applicationVersion)
    LD_LIBRARY_PATH = os.path.join(mysplitDir,"lib")+":"+new_ld_lib

    

    scriptContent = """
#!/bin/sh

################################################################################
# Dynamically generated script by LCIOConcatenate module                       #
################################################################################

declare -x LD_LIBRARY_PATH=%s

%s/hepsplit --infile %s --nw_per_file %s --outpref %s

exit $?

""" %(
    LD_LIBRARY_PATH,
    mysplitDir,
    runonstdhep,
    self.nbEventsPerSlice,
    prefix
)

    # Write script to file

    scriptPath = 'StdHepSplit_%s_Run_%s.tcl' %( self.applicationVersion, self.STEP_NUMBER )

    if os.path.exists(scriptPath):
      os.remove(scriptPath)

    script = open( scriptPath, 'w' )
    script.write( scriptContent )
    script.close()

    # Setup log file for application stdout

    if os.path.exists(self.applicationLog):
      os.remove(self.applicationLog)

    # Run code

    os.chmod( scriptPath, 0755 )

    command = '"./%s"' %( scriptPath )

    self.setApplicationStatus( 'StdHepSplit %s step %s' %( self.applicationVersion, self.STEP_NUMBER ) )
    self.stdError = ''

    self.result = shellCall(
                            0,
                            command,
                            callbackFunction = self.redirectLogOutput,
                            bufferLimit = 20971520
                            )

        # Check results

    resultTuple = self.result['Value']
    status      = resultTuple[0]

    if not os.path.exists(self.applicationLog):
      self.log.error("Cannot access log file, cannot proceed")
      return S_ERROR("Failed reading the log file")

    logf = file(self.applicationLog,"r")
    numberofeventsdict = {}
    fname = ''
    for line in logf:
      line = line.rstrip()
      if line.count(prefix) and not line.count('Output File prefix'):
        fname = line.split()[-1].rstrip().rstrip("\0")
        numberofeventsdict[fname] = 0
      elif line.count("Record") and not line.count('Output Begin Run') :
        #print line
        val = line.split("=")[1].rstrip().lstrip()
        numberofeventsdict[fname] = int(val)
    
    self.log.verbose("numberofeventsdict dict: %s"%numberofeventsdict)   

    ##Now update the workflow_commons dict with the relation between filename and number of events: needed for the registerOutputData
    self.workflow_commons['file_number_of_event_relation'] = numberofeventsdict
    if self.listoutput:
      outputlist = []
      for f in numberofeventsdict.keys():
        item = {}
        item['outputFile'] = f
        item['outputPath'] = self.listoutput['outputPath']
        item['outputDataSE']= self.listoutput['outputDataSE']
        outputlist.append(item)
      self.step_commons['listoutput'] = outputlist
      
    #Not only the step_commons must be updated  
    if self.workflow_commons.has_key('ProductionOutputData'):
      proddata = self.workflow_commons['ProductionOutputData'].split(";")
      finalproddata = []
      this_split_data = ''
      for item in proddata:
        if not item.count(prefix):
          finalproddata.append(item)
        else:
          this_split_data = item
      path = os.path.dirname(this_split_data)
      for f in numberofeventsdict.keys():
        finalproddata.append(os.path.join(path,f))
      self.workflow_commons['ProductionOutputData']= ";".join(finalproddata)  
    
    self.log.info( "Status after the application execution is %s" % str( status ) )
    if status==2:
      self.log.info("Reached end of input file")
      status = 0
    return self.finalStatusReport(status)

