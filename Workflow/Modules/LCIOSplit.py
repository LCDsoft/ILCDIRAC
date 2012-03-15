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

import DIRAC
import os
import sys

class LCIOSplit(ModuleBase):
  """ LCIO cdoncatenate module
  """
  def __init__(self):

    ModuleBase.__init__(self)

    self.STEP_NUMBER = ''
    self.log         = gLogger.getSubLogger( "LCIOSplit" )
    self.result      = S_ERROR()
    self.nbEventsPerSlice = 0
    self.InputFile = ''
    # Step parameters
    self.prod_outputdata = []
    self.applicationName = "lcio"
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
            if obj.lower().count("_sim_") or obj.lower().count("_rec_") or obj.lower().count("_dst_"):
              self.OutputFile = os.path.basename(obj)
        else:
          self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
                                              int(self.workflow_commons["JOB_ID"]))
          
    if len(self.InputFile)==0 and not len(self.InputData)==0:
      inputfiles = self.InputData.split(";")
      for files in inputfiles:
        if files.lower().find(".slcio")>-1:
          self.InputFile += files+";"
      self.InputFile = self.InputFile.rstrip(";")
      
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

    if not os.environ.has_key("LCIO"):
      self.log.error("Environment variable LCIO was not defined, cannot do anything")
      return S_ERROR("Environment variable LCIO was not defined, cannot do anything")

    if self.InputFile:
      inputfilelist = self.InputFile.split(";")
      res = resolveIFpaths(inputfilelist)
      if not res['OK']:
        self.setApplicationStatus('LCSIM: missing input slcio file')
        return S_ERROR('Missing slcio file!')
      runonslcio = res['Value'][0]
    else:
      return S_OK("No files found to process")
    # removeLibc

    removeLibc( os.path.join( os.environ["LCIO"], "lib" ) )

    # Setting up script

    LD_LIBRARY_PATH = os.path.join( "$LCIO", "lib" )
    if os.environ.has_key('LD_LIBRARY_PATH'):
      LD_LIBRARY_PATH += ":" + os.environ['LD_LIBRARY_PATH']

    PATH = "$LCIO/bin"
    if os.environ.has_key('PATH'):
      PATH += ":" + os.environ['PATH']

    scriptContent = """
#!/bin/sh

################################################################################
# Dynamically generated script by LCIOConcatenate module                       #
################################################################################

declare -x LD_LIBRARY_PATH=%s
declare -x PATH=%s

lcio split -i %s -n %s

exit $?

""" %(
    LD_LIBRARY_PATH,
    PATH,
    runonslcio,
    self.nbEventsPerSlice
)

    # Write script to file

    scriptPath = 'LCIOSplit_%s_Run_%s.tcl' %( self.applicationVersion, self.STEP_NUMBER )

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

    self.setApplicationStatus( 'LCIOSplit %s step %s' %( self.applicationVersion, self.STEP_NUMBER ) )
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
    baseinputfilename = os.path.basename(runonslcio).split(".slcio")[0]
    output_file_base_name = ''
    if self.OutputFile:
      output_file_base_name = self.OutputFile.split('.slcio')[0]
    if self.listoutput:
      output_file_base_name = self.listoutput['outputFile'].split('.slcio')[0]
    numberofeventsdict = {}
    fname = ''
    producedfiles = []
    for line in logf:
      line = line.rstrip()
      if line.count(baseinputfilename):
        #First, we need to rename those guys
        current_file = os.path.basename(line).replace(".slcio","")
        current_file_extension = current_file.replace(baseinputfilename,"")
        newfile = output_file_base_name+current_file_extension+".slcio"
        os.rename(line,newfile)
        fname = newfile
        numberofeventsdict[fname] = 0
        producedfiles.append("fname")
      elif line.count("events"):
        numberofeventsdict[fname] = int(line.split()[0])
       

    ##Now update the workflow_commons dict with the relation between filename and number of events: needed for the registerOutputData
    self.workflow_commons['file_number_of_event_relation'] = numberofeventsdict
    if self.listoutput:
      outputlist = []
      for file in numberofeventsdict.keys():
        item = {}
        item['outputFile'] = file
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
        if not item.count(output_file_base_name):
          finalproddata.append(item)
        else:
          this_split_data = item
      path = os.path.dirname(this_split_data)
      for file in numberofeventsdict.keys():
        finalproddata.append(os.path.join(path,file))
      self.workflow_commons['ProductionOutputData']= ";".join(finalproddata)  
    
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)

