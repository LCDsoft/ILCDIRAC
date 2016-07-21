'''
Run LCSIM

Called by Job Agent. 

:since: Apr 7, 2010

:author: Stephane Poss
'''

__RCSID__ = "$Id$"

import os, shutil, types
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from ILCDIRAC.Workflow.Utilities.CompactMixin                import CompactMixin
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation    import getSoftwareFolder
from ILCDIRAC.Core.Utilities.PrepareOptionFiles              import prepareLCSIMFile, getNewLDLibs
from ILCDIRAC.Core.Utilities.resolvePathsAndNames            import resolveIFpaths, getProdFilename
from ILCDIRAC.Core.Utilities.PrepareLibs                     import removeLibc
from ILCDIRAC.Core.Utilities.FindSteeringFileDir             import getSteeringFileDirName

from DIRAC                                                   import S_OK, S_ERROR, gLogger

class LCSIMAnalysis(CompactMixin, ModuleBase):
  """Define the LCSIM analysis part of the workflow
  """
  def __init__(self):
    super(LCSIMAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "LCSIMAnalysis" )
    self.result = S_ERROR()
    self.sourcedir = ''
    self.SteeringFile = ''
    self.InputFile = []
    self.InputData = []
    self.outputREC = ""
    self.outputDST = ""
    self.aliasproperties = ''
    self.applicationName = 'LCSIM'
    self.eventstring = ['']
    self.extraparams = ''
    self.OutputFile = '' #Set in ModuleBase
    self.detectorModel = ''
    self.trackingstrategy = ''
    self.NumberOfEvents = -1
     
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.

    :return: S_OK()
    """
    
    if 'inputXML' in self.step_commons:
      self.SteeringFile = self.step_commons['inputXML']

    #TODO: Next is necessary for old interface, should be removed when old prods are archived.
    if 'outputREC' in self.step_commons:
      self.outputREC = self.step_commons['outputREC']
      
    if 'outputDST' in self.step_commons:
      self.outputDST = self.step_commons['outputDST']
      
    if 'inputSlcio' in self.step_commons:
      inputf = self.step_commons["inputSlcio"]
      if not type(inputf) == types.ListType:
        inputf = inputf.split(";")
      self.InputFile = inputf

    #FIXME: hardcode default detector model add check for detectorModel when submitting lcsim jobs!
      #because currently no detectormodel is required!
    if "IS_PROD" in self.workflow_commons:
      if not self.detectorModel:
        self.detectorModel = "clic_sid_cdr.zip"
      
    if 'ExtraParams' in self.step_commons:
      self.extraparams = self.step_commons['ExtraParams']    

    if 'IS_PROD' in self.workflow_commons:
      if self.workflow_commons["IS_PROD"]:
        #self.outputREC = getProdFilename(self.outputREC,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #self.outputDST = getProdFilename(self.outputDST,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                 int(self.workflow_commons["JOB_ID"]))
        #if 'MokkaOutput' in self.workflow_commons:
        #  self.InputFile = getProdFilename(self.workflow_commons["MokkaOutput"],int(self.workflow_commons["PRODUCTION_ID"]),
        #                                    int(self.workflow_commons["JOB_ID"]))
        if 'ProductionOutputData' in self.workflow_commons:
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_rec_"):
              self.outputREC = os.path.basename(obj)
            elif obj.lower().count("_dst_"):
              self.outputDST = os.path.basename(obj)
            elif obj.lower().count("_sim_"):
              self.InputFile = [os.path.basename(obj)]
        else:
          self.outputREC = getProdFilename(self.outputREC, int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          self.outputDST = getProdFilename(self.outputDST, int(self.workflow_commons["PRODUCTION_ID"]),
                                           int(self.workflow_commons["JOB_ID"]))
          if 'SLICOutput' in self.workflow_commons:
            self.InputFile = [getProdFilename(self.workflow_commons["SLICOutput"], 
                                              int(self.workflow_commons["PRODUCTION_ID"]),
                                              int(self.workflow_commons["JOB_ID"]))]

    if 'aliasproperties' in self.step_commons:
      self.aliasproperties = self.step_commons["aliasproperties"]

    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".slcio") > -1:
          self.InputFile.append(files)
    self.log.info("Input files to treat %s" % self.InputFile)      
    return S_OK('Parameters resolved')

  def runIt(self):
    """
    Called by JobAgent
    
    Execute the following:
      - prepend in the LD_LIBRARY_PATH any lib directory of any dependency (e.g. root)
      - prepare the list of files to run on
      - set the cacheDirectory and put in there the alias.properties
      - set the lcsim file using :any:`prepareLCSIMFile`
      - run java and catch the exit code

    :return: S_OK(), S_ERROR()
    """
    self.result = S_OK()
    if not self.platform:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      self.log.error("Failed to resolve input parameters:", self.result["Message"])
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('LCSIM should not proceed as previous step did not end properly')
    
    
    res = getSoftwareFolder(self.platform, self.applicationName, self.applicationVersion)
    if not res['OK']:
      self.log.error('LCSIM was not found in either the local area or shared area:', res['Message'])
      return res
    lcsim_name = res['Value']
    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = getNewLDLibs(self.platform, self.applicationName, self.applicationVersion)

    runonslcio = []
    if len(self.InputFile):
      res = resolveIFpaths(self.InputFile)
      if not res['OK']:
        self.setApplicationStatus('LCSIM: missing input slcio file')
        return S_ERROR('Missing slcio file!')
      runonslcio = res['Value']
    #for inputfile in inputfilelist:
    #  self.log.verbose("Will try using %s"%(os.path.basename(inputfile)))
    #  runonslcio.append(os.path.join(os.getcwd(),os.path.basename(inputfile)))


    retMod = self.downloadDetectorZip()
    if not retMod:
      return retMod

    ##Collect jar files to put in classspath
    jars = []
    if os.path.exists("lib"):
      for libs in os.listdir("lib"):
        if os.path.basename(libs).find(".jar") > 0:
          jars.append(os.path.abspath(os.path.join("lib", libs)))
      new_ld_lib_path = "./lib:%s" % new_ld_lib_path
      #Remove any libc remaining in .lib
      removeLibc("./lib")
    
    
    ###Define cache directory as local folder
    aliasproperties = os.path.basename(self.aliasproperties)
    cachedir = os.getcwd()
    if not os.path.isdir(os.path.join(cachedir, ".lcsim")):
      try:
        os.mkdir(os.path.join(cachedir, ".lcsim"))
      except OSError, x:
        self.log.error("Could not create .lcsim folder !", str(x))
    if os.path.exists(os.path.join(cachedir, ".lcsim")):
      lcsimfolder = os.path.join(cachedir, ".lcsim")
      if os.path.exists(aliasproperties):
        self.log.verbose("Copy alias.properties file in %s" % (lcsimfolder))
        shutil.copy(aliasproperties, os.path.join(lcsimfolder, aliasproperties))
      if os.path.exists(os.path.basename(self.detectorModel)):
        try:
          os.mkdir(os.path.join(lcsimfolder, "detectors"))
        except OSError, x:
          self.log.error("Could not create detectors folder !", str(x))
        if os.path.exists(os.path.join(lcsimfolder, "detectors")):
          self.log.verbose("Copy detector model.zip into the .lcsim/detectors folder")
          shutil.copy(os.path.basename(self.detectorModel), 
                      os.path.join(lcsimfolder, "detectors", os.path.basename(self.detectorModel)))
          
    paths = {}
    paths[os.path.basename(self.SteeringFile)] = os.path.basename(self.SteeringFile)
    paths[os.path.basename(self.trackingstrategy)] = os.path.basename(self.trackingstrategy)
    for myfile in paths.keys():  
      if len(myfile):
        #file = os.path.basename(file)
        if not os.path.exists(myfile):
          res =  getSteeringFileDirName(self.platform, self.applicationName, self.applicationVersion)
          if not res['OK']:
            self.log.error('Failed finding the steering file folder:', res["Message"])
            return res
          steeringfiledirname = res['Value']
          if os.path.exists(os.path.join(steeringfiledirname, myfile)):
            paths[myfile] = os.path.join(steeringfiledirname, myfile)
        if not os.path.exists(paths[myfile]):
          self.log.error('Failed finding file', paths[myfile])
          return S_ERROR("Could not find file %s" % paths[myfile])    
    self.SteeringFile = paths[os.path.basename(self.SteeringFile)]
    self.trackingstrategy = paths[os.path.basename(self.trackingstrategy)] 
    
    lcsimfile = "job_%s.lcsim" % self.STEP_NUMBER
    res = prepareLCSIMFile(self.SteeringFile, lcsimfile, self.NumberOfEvents,
                           self.trackingstrategy, runonslcio, jars, cachedir,
                           self.OutputFile, self.outputREC, self.outputDST, self.debug)
    if not res['OK']:
      self.log.error("Could not treat input lcsim file because %s" % res['Message'])
      return S_ERROR("Error creating lcsim file")
    else:
      self.log.verbose("File job.lcsim created properly")
    self.eventstring = [res['Value']]
    
    scriptName = 'LCSIM_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    if new_ld_lib_path:
      script.write("declare -x LD_LIBRARY_PATH=%s\n" % new_ld_lib_path)
    script.write("declare -x JAVALIBPATH=./\n")
    if os.path.exists("lib"):
      script.write("declare -x JAVALIBPATH=./lib\n")
    script.write('echo =========\n')
    script.write('echo java version :\n')
    script.write('java -version\n')
    script.write('env | sort >> localEnv.log\n')
    script.write('echo =========\n')    
    comm = "java -Xmx1536m -Xms256m -server -Djava.library.path=$JAVALIBPATH -Dorg.lcsim.cacheDir=%s -jar %s %s %s %s\n" % (cachedir, 
                                                                                                                            lcsim_name, 
                                                                                                                            self.extraparams, 
                                                                                                                            lcsimfile,
                                                                                                                            self.extraCLIarguments)
    self.log.info("Will run %s" % comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')    
    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % scriptName
    self.setApplicationStatus('LCSIM %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      return S_ERROR('%s did not produce the expected log' % (self.applicationName))

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is", str( status ) )

    return self.finalStatusReport(status)

