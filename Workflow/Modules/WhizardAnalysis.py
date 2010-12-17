
'''
Whizard analysis module. Called by Job Agent. 

Created on Sep 22, 2010

@author: sposs
'''
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareWhizardFile
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareWhizardFileTemplate
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from ILCDIRAC.Core.Utilities.ProcessList            import ProcessList
from ILCDIRAC.Core.Utilities.resolveOFnames import getProdFilename

from DIRAC import gLogger,S_OK,S_ERROR, gConfig

import os,re,sys, shutil

class WhizardAnalysis(ModuleBase):
  """
  Specific Module to run a Whizard job.
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.debug = True
    self.log = gLogger.getSubLogger( "WhizardAnalysis" )
    self.result = S_ERROR()
    self.inFile = ''
    self.stdhepFile = ''
    self.NumberOfEvents = 1
    self.Lumi = 0
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion = ''
    self.applicationName = 'whizard'
    self.evttype = ""
    self.randomseed = 0
    self.energy = 3000
    self.getProcessInFile = False
    self.rm = ReplicaManager()
    self.processlist = None
    self.jobindex = None
    self.debug = False
    self.outputFile = ''
    self.parameters = {}
    self.susymodel = 0

  def obtainProcessList(self):
    """Internal function
    
    Get the process list from storage if whizard.in was not provided
    @return: S_OK(), S_ERROR()
    """
    res = gConfig.getOption("/Operations/ProcessList/Location","")
    if not res['OK']:
      return res
    processlistloc = res['Value']
    res = self.rm.getFile(processlistloc)
    if not res['OK']:
      return res
    self.processlist = ProcessList(os.path.basename(processlistloc))
    return S_OK()
    
  def resolveInputVariables(self):
    """Resolve module input
    @return: S_OK()
    """
    
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
      
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']
 
    if self.step_commons.has_key("Energy"):
      self.energy = self.step_commons["Energy"]
      self.parameters['ENERGY']=self.energy
 
    if self.step_commons.has_key("RandomSeed"):
      self.randomseed = self.step_commons['RandomSeed']
    elif self.workflow_commons.has_key("IS_PROD"):  
      self.randomseed = int(str(int(self.workflow_commons["PRODUCTION_ID"]))+str(int(self.workflow_commons["JOB_ID"])))
    elif self.jobID:
      self.randomseed = self.jobID
    self.parameters['SEED'] = self.randomseed

    if self.step_commons.has_key('NbOfEvts'):
      self.NumberOfEvents = self.step_commons['NbOfEvts']
      self.parameters['NBEVTS']=self.NumberOfEvents
      
    if self.step_commons.has_key('Lumi'):
      self.Lumi = self.step_commons['Lumi']
      self.parameters['LUMI']=self.Lumi
      
    if self.step_commons.has_key('SusyModel'):
      self.susymodel = self.step_commons['SusyModel']
      
    if self.step_commons.has_key("InputFile"):
      self.inFile = os.path.basename(self.step_commons["InputFile"])

    if not len(self.inFile):
      self.getProcessInFile = True
      
    if self.step_commons.has_key("EvtType"):
      self.evttype = os.path.basename(self.step_commons["EvtType"])
      self.parameters['PROCESS']=self.evttype
    if self.step_commons.has_key("JobIndex"):
      self.jobindex = self.step_commons["JobIndex"]
      
    if self.step_commons.has_key("debug"):
      self.debug = self.step_commons["debug"]

    if self.inFile == "whizard.in":
      os.rename(self.inFile, "whizardnew.in")
      self.inFile = "whizardnew.in"
    if self.step_commons.has_key("outputFile"):
      self.outputFile = self.step_commons["outputFile"]
 
    if self.step_commons.has_key("parameters"):
      params= self.step_commons["parameters"]
      listofparams= params.split(";")
      for param in listofparams:
        self.parameters[param.split("=")[0]]=param.split("=")[1]
 
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"]:
        #self.outputFile = getProdFilename(self.outputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_gen_"):
              self.outputFile = os.path.basename(obj)
              break
        else:
          self.outputFile = getProdFilename(self.outputFile,int(self.workflow_commons["PRODUCTION_ID"]),
                                            int(self.workflow_commons["JOB_ID"]))
 
      
    return S_OK()

  def execute(self):
    """ Called by Agent
    
    Executes the following
      - resolve input variables
      - resolve installation location
      - resolve dependencies location (beam_spectra)
      - get processlist if needed
      - define output file name
      - prepare whizard.in
      - make magic
      
    @return: S_OK(), S_ERROR()
    """
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('Whizard should not proceed as previous step did not end properly')

    whizardDir = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"whizard",self.applicationVersion),'')
    whizardDir = whizardDir.replace(".tgz","").replace(".tar.gz","")
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,whizardDir)):
      mySoftwareRoot = localArea
    elif os.path.exists('%s%s%s' %(sharedArea,os.sep,whizardDir)):
      mySoftwareRoot = sharedArea
    else:
      self.setApplicationStatus('Whizard: Could not find neither local area not shared area install')
      return S_ERROR('Missing installation of Whizard!')
    mySoftDir = os.path.join(mySoftwareRoot,whizardDir)
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH']=mySoftDir+"/lib:"+os.environ['LD_LIBRARY_PATH']
    else:
      os.environ['LD_LIBRARY_PATH']=mySoftDir+"/lib"
    ### Resolve dependencies (look for beam_spectra)
    deps = resolveDepsTar(self.systemConfig,"whizard",self.applicationVersion)
    path_to_beam_spectra = ""
    path_to_gridfiles = ""
    for dep in deps:
      if os.path.exists(os.path.join(mySoftwareRoot,dep.replace(".tgz","").replace(".tar.gz",""))):
        depfolder = dep.replace(".tgz","").replace(".tar.gz","")
        if depfolder.count("beam_spectra"):
          path_to_beam_spectra=os.path.join(mySoftwareRoot,depfolder)
        elif depfolder.count("gridfiles"):
          path_to_gridfiles=os.path.join(mySoftwareRoot,depfolder)

    ##Env variables needed to run whizard: avoids hard coded locations
    os.environ['LUMI_LINKER'] = path_to_beam_spectra+"/lumi_linker_000"
    os.environ['PHOTONS_B1'] = path_to_beam_spectra+"/photons_beam1_linker_000"
    os.environ['PHOTONS_B2'] = path_to_beam_spectra+"/photons_beam2_linker_000"
    os.environ['EBEAM'] = path_to_beam_spectra+"/ebeam_in_linker_000"
    os.environ['PBEAM'] = path_to_beam_spectra+"/pbeam_in_linker_000"

    list_of_gridfiles = []
    if path_to_gridfiles:
      list_of_gridfiles = os.listdir(path_to_gridfiles)
      
    template=False
    if self.inFile.count("template"):
      template=True
    ## Get from process file the proper whizard.in file
    if self.getProcessInFile:
      whizardin = ""
      res = self.obtainProcessList()
      if not res['OK']:
        self.log.error("Could not obtain process list")
        return res
      whizardin = self.processlist.getInFile(self.evttype)
      if not whizardin:
        self.log.error("Whizard input file was not found in process list, cannot proceed")
        return S_ERROR("Error while resolving whizard input file")
      if whizardin.count("template"):
        template=True
      try:
        shutil.copy("%s/%s"%(mySoftDir,whizardin), "./whizardnew.in")
        self.inFile = "whizardnew.in"
      except:
        self.log.error("Could not copy %s from %s"%(whizardin,mySoftDir))
        return S_ERROR("Failed to obtain %s"%whizardin)

    ##Check existence of Les Houches input file
    leshouchesfiles = False
    if self.susymodel:
      if self.susymodel==1:
        if os.path.exists("%s/LesHouches_slsqhh.msugra_1.in"%(mySoftDir)):
          leshouchesfiles = True
      if self.susymodel==2:
        if os.path.exists("%s/LesHouches_chne.msugra_1.in"%(mySoftDir)):
          leshouchesfiles = True
          

    outputfilename = self.evttype
    if self.jobindex:
      outputfilename = "%s_%s"%(outputfilename,self.jobindex)
    if not template:  
      res = PrepareWhizardFile(self.inFile,outputfilename,self.energy,self.randomseed,self.NumberOfEvents,self.Lumi,"whizard.in")
    else:
      res = PrepareWhizardFileTemplate(self.inFile,outputfilename,self.parameters,"whizard.in")
    if not res['OK']:
      self.log.error('Something went wrong with input file generation')
      self.setApplicationStatus('Whizard: something went wrong with input file generation')
      return S_ERROR('Something went wrong with whizard.in file generation')
    foundproceesinwhizardin = res['Value']
    
    scriptName = 'Whizard_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n'%mySoftDir)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    script.write('echo Printing content of whizard.in \n')
    script.write('cat whizard.in\n')
    script.write('echo =============================\n')
    script.write('ln -s %s/whizard.mdl\n'%mySoftDir)
    if leshouchesfiles:
      if self.susymodel==1:
        script.write('cp %s/LesHouches_slsqhh.msugra_1.in ./LesHouches.msugra_1.in\n'%mySoftDir)
      if self.susymodel==2:
        script.write('cp %s/LesHouches_chne.msugra_1.in ./LesHouches.msugra_1.in\n'%mySoftDir)
      script.write('ln -s LesHouches.msugra_1.in fort.71\n')
    if len(list_of_gridfiles):
      for gridfile in list_of_gridfiles:
        script.write('ln -s %s/%s\n'%(path_to_gridfiles,gridfile))
    script.write('ln -s %s/whizard.prc\n'%mySoftDir)
    script.write('ln -s %s/whizard.cut1\n'%mySoftDir)
    script.write('echo =============================\n')
    script.write('echo Printing content of whizard.prc \n')
    script.write('cat whizard.prc\n')
    script.write('echo =============================\n')
    extracmd = ""
    if not self.debug:
      extracmd = "2>/dev/null" 
      
    comm = ""
    if foundproceesinwhizardin:
      comm = 'whizard --simulation_input \'write_events_file = \"%s\"\' %s\n'%(outputfilename,extracmd)
    else:
      comm= 'whizard --process_input \'process_id =\"%s\"\' --simulation_input \'write_events_file = \"%s\"\' %s\n'%(self.evttype,outputfilename,extracmd)
    self.log.info("Will run %s"%comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)
    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %(scriptName)    
    self.setApplicationStatus('Whizard %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    message = ""
    ###Analyse log file
    logfile = file(self.applicationLog)
    for line in logfile:
      if line.count("*** Fatal error:"):
        status = 1
        message = line
        break
      elif line.count("PYSTOP"):
        status = 1
        message = line
        break
      elif line.count("No matrix element available"):
        status = 1
        message = line
        break
      elif line.count("Floating point exception"):
        status=1
        message=line
        break
      else:
        status = 0

    #stdOutput = resultTuple[1]
    #stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )
    failed = False
    if status != 0:
      self.log.error( "Whizard execution completed with errors:" )
      failed = True
    else:
      self.log.info( "Whizard execution completed successfully")
      ###Deal with output file
      if len(self.outputFile):
        if os.path.exists(outputfilename+".001.stdhep"):
          os.rename(outputfilename+".001.stdhep", self.outputFile)
      

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( message )
      self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('Whizard Exited With Status %s' %(status))
      return S_ERROR('Whizard Exited With Status %s' %(status))

    self.setApplicationStatus('Whizard %s Successful' %(self.applicationVersion))
    return S_OK('Whizard %s Successful' %(self.applicationVersion))
    
  def redirectLogOutput(self, fd, message):
    """Catch the output from the application
    """
    sys.stdout.flush()
    if message:
      print message
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message
      