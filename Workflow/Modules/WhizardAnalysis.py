########################################################################
# $HeadURL$
########################################################################
'''
Whizard analysis module.

Called by Job Agent. 

@since: Sep 22, 2010

@author: S. Poss
'''

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                       import shellCall
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSoftwareFolder
from ILCDIRAC.Core.Utilities.ResolveDependencies           import resolveDepsTar
from ILCDIRAC.Core.Utilities.PrepareOptionFiles            import PrepareWhizardFile
from ILCDIRAC.Core.Utilities.PrepareOptionFiles            import PrepareWhizardFileTemplate, GetNewLDLibs
from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from ILCDIRAC.Core.Utilities.ProcessList                   import ProcessList
from ILCDIRAC.Core.Utilities.resolvePathsAndNames          import getProdFilename
from ILCDIRAC.Core.Utilities.PrepareLibs                   import removeLibc
from ILCDIRAC.Core.Utilities.GeneratorModels               import GeneratorModels
from ILCDIRAC.Core.Utilities.WhizardOptions                import WhizardOptions

from DIRAC import gLogger, S_OK, S_ERROR

import os, shutil, glob

class WhizardAnalysis(ModuleBase):
  """
  Specific Module to run a Whizard job.
  """
  def __init__(self):
    super(WhizardAnalysis, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.debug = True
    self.log = gLogger.getSubLogger( "WhizardAnalysis" )
    self.SteeringFile = ''
    self.OutputFile = ''
    self.NumberOfEvents = 1
    self.Lumi = 0
    self.applicationName = 'whizard'
    self.evttype = ""
    self.RandomSeed = 0
    self.energy = 3000.
    self.getProcessInFile = False
    self.rm = ReplicaManager()
    self.processlist = None
    self.jobindex = None
    self.parameters = {}
    self.susymodel = 0
    self.Model = ''
    self.genmodel = GeneratorModels()
    self.eventstring = ['! ', 'Fatal error:', 'PYSTOP', 'No matrix element available',
                        'Floating point exception', 'Event generation finished.', " n_events","luminosity", "  sum            "]
    self.excludeAllButEventString = False
    self.steeringparameters = ''
    self.options = None
    self.optionsdict = {}
    self.OptionsDictStr = ''
    self.GenLevelCutDictStr = ''
    self.genlevelcuts = {}
    self.willCut = False
    self.useGridFiles = False
    
    
  def obtainProcessList(self):
    """Internal function
    
    Get the process list from storage if whizard.in was not provided
    @return: S_OK(), S_ERROR()
    """
    
    res = self.ops.getValue("/ProcessList/Location", "")
    if not res:
      return S_ERROR("No process list found")
    processlistloc = res
    if not os.path.exists(os.path.basename(processlistloc)):
      res = self.rm.getFile(processlistloc)
      if not res['OK']:
        self.log.error('Could not get processlist: %s' % res['Message'])
        return res
    self.processlist = ProcessList(os.path.basename(processlistloc))
    return S_OK()
    
  def applicationSpecificInputs(self):
    """Resolve module input
    @return: S_OK()
    """

    if self.step_commons.has_key("Energy"):
      self.energy = self.step_commons["Energy"]
    self.parameters['ENERGY'] = self.energy

    if not self.RandomSeed:
      if self.step_commons.has_key("RandomSeed"):
        self.RandomSeed = self.step_commons['RandomSeed']
      elif self.jobID:
        self.RandomSeed = self.jobID
    if self.workflow_commons.has_key("IS_PROD") or self.workflow_commons.has_key("IS_DBD_GEN_PROD"):  
      self.RandomSeed = int(str(int(self.workflow_commons["PRODUCTION_ID"])) + str(int(self.workflow_commons["JOB_ID"])))  
    self.parameters['SEED'] = self.RandomSeed

    if self.step_commons.has_key('NbOfEvts'):
      self.NumberOfEvents = self.step_commons['NbOfEvts']
    self.parameters['NBEVTS'] = self.NumberOfEvents
      
    if self.step_commons.has_key('Lumi'):
      self.Lumi = self.step_commons['Lumi']
    self.parameters['LUMI'] = self.Lumi
      
    if self.step_commons.has_key('SusyModel'):
      self.susymodel = self.step_commons['SusyModel']
      
    if self.step_commons.has_key("InputFile"):
      self.SteeringFile = os.path.basename(self.step_commons["InputFile"])
      
    if self.step_commons.has_key("EvtType"):
      self.evttype = os.path.basename(self.step_commons["EvtType"])
    self.parameters['PROCESS'] = self.evttype
      
    if self.step_commons.has_key("JobIndex"):
      self.jobindex = self.step_commons["JobIndex"]

    if self.SteeringFile == "whizard.in":
      os.rename(self.SteeringFile, "whizardnew.in")
      self.SteeringFile = "whizardnew.in"
 
    if self.step_commons.has_key("parameters"):
      self.steeringparameters = self.step_commons["parameters"]
    listofparams = self.steeringparameters.split(";")
    for param in listofparams:
      if param.count("="):
        self.parameters[param.split("=")[0]] = param.split("=")[1]
 
    if self.OptionsDictStr:
      self.log.info("Will use whizard.in definition from WhizardOptions.")
      try:
        self.optionsdict = eval(self.OptionsDictStr)
        if not self.optionsdict.has_key('integration_input'):
          self.optionsdict['integration_input'] = {}
        if not self.optionsdict['integration_input'].has_key('seed'):
          self.optionsdict['integration_input']['seed'] = int(self.RandomSeed)
        if self.optionsdict.has_key('process_input'):
          if self.optionsdict['process_input'].has_key('sqrts'):
            self.energy = self.optionsdict['process_input']['sqrts']
      except:
        return S_ERROR("Could not convert string to dictionary for optionsdict")
    
    if self.GenLevelCutDictStr:
      self.log.info("Found generator level cuts")
      try:
        self.genlevelcuts = eval(self.GenLevelCutDictStr)
      except:
        return S_ERROR("Could not convert the generator level cuts back to dictionary")  
    
    if not len(self.SteeringFile) and not self.optionsdict:
      self.getProcessInFile = True
 
    if self.workflow_commons.has_key("IS_PROD"):
      if self.workflow_commons["IS_PROD"] and not self.willCut:
        #self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            if obj.lower().count("_gen_"):
              self.OutputFile = os.path.basename(obj)
              break
        else:
          #This is because most likely there is stdhepcut running after
          self.OutputFile = "willcut.stdhep" 
          
          #getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
          #                                  int(self.workflow_commons["JOB_ID"]))
          
    if self.workflow_commons.has_key("IS_DBD_GEN_PROD"):
      if self.workflow_commons["IS_DBD_GEN_PROD"]:
        #self.OutputFile = getProdFilename(self.OutputFile,int(self.workflow_commons["PRODUCTION_ID"]),
        #                                  int(self.workflow_commons["JOB_ID"]))
        if self.workflow_commons.has_key('ProductionOutputData'):
          outputlist = self.workflow_commons['ProductionOutputData'].split(";")
          for obj in outputlist:
            self.OutputFile = os.path.basename(obj)
            break
        else:
          self.OutputFile = getProdFilename(self.OutputFile, int(self.workflow_commons["PRODUCTION_ID"]),
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
    self.result = self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('Whizard should not proceed as previous step did not end properly')

    #if self.debug:
    #  self.excludeAllButEventString = False

    whizardDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig, self.applicationName, 
                                                                          self.applicationVersion), '')
    if not whizardDir:
      self.log.error('Could not get info from CS')
      self.setApplicationStatus('Failed finding info from CS')
      return S_ERROR('Failed finding info from CS')
    whizardDir = whizardDir.replace(".tgz", "").replace(".tar.gz", "")
    res = getSoftwareFolder(whizardDir)
    if not res['OK']:
      self.log.error("Failed getting software folder", res['Message'])
      self.setApplicationStatus('Failed finding software')
      return res
    mySoftDir = res['Value']

    ###Remove libc
    removeLibc(mySoftDir + "/lib")

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = GetNewLDLibs(self.systemConfig, self.applicationName, self.applicationVersion)
    #Don't forget to prepend the application's libs
    new_ld_lib_path = mySoftDir + "/lib:" + new_ld_lib_path
    ### Resolve dependencies (look for beam_spectra)
    deps = resolveDepsTar(self.systemConfig, self.applicationName, self.applicationVersion)
    path_to_beam_spectra = ""
    path_to_gridfiles = ""
    for dep in deps:
      depfolder = dep.replace(".tgz", "").replace(".tar.gz", "")
      res = getSoftwareFolder(depfolder)
      if not res['OK']:
        self.log.error("Failed getting software folder", res['Message'])
        self.setApplicationStatus('Failed finding software')
        return res
      depfolder = res['Value']
      if depfolder.count("beam_spectra"):
        path_to_beam_spectra = depfolder
      elif depfolder.count("gridfiles"):
        path_to_gridfiles = depfolder

    ##Env variables needed to run whizard: avoids hard coded locations
    os.environ['LUMI_LINKER'] = path_to_beam_spectra + "/lumi_linker_000"
    os.environ['PHOTONS_B1'] = path_to_beam_spectra + "/photons_beam1_linker_000"
    os.environ['PHOTONS_B2'] = path_to_beam_spectra + "/photons_beam2_linker_000"
    os.environ['EBEAM'] = path_to_beam_spectra + "/ebeam_in_linker_000"
    os.environ['PBEAM'] = path_to_beam_spectra + "/pbeam_in_linker_000"

    os.environ['LUMI_EE_LINKER'] = path_to_beam_spectra + "/lumi_ee_linker_000"
    os.environ['LUMI_EG_LINKER'] = path_to_beam_spectra + "/lumi_eg_linker_000"
    os.environ['LUMI_GE_LINKER'] = path_to_beam_spectra + "/lumi_ge_linker_000"
    os.environ['LUMI_GG_LINKER'] = path_to_beam_spectra + "/lumi_gg_linker_000"
    

    list_of_gridfiles = []
    if path_to_gridfiles and self.useGridFiles:
      tmp_list_of_gridfiles = [os.path.join(path_to_gridfiles, item) for item in os.listdir(path_to_gridfiles)]
      gridfilesfound = False
      for path in tmp_list_of_gridfiles:
        if os.path.isdir(path) and path.count(str(self.energy)): #Here look for a sub directory for the energy related grid files
          list_of_gridfiles = [os.path.join(path, item) for item in os.listdir(path)]
          gridfilesfound = True
          self.log.info('Found grid files specific for energy %s' % self.energy)
          break
      if not gridfilesfound:
        self.log.info("Will use generic grid files found, hope the energy is set right")
        list_of_gridfiles = [item for item in glob.glob(os.path.join(path_to_gridfiles, "*.grb")) + glob.glob(os.path.join(path_to_gridfiles, "*.grc"))]
         
    template = False
    if self.SteeringFile.count("template"):
      template = True
    ## Get from process file the proper whizard.in file
    if self.getProcessInFile:
      whizardin = ""
      res = self.obtainProcessList()
      if not res['OK']:
        self.log.error("Could not obtain process list")
        self.setApplicationStatus('Failed getting processlist')
        return res
      whizardin = self.processlist.getInFile(self.evttype)
      if not whizardin:
        self.log.error("Whizard input file was not found in process list, cannot proceed")
        self.setApplicationStatus('Whizard input file was not found')
        return S_ERROR("Error while resolving whizard input file")
      if whizardin.count("template"):
        template = True
      try:
        shutil.copy("%s/%s" % (mySoftDir, whizardin), "./whizardnew.in")
        self.SteeringFile = "whizardnew.in"
      except:
        self.log.error("Could not copy %s from %s" % (whizardin, mySoftDir))
        self.setApplicationStatus('Failed getting whizard.in file')
        return S_ERROR("Failed to obtain %s" % whizardin)

    ##Check existence of Les Houches input file
    leshouchesfiles = ''
    if not os.path.exists("LesHouches.msugra_1.in"):
      if self.susymodel:
        if self.susymodel == 1:
          if os.path.exists("%s/LesHouches_slsqhh.msugra_1.in" % (mySoftDir)):
            leshouchesfiles = "%s/LesHouches_slsqhh.msugra_1.in" % (mySoftDir)
        if self.susymodel == 2:
          if os.path.exists("%s/LesHouches_chne.msugra_1.in" % (mySoftDir)):
            leshouchesfiles = "%s/LesHouches_chne.msugra_1.in" % (mySoftDir)
      if self.Model:
        if self.genmodel.hasModel(self.Model)['OK']:
          if self.genmodel.getFile(self.Model)['OK']:
            if os.path.exists("%s/%s" % (mySoftDir, self.genmodel.getFile(self.Model)['Value'])):
              leshouchesfiles = "%s/%s" % (mySoftDir, self.genmodel.getFile(self.Model)['Value'])
            else:
              self.log.error("Request LesHouches file is missing, cannot proceed")
              self.setApplicationStatus("LesHouches file missing")
              return S_ERROR("The LesHouches file was not found. Probably you are using a wrong version of whizard.") 
          else:
            self.log.warn("No file found attached to model %s" % self.Model)
        else:
          self.log.error("Model undefined:",self.Model)
          self.setApplicationStatus("Model undefined")
          return S_ERROR("No Model %s defined" % self.Model)
    else:
      leshouchesfiles = "LesHouches.msugra_1.in"

    outputfilename = self.evttype
    if self.jobindex:
      outputfilename = "%s_%s" % (outputfilename, self.jobindex)
    
          
    if self.optionsdict:
      self.log.info("Using: %s" % self.optionsdict)
      self.options = WhizardOptions(self.Model)
      res = self.options.changeAndReturn(self.optionsdict)
      if not res['OK']:
        return res
      res = self.options.toWhizardDotIn("whizard.in")
    elif not template:  
      res = PrepareWhizardFile(self.SteeringFile, outputfilename, self.energy, 
                               self.RandomSeed, self.NumberOfEvents, self.Lumi, 
                               "whizard.in")
    else:
      res = PrepareWhizardFileTemplate(self.SteeringFile, outputfilename, 
                                       self.parameters, "whizard.in")
    if not res['OK']:
      self.log.error('Something went wrong with input file generation')
      self.setApplicationStatus('Whizard: something went wrong with input file generation')
      return S_ERROR('Something went wrong with whizard.in file generation')
    foundproceesinwhizardin = res['Value']
    
    scriptName = 'Whizard_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n' % mySoftDir)
    script.write('declare -x LD_LIBRARY_PATH=%s\n' % new_ld_lib_path)
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    script.write('echo Printing content of whizard.in \n')
    script.write('cat whizard.in\n')
    script.write('echo =============================\n')
    script.write('cp  %s/whizard.mdl ./\n' % mySoftDir)
    if leshouchesfiles:
      if not leshouchesfiles == 'LesHouches.msugra_1.in':
        script.write('cp %s ./LesHouches.msugra_1.in\n' % (leshouchesfiles))
      script.write('ln -s LesHouches.msugra_1.in fort.71\n')
    if len(list_of_gridfiles):
      for gridfile in list_of_gridfiles:
        script.write('cp %s ./\n' % (gridfile))
    script.write('cp %s/whizard.prc ./\n' % mySoftDir)
    if self.genlevelcuts:
      res = self.makeWhizardDotCut1(self.genlevelcuts)
      if not res['OK']:
        script.close()
        self.log.error("Could not create the cut1 file")
        return S_ERROR("Could not create the cut1 file")
    script.write('echo =============================\n')
    script.write('echo Printing content of whizard.prc \n')
    script.write('cat whizard.prc\n')
    script.write('echo =============================\n')
    extracmd = ""
    if not self.debug:
      extracmd = "2>/dev/null" 
      
    comm = ""
    if foundproceesinwhizardin:
      comm = 'whizard --simulation_input \'write_events_file = \"%s\"\' %s\n' % (outputfilename, extracmd)
    else:
      comm = 'whizard --process_input \'process_id =\"%s\"\' --simulation_input \'write_events_file = \"%s\"\' %s\n' % (self.evttype, 
                                                                                                                        outputfilename, 
                                                                                                                        extracmd)
    self.log.info("Will run %s" % comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    
    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)
    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)    
    self.setApplicationStatus('Whizard %s step %s' %(self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit=209715200)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    if not self.result['OK']:
      self.log.error("Failed with error %s" % self.result['Message'])
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' % (self.applicationName))
    lumi = ''
    message = ""
    success = False
    ###Analyse log file
    logfile = file(self.applicationLog)
    for line in logfile:
      if line.count('! Event sample corresponds to luminosity'):
        elems = line.split()
        lumi = elems[-1]
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
        status = 1
        message = line
        break
      elif line.count("Event generation finished."):
        success = True
      else:
        status = 0
    if success:
      status = 0
    else:
      status = 1
    self.log.info('The sample generated has an equivalent luminosity of %s' % lumi)
    if lumi:
      self.workflow_commons['Luminosity'] = float(lumi)
    else:
      status = 1  
    
    ##Now care for the cross sections
    info = {}
    res = self.options.getAsDict()
    if os.path.exists("whizard.out") and res['OK']:
      full_opts_dict = res['Value']
      processes = full_opts_dict['process_input']['process_id'].split()
      info = {}
      info['xsection'] = {}
      processes.append('sum')
      inf = open("whizard.out", "r")
      for line in inf:
        line = line.rstrip()
        for process in processes:
          if not process:
            continue
          if line.count("   %s            " % process):
            info['xsection'][process] = {}
            line = line.lstrip()
            crosssection = line.split()[1]
            err_crosssection = line.split()[2]
            frac = line.split()[4]
            info['xsection'][process]['xsection'] = float(crosssection)
            info['xsection'][process]['err_xsection'] = float(err_crosssection)
            info['xsection'][process]['fraction'] = float(frac)

      inf.close()
    if info:
      if 'Info' not in self.workflow_commons:
        self.workflow_commons['Info'] = info
      else:
        self.workflow_commons['Info'].update(info)

    self.log.info( "Status after the application execution is %s" % str( status ) )

    messageout = 'Whizard %s Successful' % (self.applicationVersion)
    failed = False
    if status != 0:
      self.log.error( "Whizard execution completed with errors:" )
      failed = True
    else:
      self.log.info( "Whizard execution completed successfully")
      ###Deal with output file
      if len(self.OutputFile):
        if os.path.exists(outputfilename + ".001.stdhep"):
          ofnames = glob.glob('*.stdhep')
          if len(ofnames) > 1:
            basename = self.OutputFile.split(".stdhep")[0]
            i = 0
            for f in ofnames:
              i += 1
              name = basename + "_" + str(i) + ".stdhep"
              os.rename(f, name)
          else:
            os.rename(outputfilename + ".001.stdhep", self.OutputFile)    
        else:
          self.log.error( "Whizard execution did not produce a stdhep file" )
          self.setApplicationStatus('Whizard %s Failed to produce STDHEP file' % (self.applicationVersion))
          messageout = 'Whizard Failed to produce STDHEP file'
          if not self.ignoreapperrors:
            return S_ERROR(messageout)

    if failed == True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( message )
      self.setApplicationStatus('%s Exited With Status %s' % (self.applicationName, status))
      self.log.error('Whizard Exited With Status %s' % (status))
      messageout = 'Whizard Exited With Status %s' % (status)
      if not self.ignoreapperrors:
        return S_ERROR(messageout)
    else:
      self.setApplicationStatus(messageout)
    return S_OK( { "OutputFile": self.OutputFile } )

  def makeWhizardDotCut1(self, cutdict):
    """ When users need whizard cuts, this is called to prepare the file
    @return: S_OK()
    """
    cutf = file("whizard.cut1","w")
    for key, values in cutdict.items():
      cutf.write("process %s\n"%key)
      for val in values:
        cutf.write("  %s\n"%val)
    cutf.close()
    return S_OK()
    