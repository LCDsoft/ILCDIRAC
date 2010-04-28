'''
ILCDIRAC.Workflow.Modules.LCSIMAnalysis Called by Job Agent. 

@since: Apr 7, 2010

@author: Stephane Poss
'''
import os, sys, re, shutil
from DIRAC.Core.Utilities.Subprocess                      import shellCall
#from DIRAC.Core.DISET.RPCClient                           import RPCClient
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import LocalArea,SharedArea
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import PrepareLCSIMFile
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDepsTar
from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

class LCSIMAnalysis(ModuleBase):
  """Define the LCSIM analysis part of the workflow
  """
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "LCSIMAnalysis" )
    self.result = S_ERROR()
    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationVersion=''
    self.sourcedir = ''
    self.xmlfile = ''
    self.inputSLCIO = ''
    self.aliasproperties = ''
    self.debug = False
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
     
  def resolveInputVariables(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key('inputXML'):
      self.xmlfile = self.step_commons['inputXML']
    if self.step_commons.has_key('lcsimFile'):
      self.xmlfile = self.step_commons['lcsimFile']
    if self.step_commons.has_key("inputSlcio"):
      self.inputSLCIO = self.step_commons["inputSlcio"]
    if self.step_commons.has_key("aliasproperties"):
      self.aliasproperties = self.step_commons["aliasproperties"]
    if self.step_commons.has_key('debug'):
      self.debug =  self.step_commons['debug']
      
    return S_OK('Parameters resolved')

  def execute(self):
    """
    Called by JobAgent
    
    Execute the following:
      - prepend in the LD_LIBRARY_PATH any lib directory of any dependency (e.g. root)
      - put the aliasproperties file in $HOME/.lcisim, where HOME is either the real home or the software directory if real home is not defined
      - prepare the list of files to run on
      - set the cacheDirectory and put in there the alias.properties
      - set the lcsim file using L{PrepareLCSIMFile}
      - run java and catch the exit code
    @return: S_OK(), S_ERROR()
    """
    self.result =self.resolveInputVariables()
    if not self.systemConfig:
      self.result = S_ERROR( 'No ILC platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )
    if not self.result['OK']:
      return self.result
    
    #look for lcsim filename
    lcsim_name = gConfig.getValue('/Operations/AvailableTarBalls/%s/%s/%s/TarBall'%(self.systemConfig,"lcsim",self.applicationVersion),'')
    if not lcsim_name:
      self.log.error("Could not find lcsim file name from CS")
      return S_ERROR("Could not find lcsim file name from CS")
    
    mySoftwareRoot = ''
    localArea = LocalArea()
    sharedArea = SharedArea()
    if os.path.exists('%s%s%s' %(localArea,os.sep,lcsim_name)):
      mySoftwareRoot = localArea
    if os.path.exists('%s%s%s' %(sharedArea,os.sep,lcsim_name)):
      mySoftwareRoot = sharedArea
    if not mySoftwareRoot:
      self.log.error('Directory %s was not found in either the local area %s or shared area %s' %(lcsim_name,localArea,sharedArea))
      return S_ERROR('Failed to discover software')

    ### Resolve dependencies
    deps = resolveDepsTar(self.systemConfig,"lcsim",self.applicationVersion)
    for dep in deps:
      if os.path.exists(os.path.join(mySoftwareRoot,dep.rstrip(".tgz").rstrip(".tar.gz"))):
        depfolder = dep.rstrip(".tgz").rstrip(".tar.gz")
        if os.path.exists(os.path.join(mySoftwareRoot,depfolder,"lib")):
          self.log.verbose("Found lib folder in %s"%(depfolder))
          if os.environ.has_key("LD_LIBRARY_PATH"):
            os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")+":%s"%os.environ["LD_LIBRARY_PATH"]
          else:
            os.environ["LD_LIBRARY_PATH"] = os.path.join(mySoftwareRoot,depfolder,"lib")
    

    runonslcio = []
    inputfilelist = self.inputSLCIO.split(";")
    for inputfile in inputfilelist:
      self.log.verbose("Will try using %s"%(os.path.basename(inputfile)))
      runonslcio.append(os.path.join(os.getcwd(),os.path.basename(inputfile)))


    ##Collect jar files to put in classspath
    jars = []
    if os.path.exists("lib"):
      for libs in os.listdir("lib"):
        if os.path.basename(libs).find(".jar")>0:
          jars.append(os.path.abspath(os.path.join("lib",libs)))
      os.environ['LD_LIBRARY_PATH']= "./lib:%s"%(os.environ['LD_LIBRARY_PATH'])

    ###Define cache directory as local folder
    aliasproperties = os.path.basename(self.aliasproperties)
    cachedir = os.getcwd()
    try:
      os.mkdir(os.path.join(cachedir,".lcsim"))
    except:
      self.log.error("Could not create .lcsim folder !")
    if os.path.exists(os.path.join(cachedir,".lcsim")):
      self.log.verbose("Copy alias.properties file in %s"%(os.path.join(cachedir,".lcsim")))
      shutil.copy(aliasproperties,os.path.join(cachedir,".lcsim",aliasproperties))
          
    lcsimfile = "job.lcsim"
    xmlfileok = PrepareLCSIMFile(self.xmlfile,lcsimfile,runonslcio,jars,cachedir,self.debug)
    if not xmlfileok:
      self.log.error("Could not treat input lcsim file")
      return S_ERROR("Error parsing input lcsim file")
    else:
      self.log.verbose("File job.lcsim created properly")
    
    scriptName = 'LCSIM_%s_Run_%s.sh' %(self.applicationVersion,self.STEP_NUMBER)
    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    #for lib in os.path("%s/GeomConverter/target/lib"%(mySoftwareRoot)):
    #  script.write("declare -x CLASSPATH=$CLASSPATH:%s\n"%lib)
    #script.write("declare -x CLASSPATH=$CLASSPATH:%s/lcsim/target/lcsim-%s.jar\n"%(mySoftwareRoot,self.applicationVersion))
    #script.write("declare -x BINPATH=%s/bin\n"%(sourcedir))
    #script.write("declare -x SOURCEPATH=%s/src\n"%(sourcedir))
    script.write("declare -x JAVALIBPATH=./\n")
    if os.path.exists("lib"):
      script.write("declare -x JAVALIBPATH=./lib\n")
    script.write('echo =========\n')
    script.write('env | sort >> localEnv.log\n')
    script.write('echo =========\n')
    
    comm = "java -server -Djava.library.path=$JAVALIBPATH -Dorg.lcsim.cacheDir=%s -jar %s/%s %s\n"%(cachedir,mySoftwareRoot,lcsim_name,lcsimfile)
    self.log.info("Will run %s"%comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')    
    script.close()
    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %scriptName
    self.setApplicationStatus('LCSIM %s step %s' %(self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    failed = False
    if status != 0:
      self.log.error( "LCSIM execution completed with errors:" )
      failed = True
    else:
      self.log.info( "LCSIM execution completed successfully")

    if failed:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError) 
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('LCSIM Exited With Status %s' %(status))
      return S_ERROR('LCSIM Exited With Status %s' %(status))    
    return S_OK('LCSIM %s Successful' %(self.applicationVersion))

  #############################################################################
  def redirectLogOutput(self, fd, message):
    sys.stdout.flush()
    if message:
      if re.search('INFO Evt',message): print message
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message
    #############################################################################
    