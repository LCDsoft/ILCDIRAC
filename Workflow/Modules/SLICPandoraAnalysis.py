#####################################################
# $HeadURL: $
#####################################################
'''
Run SLICPandora

@since: Oct 25, 2010

@author: sposs
'''

__RCSID__ = "$Id: $"

import os, urllib, zipfile, shutil, glob, types

from DIRAC.Core.Utilities.Subprocess                      import shellCall

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from ILCDIRAC.Core.Utilities.resolvePathsAndNames         import resolveIFpaths
from ILCDIRAC.Core.Utilities.PrepareOptionFiles           import GetNewLDLibs, GetNewPATH
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc

from DIRAC                                                import S_OK, S_ERROR, gLogger

def unzip_file_into_dir(myfile, mydir):
  """Used to unzip the downloaded detector model
  """
  zfobj = zipfile.ZipFile(myfile)
  for name in zfobj.namelist():
    if name.endswith('/'):
      os.mkdir(os.path.join(mydir, name))
    else:
      outfile = open(os.path.join(mydir, name), 'wb')
      outfile.write(zfobj.read(name))
      outfile.close()
        
class SLICPandoraAnalysis (ModuleBase):
  """ Run SLIC Pandora  
  """
  def __init__(self):
    super(SLICPandoraAnalysis, self).__init__()
    self.STEP_NUMBER = ''
    self.result = S_ERROR()
    self.applicationName = 'SLICPandora'
    self.pandorasettings = ""
    self.detectorxml = ""
    self.InputFile = []
    self.NumberOfEvents = -1
    self.startFrom = 0
    self.eventstring = ['>>>>>> EVENT']
    self.log = gLogger.getSubLogger('SLICPandora')
    
  def applicationSpecificInputs(self):

    if self.step_commons.has_key("PandoraSettings"):
      self.pandorasettings = self.step_commons["PandoraSettings"]
    if not self.pandorasettings:
      self.pandorasettings  = "PandoraSettings.xml"
    else:
      self.pandorasettings  = os.path.basename(self.pandorasettings)
       
    if self.step_commons.has_key("DetectorXML"):
      self.detectorxml = self.step_commons["DetectorXML"]

    if self.step_commons.has_key("inputSlcio"):
      inputf = self.step_commons["inputSlcio"]
      if not type(inputf) == types.ListType:
        inputf = inputf.split(";")
      self.InputFile = inputf
        
    if self.step_commons.has_key('EvtsToProcess'):
      self.NumberOfEvents = self.step_commons['EvtsToProcess']
          
    if self.step_commons.has_key('startFrom'):
      self.startFrom = self.step_commons['startFrom']
      
    if not len(self.InputFile) and len(self.InputData):
      for files in self.InputData:
        if files.lower().find(".slcio") > -1:
          self.InputFile.append(files)
           
    return S_OK('Parameters resolved')
  
  
  
  
  def execute(self):
    """ Called from Workflow
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
      return S_OK('SLIC Pandora should not proceed as previous step did not end properly')
    
    slicPandoraDir = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall' % (self.systemConfig, 
                                                                                self.applicationName, 
                                                                                self.applicationVersion), '')
    slicPandoraDir = slicPandoraDir.replace(".tgz", "").replace(".tar.gz", "")
    res = getSoftwareFolder(slicPandoraDir)
    if not res['OK']:
      self.setApplicationStatus('SLICPandora: Could not find neither local area not shared area install')
      return res
    myslicPandoraDir = res['Value']

    ##Remove libc lib
    removeLibc(myslicPandoraDir + "/LDLibs")

    ##Need to fetch the new LD_LIBRARY_PATH
    new_ld_lib_path = GetNewLDLibs(self.systemConfig, self.applicationName, self.applicationVersion)

    new_path = GetNewPATH(self.systemConfig, self.applicationName, self.applicationVersion)

    res = resolveIFpaths(self.InputFile)
    if not res['OK']:
      self.setApplicationStatus('SLICPandora: missing slcio file')
      return S_ERROR('Missing slcio file!')
    runonslcio = res['Value'][0]
    
    if not self.detectorxml.count(".xml") or not os.path.exists(os.path.basename(self.detectorxml)):
      detmodel = self.detectorxml.replace("_pandora.xml", "")
      if os.path.exists(detmodel + ".zip"):
        try:
          unzip_file_into_dir(open(detmodel + ".zip"), os.getcwd())
        except:
          os.unlink(detmodel + ".zip") 
      if not os.path.exists(detmodel + ".zip"):  
        #retrieve detector model from web
        detector_urls = self.ops.getValue('/SLICweb/SLICDetectorModels', [''])
        if len(detector_urls[0]) < 1:
          self.log.error('Could not find in CS the URL for detector model')
          return S_ERROR('Could not find in CS the URL for detector model')

        for detector_url in detector_urls:
          try:
            urllib.urlretrieve("%s%s"%(detector_url, detmodel + ".zip"), detmodel + ".zip")
          except:
            self.log.error("Download of detector model failed")
            continue
          try:
            unzip_file_into_dir(open(detmodel + ".zip"), os.getcwd())
            break
          except:
            os.unlink(detmodel + ".zip")
            continue
      #if os.path.exists(detmodel): #and os.path.isdir(detmodel):
      self.detectorxml = os.path.join(os.getcwd(), self.detectorxml)
      self.detectorxml = self.detectorxml + "_pandora.xml"
    
    if not os.path.exists(self.detectorxml):
      self.log.error('Detector model xml %s was not found, exiting' % self.detectorxml)
      return S_ERROR('Detector model xml %s was not found, exiting' % self.detectorxml)
    
    if not os.path.exists(self.pandorasettings):
      if os.path.exists("./Settings/%s" % self.pandorasettings):
        self.pandorasettings = "./Settings/%s" % self.pandorasettings
      elif os.path.exists(os.path.join(myslicPandoraDir, 'Settings')):
        xmllist = glob.glob(os.path.join(myslicPandoraDir, 'Settings', "*.xml"))
        for f in xmllist:
          try:
            shutil.copy(f, os.path.join(os.getcwd(), os.path.basename(f)))
          except Exception, x:
            self.log.error('Could not copy %s, exception: %s' % (f, str(x)))
            return S_ERROR('Could not copy PandoraSettings file')
      else:
        self.log.error("Failed to find PandoraSettings anywhere, possibly SLICPandora install broken")
        return S_ERROR("Failed to find PandoraSettings anywhere")
    if not os.path.exists(self.pandorasettings):
      self.log.error("PandoraSettings %s not found" % (self.pandorasettings))
      return S_ERROR("PandoraSettings not found locally")
    
    oldversion = False
    if self.applicationVersion in ['CLIC_CDR', 'CDR1', 'CDR2', 'CDR0', 'V2', 'V3', 'V4']:
      oldversion = True
    
    scriptName = 'SLICPandora_%s_Run_%s.sh' % (self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(scriptName): 
      os.remove(scriptName)
    script = open(scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('declare -x PATH=%s:$PATH\n' % new_path)
    script.write('echo =============================\n')
    script.write('echo PATH is \n')
    script.write('echo $PATH | tr ":" "\n"  \n')
    script.write('echo ==============\n')
    script.write('which ls\n')
    script.write('declare -x ROOTSYS=%s/ROOT\n' % (myslicPandoraDir))

    if os.environ.has_key('LD_LIBRARY_PATH'):
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs:%s\n' % (myslicPandoraDir, new_ld_lib_path))
    else:
      script.write('declare -x LD_LIBRARY_PATH=$ROOTSYS/lib:%s/LDLibs\n' % (myslicPandoraDir))

    if os.path.exists("./lib"):
      script.write('declare -x LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH\n')
    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is \n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo ============================= \n')
    script.write('env | sort >> localEnv.log\n')
    prefixpath = ""
    if os.path.exists("PandoraFrontend"):
      prefixpath = "."
    elif (os.path.exists("%s/Executable/PandoraFrontend" % myslicPandoraDir)):
      prefixpath ="%s/Executable" % myslicPandoraDir
    if prefixpath:
      if oldversion:
        comm = '%s/PandoraFrontend %s %s %s %s %s' % (prefixpath, self.detectorxml, self.pandorasettings,
                                                        runonslcio, self.OutputFile, str(self.NumberOfEvents))
      else:
        comm = '%s/PandoraFrontend -g %s -c %s -i %s -o %s -r %s' % (prefixpath, self.detectorxml, 
                                                                      self.pandorasettings, runonslcio,
                                                                      self.OutputFile, str(self.NumberOfEvents))
      comm = "%s %s\n" % (comm, self.extraCLIarguments)
      self.log.info("Will run %s" % comm)
      script.write(comm)
    else:
      script.close()
      self.log.error("PandoraFrontend executable is missing, something is wrong with the installation!")
      return S_ERROR("PandoraFrontend executable is missing")
    
    script.write('declare -x appstatus=$?\n')
    #script.write('where\n')
    #script.write('quit\n')
    #script.write('EOF\n')
    script.write('exit $appstatus\n')

    script.close()
    if os.path.exists(self.applicationLog): 
      os.remove(self.applicationLog)

    os.chmod(scriptName, 0755)
    comm = 'sh -c "./%s"' % (scriptName)
    self.setApplicationStatus('SLICPandora %s step %s' % (self.applicationVersion, self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput,
                            bufferLimit = 20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']
    if not os.path.exists(self.applicationLog):
      self.log.error("Something went terribly wrong, the log file is not present")
      self.setApplicationStatus('%s failed terribly, you are doomed!' % (self.applicationName))
      if not self.ignoreapperrors:
        return S_ERROR('%s did not produce the expected log' % (self.applicationName))
    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]
    self.log.info( "Status after the application execution is %s" % str( status ) )

    return self.finalStatusReport(status)
    #############################################################################

  
  
  
      