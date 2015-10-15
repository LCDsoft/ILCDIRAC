#!/bin/env python
'''
Define a new WHIZARD version: update the process list, create the new tar ball, upload it, upload the new processlist

Created on Sep 21, 2010

:author: sposs
:author: sailer
:author: strube
'''

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, exit as dexit
from DIRAC.Core.Base import Script
from DIRAC import S_OK,S_ERROR
import os, tarfile, shutil, sys, subprocess

try:
  import hashlib as md5
except ImportError:
  import md5

class Params(object):
  def __init__(self):
    self.path = ''
    self.version = ''
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.beam_spectra = ''

  def setVersion(self, optionValue):
    self.version = optionValue
    return S_OK()
  def setPlatform(self, optionValue):
    self.platform = optionValue
    return S_OK()
  def setBeamSpectra(self, optionValue):
    self.beam_spectra = optionValue
    return S_OK()
  def setPath(self, optionValue):
    self.path = optionValue
    return S_OK()
  def registerSwitches(self):
    Script.registerSwitch('P:', "Platform=", 'Platform to use', self.setPlatform)
    Script.registerSwitch('p:', "Path=", "Path to the Whizard results directory", self.setPath)
    Script.registerSwitch("V:", "Version=", "Whizard version", self.setVersion)
    Script.registerSwitch('b:', 'BeamSpectra=', 'Beam spectra version', self.setBeamSpectra)
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                         '\nUsage:',
                                         '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

def redirectLogOutput(dummy_fd, message):
  """ Needed to catch the log output of the shellCall below
  """
  sys.stdout.flush()
  print message
  
def readPRCFile(prc):
  """ Read the prc file to create the process description
  """
  myPRCList = {}
  with open(prc) as myprc:
    model = ""
    for process in myprc:
      process = process.rstrip()
      if not len(process):
        continue
      if process[0] == "#":
        continue
      elems = process.split()
      if elems[0] == "alias":
        continue
      elif elems[0] == "model":
        model = elems[1]
      elif not elems[0] == "model":
        myPRCList[elems[0]] = {}
        myPRCList[elems[0]]['Detail'] = "->".join(elems[1:3])
        myPRCList[elems[0]]['Generator'] = elems[3]
        myPRCList[elems[0]]['Restrictions'] = "none"
        if len(elems) > 4:
          myPRCList[elems[0]]['Restrictions'] = " ".join(elems[4:])
        myPRCList[elems[0]]['Model'] = model
        myPRCList[elems[0]]['InFile'] = "whizard.template.in"
      else:
        continue
    return myPRCList

def getDetailsFromPRC(prc, processin):
  """ Get the process details from the prc file
  """
  details = {}
  with open(prc) as myprc:
    model = ""
    for process in myprc:
      process = process.rstrip()
      if not len(process):
        continue
      elems = process.split()
      if process[0] == "#":
        continue
      elif elems[0] == "model":
        model = elems[1]
      elif not elems[0] == "model":
        if elems[0] == processin:
          details['Model'] = model
          details['Generator'] = elems[3]
          details['Restrictions'] = "none"
          if len(elems) > 4:
            details['Restrictions'] = " ".join(elems[4:])
          break
  return details


def checkGFortranVersion():
  """Find the version of the gfortran compiler"""
  proc = subprocess.Popen(['gfortran', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  out, dummy_err = proc.communicate()
  if out.find("4.4") > -1:
    return S_OK()
  return S_ERROR("Wrong gFortran version")


def checkSLCVersion():
  """Check the version of the operating system compiler"""
  proc = subprocess.Popen(['cat', '/etc/issue'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  out, dummy_err = proc.communicate()
  if out.find("release 5") > -1:
    return S_OK()
  return S_ERROR("Wrong Operating System")


def getListOfLibraries(pathName):
  """Get List of Libraries for library or executable"""
  listOfLibraries = []
  proc = subprocess.Popen(['ldd', pathName], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  out, dummyerr = proc.communicate()
  for line in out.split("\n"):
    fields = line.split("=>")
    if len(fields) == 2:
      newFields = fields[1].split()
      if len(newFields) == 2:
        lib = newFields[0].strip()
        listOfLibraries.append(lib)
  return listOfLibraries


def doTheWhizardInstallation():
  """Do the instalation for new whizard version Copy libraries, create tarball,
  upload processList file add entry in configuration system

  """
  res = checkSLCVersion()
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)

  res = checkGFortranVersion()
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)

  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors= False)
  
  whizardResultFolder = cliParams.path
  platform = cliParams.platform
  whizard_version = cliParams.version
  appVersion = whizard_version
  beam_spectra_version = cliParams.beam_spectra

  if not whizardResultFolder or not whizard_version or not beam_spectra_version:
    Script.showHelp()
    dexit(2)
  
  from ILCDIRAC.Core.Utilities.ProcessList                     import ProcessList
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations     import Operations 
  from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
  from ILCDIRAC.Core.Utilities.FileUtils                       import upload
  from DIRAC.DataManagementSystem.Client.DataManager           import DataManager
  diracAdmin = DiracAdmin()

  modifiedCS = False

  softwareSection = "/Operations/Defaults/AvailableTarBalls"
  processlistLocation = "ProcessList/Location"

  appName = "whizard"

  ops = Operations()
  path_to_process_list = ops.getValue(processlistLocation, "")
  if not path_to_process_list:
    gLogger.error("Could not find process list location in CS")
    dexit(2)
    
  gLogger.verbose("Getting process list from file catalog")
  datMan = DataManager()
  res = datMan.getFile(path_to_process_list)
  if not res['OK']:
    gLogger.error("Error while getting process list from storage")
    dexit(2)
  gLogger.verbose("done")

  ##just the name of the local file in current working directory
  processlist = os.path.basename(path_to_process_list)
  if not os.path.exists(processlist):
    gLogger.error("Process list does not exist locally")
    dexit(2)


  pl = ProcessList(processlist)
  
  startDir = os.getcwd()
  inputlist = {}
  os.chdir(whizardResultFolder)
  folderlist = os.listdir(whizardResultFolder)

  whiz_here = folderlist.count("whizard")
  if whiz_here == 0:
    gLogger.error("whizard executable not found in %s, please check" % whizardResultFolder)
    os.chdir(startDir)
    dexit(2)

  whizprc_here = folderlist.count("whizard.prc")
  if whizprc_here == 0:
    gLogger.error("whizard.prc not found in %s, please check" % whizardResultFolder)
    os.chdir(startDir)
    dexit(2)

  whizmdl_here = folderlist.count("whizard.mdl")
  if whizmdl_here == 0:
    gLogger.error("whizard.mdl not found in %s, please check" % whizardResultFolder)
    os.chdir(startDir)
    dexit(2)
   
    
  gLogger.verbose("Preparing process list")

  ## FIXME:: What is this doing exactly? Is this necessary? -- APS, JFS
  for f in folderlist:
    if f.count(".in"):
      infile = open(f, "r")
      found_detail = False
      
      for line in infile:
        if line.count("decay_description"):
          currprocess = f.split(".template.in")[0] 
          inputlist[currprocess] = {}        
          inputlist[currprocess]["InFile"] = f.rstrip("~")
          inputlist[currprocess]["Detail"] = line.split("\"")[1]
          found_detail = True
        if line.count("process_id") and found_detail:
          process_id = line.split("\"")[1]
          inputlist[currprocess]["Model"] = ""
          inputlist[currprocess]["Generator"] = ""
          inputlist[currprocess]["Restrictions"] = ""
          for process in process_id.split():
            print "Looking for detail of process %s" % (process)
            process_detail = getDetailsFromPRC("whizard.prc", process)  
            inputlist[currprocess]["Model"] = process_detail["Model"]
            inputlist[currprocess]["Generator"] = process_detail["Generator"]
            if len(inputlist[currprocess]["Restrictions"]):
              inputlist[currprocess]["Restrictions"] = inputlist[currprocess]["Restrictions"] + ", " + process_detail["Restrictions"]
            else:
              inputlist[currprocess]["Restrictions"] = process_detail["Restrictions"]
      #if len(inputlist[currprocess].items()):
      #  inputlist.append(processdict)    
  ## END FIXEME


  ##Update inputlist with what was found looking in the prc file
  processes = readPRCFile("whizard.prc")
  inputlist.update(processes)
  
  ##get from cross section files the cross sections for the processes in inputlist
  #Need full process list
  for f in folderlist:
    if f.count("cross_sections_"):
      crossfile = open(f, "r")
      for line in crossfile:
        line = line.rstrip().lstrip()
        if not len(line):
          continue
        if line[0] == "#" or line[0] == "!":
          continue
        if len(line.split()) < 2:
          continue
        currprocess = line.split()[0]
        if inputlist.has_key(currprocess):
          inputlist[currprocess]['CrossSection'] = line.split()[1]
  
  
  gLogger.notice("Preparing Tarball")

  ##Make a folder in the current directory of the user to store the whizard libraries, executable et al.
  localWhizardFolderRel = ("whizard" + whizard_version) # relative path
  localWhizardFolder = os.path.join(startDir, localWhizardFolderRel)

  if not os.path.exists(localWhizardFolder):
    os.makedirs(localWhizardFolder)
  
  localWhizardLibFolder = os.path.join(localWhizardFolder,'lib')
  if os.path.exists(localWhizardLibFolder):
    shutil.rmtree(localWhizardLibFolder)
  os.makedirs(localWhizardLibFolder) ##creates the lib folder

  whizardLibraries = getListOfLibraries(os.path.join(whizardResultFolder, "whizard"))
  copyLibsCall = ["rsync","-avzL"]
  for lib in whizardLibraries:
    copyLibsCall.append(lib)
  copyLibsCall.append(localWhizardLibFolder)
  subprocess.Popen(copyLibsCall, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  for fileName in folderlist:
    shutil.copy(fileName, localWhizardFolder)

  ##Get the list of md5 sums for all the files in the folder to be tarred
  os.chdir( localWhizardFolder )
  subprocess.call(["find . -type f -exec md5sum {} > ../md5_checksum.md5 \\; && mv ../md5_checksum.md5 ."], shell=True)
  os.chdir(startDir)

  ##Create the Tarball
  gLogger.notice("Creating Tarball...")
  appTar = localWhizardFolder + ".tgz"
  myappTar = tarfile.open(appTar, "w:gz")
  myappTar.add(localWhizardFolderRel)
  myappTar.close()
  
  md5sum = md5.md5(open( appTar, 'r' ).read()).hexdigest()
  
  gLogger.notice("...Done")

  gLogger.notice("Registering new Tarball in CS")
  tarballurl = {}
  
  av_platforms = gConfig.getSections(softwareSection, [])
  if av_platforms['OK']:
    if not platform in av_platforms['Value']:
      gLogger.error("Platform %s unknown, available are %s." % (platform, ", ".join(av_platforms['Value'])))
      gLogger.error("If yours is missing add it in CS")
      dexit(255)
  else:
    gLogger.error("Could not find all platforms available in CS")
    dexit(255)
  
  av_apps = gConfig.getSections("%s/%s" % (softwareSection, platform), [])
  if not av_apps['OK']:
    gLogger.error("Could not find all applications available in CS")
    dexit(255)
  
  if appName.lower() in av_apps['Value']:
    versions = gConfig.getSections("%s/%s/%s" % (softwareSection, platform, appName.lower()), 
                                   [])
    if not versions['OK']:
      gLogger.error("Could not find all versions available in CS")
      dexit(255)
    if appVersion in versions['Value']:
      gLogger.error('Application %s %s for %s already in CS, nothing to do' % (appName.lower(), appVersion, platform))
      dexit(0)
    else:
      result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall" % (softwareSection, platform, appName.lower(), appVersion),
                                      os.path.basename(appTar))
      if result['OK']:
        modifiedCS = True
        tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()), "")
        if len(tarballurl['Value']) > 0:
          res = upload(tarballurl['Value'], appTar)
          if not res['OK']:
            gLogger.error("Upload to %s failed" % tarballurl['Value'])
            dexit(255)
      result = diracAdmin.csSetOption("%s/%s/%s/%s/Md5Sum" % (softwareSection, platform, appName.lower(), appVersion),
                                      md5sum)
      if result['OK']:
        modifiedCS = True      
      result = diracAdmin.csSetOption("%s/%s/%s/%s/Dependencies/beam_spectra/version" % (softwareSection,
                                                                                         platform,
                                                                                         appName.lower(),
                                                                                         appVersion),
                                      beam_spectra_version)
      
  
  else:
    result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall" % (softwareSection, platform,
                                                             appName.lower(), appVersion),
                                    os.path.basename(appTar))
    if result['OK']:  
      modifiedCS = True
      tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()),
                                     "")
      if len(tarballurl['Value']) > 0:
        res = upload(tarballurl['Value'], appTar)
        if not res['OK']:
          gLogger.error("Upload to %s failed" % tarballurl['Value'])
          dexit(255)

    result = diracAdmin.csSetOption("%s/%s/%s/%s/Md5Sum" % (softwareSection, platform, appName.lower(), appVersion),
                                    md5sum)
          
    result = diracAdmin.csSetOption("%s/%s/%s/%s/Dependencies/beam_spectra/version" % (softwareSection,
                                                                                       platform,
                                                                                       appName.lower(),
                                                                                       appVersion),
                                    beam_spectra_version)

  gLogger.verbose("Done uploading the tar ball")
  
  os.remove(appTar)

  #Set for all new processes the TarBallURL
  for process in inputlist.keys():
    inputlist[process]['TarBallCSPath'] = tarballurl['Value'] + os.path.basename(appTar)
  
  pl.updateProcessList(inputlist)

  pl.writeProcessList()
  
  raw_input("Do you want to upload the process list? Press ENTER to proceed or CTRL-C to abort!")

  pl.uploadProcessListToFileCatalog(path_to_process_list, appVersion)

  #Commit the changes if nothing has failed and the CS has been modified
  if modifiedCS:
    result = diracAdmin.csCommitChanges(False)
    gLogger.verbose(result)
  gLogger.notice('All done OK!')
  dexit(0)

if __name__=="__main__":
  doTheWhizardInstallation()
  dexit(0)
