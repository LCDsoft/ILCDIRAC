#!/bin/env python
'''
Define a new WHIZARD version: update the process list, create the new tar ball, upload it, upload the new processlist

Created on Sep 21, 2010

@author: sposs
'''

from DIRAC.Core.Base import Script
from DIRAC import S_OK,S_ERROR
import os, tarfile, shutil, sys, string

try:
  import hashlib as md5
except:
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
    Script.registerSwitch('p:', "Path=", "Path where Whizard is", self.setPath)
    Script.registerSwitch("V:", "Version=", "Whizard version", self.setVersion)
    Script.registerSwitch('b:', 'BeamSpectra=', 'Beam spectra version', self.setBeamSpectra)
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

def redirectLogOutput(fd, message):
  """ Needed to catch the log output of the shellCall below
  """
  sys.stdout.flush()
  print message
  
def readPRCFile(prc):
  """ Read the prc file to create the process description
  """
  list = {}
  myprc = file(prc)
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
      list[elems[0]] = {}
      list[elems[0]]['Detail'] = string.join(elems[1:3], "->")
      list[elems[0]]['Generator'] = elems[3]
      list[elems[0]]['Restrictions'] = "none"
      if len(elems) > 4:
        list[elems[0]]['Restrictions'] = string.join(elems[4:], " ")
      list[elems[0]]['Model'] = model
      list[elems[0]]['InFile'] = "whizard.template.in"
    else:
      continue
  
  return list

def getDetailsFromPRC(prc, processin):
  """ Get the process details from the prc file
  """
  details = {}
  myprc = file(prc)
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
          details['Restrictions'] = string.join(elems[4:], " ")
        break
  return details


"""Find the version of the gfortran compiler"""
def checkGFortranVersion():
  p = subprocess.Popen(['gfortran', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  out, err = p.communicate()
  if out.find("4.4") > -1:
    return S_OK
  else:
    return S_ERROR



if __name__=="__main__":

  if checkGFortranVersion() == S_ERROR:
    gLogger.error("Wrong Version of gfortran found, need version 4.4")
    dexit(1)

  cliParams = Params()
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors= False)
  
  from DIRAC import gConfig, gLogger, exit as dexit
  whizard_location = cliParams.path
  platform = cliParams.platform
  whizard_version = cliParams.version
  appVersion = whizard_version
  beam_spectra_version = cliParams.beam_spectra

  if not whizard_location or not whizard_version or not beam_spectra_version:
    Script.showHelp()
    dexit(2)
  
  from DIRAC.Core.Utilities.Subprocess                         import shellCall
  from ILCDIRAC.Core.Utilities.ProcessList                     import ProcessList
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations     import Operations 
  from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
  from ILCDIRAC.Core.Utilities.FileUtils                       import upload
  from DIRAC.DataManagementSystem.Client.ReplicaManager        import ReplicaManager
  diracAdmin = DiracAdmin()

  modifiedCS = False

  softwareSection = "/Operations/Defaults/AvailableTarBalls"
  processlistLocation = "ProcessList/Location"

  appName = "whizard"

  ops = Operations()
  path_to_process_list = ops.getValue(processlistLocation, "")
  if not path_to_process_list:
    gLogger.error("Could not find process list Location in CS")
    dexit(2)
    
  gLogger.verbose("Getting process list from storage")
  rm = ReplicaManager()
  res = rm.getFile(path_to_process_list)
  if not res['OK']:
    gLogger.error("Error while getting process list from storage")
    dexit(2)
  gLogger.verbose("done")

  processlist = os.path.basename(path_to_process_list)
  if not os.path.exists(processlist):
    gLogger.error("Process list does not exist locally")
    dexit(2)


  pl = ProcessList(processlist)
  
  startdir = os.getcwd()
  inputlist = {}
  os.chdir(whizard_location)
  folderlist = os.listdir(os.getcwd())

  whiz_here = folderlist.count("whizard")
  if whiz_here == 0:
    gLogger.error("whizard executable not found in %s, please check" % whizard_location)
    os.chdir(startdir)
    dexit(2)

  whizprc_here = folderlist.count("whizard.prc")
  if whizprc_here == 0:
    gLogger.error("whizard.prc not found in %s, please check" % whizard_location)
    os.chdir(startdir)
    dexit(2)

  whizmdl_here = folderlist.count("whizard.mdl")
  if whizprc_here == 0:
    gLogger.error("whizard.mdl not found in %s, please check" % whizard_location)
    os.chdir(startdir)
    dexit(2)
   
    
  gLogger.verbose("Preparing process list")
  
  for f in folderlist:
    if f.count(".in"):
      infile = file(f, "r")
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
  
  ##Update inputlist with what was found looking in the prc file
  processes = readPRCFile("whizard.prc")
  inputlist.update(processes)
  
  ##get from cross section files the cross sections for the processes in inputlist
  #Need full process list
  for f in folderlist:
    if f.count("cross_sections_"):
      crossfile = file(f, "r")
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
  
  
  gLogger.notice("Preparing Tar ball")
  appTar = os.path.join(os.getcwd(), "whizard" + whizard_version + ".tgz")
  
  if os.path.exists('lib'):
    shutil.rmtree('lib')
  scriptName = './ldd.sh'
  script = file(scriptName, "w")
  script.write('#!/bin/bash \n')
  script.write("""string1=$(ldd whizard | grep '=>' | sed 's/.*=>//g' | sed 's/(.*)//g')
  string=""
  for file in $string1; do
    string=\"$file $string\"
  done
  mkdir lib
  cp $string ./lib
  whizarddir=%s 
  rm -rf $whizarddir
  mkdir $whizarddir
  cp -r *.cut1 *.in cross_sections_* whizard whizard.prc whizard.mdl lib/ $whizarddir
  cd $whizarddir
  find . -type f -print0 | xargs -0 md5sum > md5_checksum.md5
  cd ..
  """ % ("whizard" + whizard_version))
  script.close()
  os.chmod(scriptName, 0755)
  comm = 'source %s' % (scriptName)
  result = shellCall(0, comm, callbackFunction = redirectLogOutput, bufferLimit = 20971520)
  os.remove("ldd.sh")
  myappTar = tarfile.open(appTar, "w:gz")
  myappTar.add("whizard" + whizard_version)
  myappTar.close()
  
  md5sum = md5.md5(file(appTar).read()).hexdigest()
  
  gLogger.verbose("Done creating tar ball")
  gLogger.notice("Registering new Tar Ball in CS")
  tarballurl = {}
  
  av_platforms = gConfig.getSections(softwareSection, [])
  if av_platforms['OK']:
    if not platform in av_platforms['Value']:
      gLogger.error("Platform %s unknown, available are %s." % (platform, string.join(av_platforms['Value'], ", ")))
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
  
  gLogger.verbose("Updating process list:")
  knownprocess = pl.getProcessesDict()
  knownprocess.update(inputlist)
  pl.updateProcessList(knownprocess)
  gLogger.verbose("Done Updating process list")
  
  #Return to initial location
  os.chdir(startdir)
  
  pl.writeProcessList()
  gLogger.verbose("Removing process list from storage")

  res = rm.removeFile(path_to_process_list)
  if not res['OK']:
    gLogger.error("Could not remove process list from storage, do it by hand")
    dexit(2)
  
  
  res = upload(os.path.dirname(path_to_process_list) + "/", processlist)
  if not res['OK']:
    gLogger.error("something went wrong in the copy")
    dexit(2)
  gLogger.verbose("Done Removing process list from storage")
  gLogger.verbose("Putting process list to local processlist directory")
  localprocesslistpath = gConfig.getOption("/LocalSite/ProcessListPath", "")
  if localprocesslistpath['Value']:
    try:
      shutil.copy(processlist, localprocesslistpath['Value'])
    except:
      gLogger.error("Copy of process list to %s failed!" % localprocesslistpath['Value'])
  gLogger.verbose("Done")
  #Commit the changes if nothing has failed and the CS has been modified
  if modifiedCS:
    result = diracAdmin.csCommitChanges(False)
    gLogger.verbose(result)
  gLogger.notice('All done OK!')
  dexit(0)
