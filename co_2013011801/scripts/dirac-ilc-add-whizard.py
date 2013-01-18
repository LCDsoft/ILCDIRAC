'''
Created on Sep 21, 2010

@author: sposs
'''

import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )
args = Script.getPositionalArgs()

whizard_location = "%s" % args[0]
platform = "%s" % args[1]
whizard_version = "%s" % args[2]
appVersion = whizard_version
beam_spectra_version = "%s" % args[3]

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC.DataManagementSystem.Client.ReplicaManager        import ReplicaManager
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from DIRAC.RequestManagementSystem.Client.RequestContainer   import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient      import RequestClient
from ILCDIRAC.Core.Utilities.ProcessList                     import ProcessList

from DIRAC import gConfig, S_ERROR, S_OK

import os, tarfile, shutil, sys, string


diracAdmin = DiracAdmin()
rm = ReplicaManager()
request = RequestContainer()
requestClient = RequestClient()
modifiedCS = False

def usage():
  print 'Usage: %s <directory_where_whizard_is> <platform> <whizard_version> <beam_spectra_version>' % (Script.scriptName)
  DIRAC.exit(2)

def upload(path, appTar):
  """ Upload to storage
  """
  global appVersion
  if not os.path.exists(appTar):
    print "File %s does not exists, cannot continue." % appTar
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data") > -1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar, "%s%s" % (final_path, appTar))
    except Exception, x:
      print "Could not copy because %s" % x
      return S_ERROR()
  elif path.find("http://") > -1:
    print "path %s was not forseen, location not known, upload to location yourself, and publish in CS manually" % path
    return S_ERROR()
  else:
    lfnpath = "%s%s" % (path, os.path.basename(appTar))
    res = rm.putAndRegister(lfnpath, appTar, "CERN-SRM")
    if not res['OK']:
      return res
    res = request.addSubRequest({'Attributes': {'Operation' : 'replicateAndRegister',
                                                'TargetSE' : 'IN2P3-SRM'},
                                 'Files':[{'LFN':lfnpath}]},
                                 'transfer')
    #res = rm.replicateAndRegister("%s%s"%(path,appTar),"IN2P3-SRM")
    if not res['OK']:
      return res
    requestName = appTar.replace('.tgz','').replace('.cfg','_%s' % appVersion)
    request.setRequestAttributes({'RequestName' : requestName})
    requestxml = request.toXML()['Value']
    res = requestClient.setRequest(requestName, requestxml)
    if not res['OK']:
      print 'Could not set replication request %s' % res['Message']
    return S_OK('Application uploaded')
  return S_OK()

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

  
if len(args) < 3:
  usage()


softwareSection = "/Operations/Defaults/AvailableTarBalls"
processlistLocation = "/Operations/Defaults/ProcessList/Location"

appName = "whizard"


path_to_process_list = gConfig.getOption(processlistLocation, None)
if not path_to_process_list:
  print "Could not find process list Location in CS"
  DIRAC.exit(2)

res = rm.getFile(path_to_process_list['Value'])
if not res['OK']:
  print "Error while getting process list from storage"
  DIRAC.exit(2)
print "done"

processlist = os.path.basename(path_to_process_list['Value'])
if not os.path.exists(processlist):
  print "Process list does not exist locally"
  DIRAC.exit(2)


pl = ProcessList(processlist)

startdir = os.getcwd()
inputlist = {}
os.chdir(whizard_location)
folderlist = os.listdir(os.getcwd())
whiz_here = folderlist.count("whizard")
if whiz_here == 0:
  print "whizard executable not found in %s, please check" % whizard_location
  os.chdir(startdir)
  DIRAC.exit(2)
whizprc_here = folderlist.count("whizard.prc")
if whizprc_here == 0:
  print "whizard.prc not found in %s, please check" % whizard_location
  os.chdir(startdir)
  DIRAC.exit(2)
whizmdl_here = folderlist.count("whizard.mdl")
if whizprc_here == 0:
  print "whizard.mdl not found in %s, please check" % whizard_location
  os.chdir(startdir)
  DIRAC.exit(2)
 
  
print "Preparing process list"

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


print "Preparing Tar ball"
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
""" % ("whizard" + whizard_version))
script.close()
os.chmod(scriptName, 0755)
comm = 'source %s' % (scriptName)
result = shellCall(0, comm, callbackFunction = redirectLogOutput, bufferLimit = 20971520)
os.remove("ldd.sh")
myappTar = tarfile.open(appTar, "w:gz")
myappTar.add("whizard" + whizard_version)
myappTar.close()
print "Done"
print "Registering new Tar Ball in CS"
tarballurl = {}

av_platforms = gConfig.getSections(softwareSection, [])
if av_platforms['OK']:
  if not platform in av_platforms['Value']:
    print "Platform %s unknown, available are %s." % (platform, string.join(av_platforms['Value'], ", "))
    print "If yours is missing add it in CS"
    DIRAC.exit(255)
else:
  print "Could not find all platforms available in CS"
  DIRAC.exit(255)

av_apps = gConfig.getSections("%s/%s" % (softwareSection, platform), [])
if not av_apps['OK']:
  print "Could not find all applications available in CS"
  DIRAC.exit(255)

if appName.lower() in av_apps['Value']:
  versions = gConfig.getSections("%s/%s/%s" % (softwareSection, platform, appName.lower()), 
                                 [])
  if not versions['OK']:
    print "Could not find all versions available in CS"
    DIRAC.exit(255)
  if appVersion in versions['Value']:
    print 'Application %s %s for %s already in CS, nothing to do' % (appName.lower(), appVersion, platform)
    DIRAC.exit(0)
  else:
    result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall" % (softwareSection, platform, appName.lower(), appVersion),
                                    os.path.basename(appTar))
    if result['OK']:
      modifiedCS = True
      tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL" % (softwareSection, platform, appName.lower()), "")
      if len(tarballurl['Value']) > 0:
        res = upload(tarballurl['Value'], appTar)
        if not res['OK']:
          print "Upload to %s failed" % tarballurl
          DIRAC.exit(255)
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
        print "Upload to %s failed" % tarballurl
        DIRAC.exit(255)
  result = diracAdmin.csSetOption("%s/%s/%s/%s/Dependencies/beam_spectra/version" % (softwareSection,
                                                                                     platform,
                                                                                     appName.lower(),
                                                                                     appVersion),
                                  beam_spectra_version)
print "Done"

os.remove(appTar)
#Set for all new processes the TarBallURL
for process in inputlist.keys():
  inputlist[process]['TarBallCSPath'] = tarballurl['Value'] + os.path.basename(appTar)


knownprocess = pl.getProcessesDict()
knownprocess.update(inputlist)
pl.updateProcessList(knownprocess)
print "Done"

#Return to initial location
os.chdir(startdir)

pl.writeProcessList()

res = rm.removeFile(path_to_process_list['Value'])
if not res['OK']:
  print "Could not remove process list from storage, do it by hand"
  DIRAC.exit(2)


res = upload(os.path.dirname(path_to_process_list['Value']) + "/", processlist)
if not res['OK']:
  print "something went wrong in the copy"
  DIRAC.exit(2)

localprocesslistpath = gConfig.getOption("/LocalSite/ProcessListPath", "")
if localprocesslistpath['Value']:
  try:
    shutil.copy(processlist, localprocesslistpath['Value'])
  except:
    print "Copy of process list to %s failed!" % localprocesslistpath['Value']
print "Done"
#Commit the changes if nothing has failed and the CS has been modified
if modifiedCS:
  result = diracAdmin.csCommitChanges(False)
  print result

exitCode = 0
DIRAC.exit(exitCode)
