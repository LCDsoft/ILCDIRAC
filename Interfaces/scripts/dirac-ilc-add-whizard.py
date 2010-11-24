'''
Created on Sep 21, 2010

@author: sposs
'''

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.Subprocess                      import shellCall

from ILCDIRAC.Core.Utilities.ProcessList import ProcessList

from DIRAC import gConfig, S_ERROR,S_OK

import os,tarfile, shutil, sys, string

Script.parseCommandLine( ignoreErrors = False )
diracAdmin = DiracAdmin()
rm = ReplicaManager()
modifiedCS = False


args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <directory_where_whizard_is> <platform> <whizard_version> <beam_spectra_version>' % (Script.scriptName)
  DIRAC.exit(2)

def upload(path,appTar):
  if not os.path.exists(appTar):
    print "File %s does not exists, cannot continue."%appTar
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data")>-1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar,"%s%s"%(final_path,appTar))
    except Exception,x:
      print "Could not copy because %s"%x
      return S_ERROR()
  elif path.find("http://")>-1:
    print "path %s was not forseen, location not known, upload to location yourself, and publish in CS manually"%path
    return S_ERROR()
  else:
    res = rm.putAndRegister("%s%s"%(path,os.path.basename(appTar)),appTar,"CERN-SRM")
    return res
  return S_OK()

def redirectLogOutput(fd, message):
  sys.stdout.flush()

def readPRCFile(prc):
  list = []
  myprc = file(prc)
  for process in myprc:
    if len(process.split()):
      if not  process[0]=="#" and not process.split()[0]=="model":
        list.append(process.split()[0])
  return list

  
if len(args) < 3:
  usage()


softwareSection = "/Operations/AvailableTarBalls"
processlistLocation = "/Operations/ProcessList/Location"

appName  ="whizard"

whizard_location = "%s"%args[0]
platform = "%s"%args[1]
whizard_version = "%s"%args[2]
appVersion = whizard_version
beam_spectra_version= "%s"%args[3]

path_to_process_list = gConfig.getOption(processlistLocation, None)
if not path_to_process_list:
  print "Could not find process list Location in CS"
  DIRAC.exit(2)

res = rm.getFile(path_to_process_list['Value'])
if not res['OK']:
  print "Error while getting process list from storage"
  DIRAC.exit(2)

processlist = os.path.basename(path_to_process_list['Value'])
if not os.path.exists(processlist):
  print "Process list does not exist locally"
  DIRAC.exit(2)


pl = ProcessList(processlist)

startdir = os.getcwd()

os.chdir(whizard_location)
folderlist = os.listdir(os.getcwd())
whiz_here = folderlist.count("whizard")
if whiz_here==0:
  print "whizard executable not found in %s, please check"%whizard_location
  os.chdir(startdir)
  DIRAC.exit(2)
whizprc_here = folderlist.count("whizard.prc")
if whizprc_here==0:
  print "whizard.prc not found in %s, please check"%whizard_location
  os.chdir(startdir)
  DIRAC.exit(2)
whizmdl_here = folderlist.count("whizard.mdl")
if whizprc_here==0:
  print "whizard.mdl not found in %s, please check"%whizard_location
  os.chdir(startdir)
  DIRAC.exit(2)
  
appTar = os.path.join(os.getcwd(),"whizard"+whizard_version+".tgz")

myappTar = tarfile.open(appTar,"w:gz")
myappTar.add("whizard")
myappTar.add("whizard.prc")
myappTar.add("whizard.mdl")
if os.path.exists('lib'):
  shutil.rmtree('lib')
os.mkdir('lib')
os.chdir('lib')
scriptName = file('ldd.sh',"w")
scriptName.write("""echo =============================
echo Running ldd recursively on whizard
echo =============================
string1=$(ldd whizard | grep '=>' | sed 's/.*=>//g' | sed 's/(.*)//g')
string=''
for file in $string1; do
  string='$file $string'
done
cp $string . \n""")
scriptName.close()
comm = 'sh -c "./%s"' %(scriptName)
result = shellCall(0,comm,callbackFunction=redirectLogOutput,bufferLimit=20971520)
os.remove("ldd.sh")
os.chdir(whizard_location)
myappTar.add('lib')
myappTar.close()
tarballurl = {}

av_platforms = gConfig.getSections(softwareSection, [])
if av_platforms['OK']:
  if not platform in av_platforms['Value']:
    print "Platform %s unknown, available are %s."%(platform,string.join(av_platforms['Value'],", "))
    print "If yours is missing add it in CS"
    DIRAC.exit(255)
else:
  print "Could not find all platforms available in CS"
  DIRAC.exit(255)

av_apps = gConfig.getSections("%s/%s"%(softwareSection,platform),[])
if not av_apps['OK']:
  print "Could not find all applications available in CS"
  DIRAC.exit(255)

if appName.lower() in av_apps['Value']:
  versions = gConfig.getSections("%s/%s/%s"%(softwareSection,platform,appName.lower()),[])
  if not versions['OK']:
    print "Could not find all versions available in CS"
    DIRAC.exit(255)
  if appVersion in versions['Value']:
    print 'Application %s %s for %s already in CS, nothing to do'%(appName.lower(),appVersion,platform)
    DIRAC.exit(0)
  else:
    result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName.lower(),appVersion),appTar)
    if result['OK']:
      modifiedCS = True
      tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL"%(softwareSection,platform,appName.lower()),"")
      if len(tarballurl['Value'])>0:
        res = upload(tarballurl['Value'],appTar)
        if not res['OK']:
          print "Upload to %s failed"%tarballurl
          DIRAC.exit(255)

else:
  result = diracAdmin.csSetOption("%s/%s/%s/%s/TarBall"%(softwareSection,platform,appName.lower(),appVersion),appTar)
  if result['OK']:  
    modifiedCS = True
    tarballurl = gConfig.getOption("%s/%s/%s/TarBallURL"%(softwareSection,platform,appName.lower()),"")
    if len(tarballurl['Value'])>0:
      res = upload(tarballurl['Value'],appTar)
      if not res['OK']:
        print "Upload to %s failed"%tarballurl
        DIRAC.exit(255)

os.remove(appTar)

processes= readPRCFile("whizard.prc")
for process in processes:
  pl.setCSPath(process,tarballurl['Value']+os.path.basename(appTar))

os.chdir(startdir)

pl.writeProcessList()

res = rm.removeFile(path_to_process_list['Value'])
if not res['OK']:
  print "Could not remove process list from storage, do it by hand"
  DIRAC.exit(2)


res = upload(os.path.dirname(path_to_process_list['Value'])+"/",processlist)
if not res['OK']:
  print "something went wrong in the copy"
  DIRAC.exit(2)

#Commit the changes if nothing has failed and the CS has been modified
if modifiedCS:
  result = diracAdmin.csCommitChanges(False)
  print result

exitCode = 0
DIRAC.exit(exitCode)
