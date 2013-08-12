#####################################################
# $HeadURL: $
#####################################################
'''
Get the overlay files

@since: Jan 27, 2011

@author: sposs
'''

__RCSID__ = "$Id: $"

from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from DIRAC.DataManagementSystem.Client.ReplicaManager        import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalogClient               import FileCatalogClient
from DIRAC.Core.DISET.RPCClient                              import RPCClient
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from ILCDIRAC.Core.Utilities.WasteCPU                        import WasteCPUCycles
from DIRAC.ConfigurationSystem.Client.Helpers.Operations     import Operations

from DIRAC                                                   import S_OK, S_ERROR, gLogger
import DIRAC
from math import ceil, modf

from decimal import Decimal

import os, time, random, string, subprocess, glob

def allowedBkg( bkg, energy = None, detector = None, detectormodel = None, machine = 'clic_cdr' ):
  """ Check is supplied bkg is allowed
  """
  #gLogger.info("Those are the arguments: %s, %s, %s, %s, %s" % (bkg, energy, detector, detectormodel, machine) )
  ops = Operations()
  #bkg_allowed = [ 'gghad', 'pairs' ]
  #if not bkg in bkg_allowed:
  #  return S_ERROR( "Bkg not allowed" )
  res = -1
  if energy:
    if detectormodel:
      res = ops.getValue( "/Overlay/%s/%s/%s/%s/ProdID" % (machine, energy, detectormodel, bkg), -1 )
      if res < 0:
        return S_ERROR( "No background to overlay" )  
    if detector:##needed for backward compatibility
      res = ops.getValue( "/Overlay/%s/%s/%s/%s/ProdID" % (machine, detector, energy, bkg), -1 )
      if res < 0 :
        return S_ERROR( "No background to overlay" )  
  return S_OK(res)

class OverlayInput (ModuleBase):
  """ Download the files for overlay.
  """
  def __init__(self):
    super(OverlayInput, self).__init__()
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "OverlayInput" )
    self.applicationName = 'OverlayInput'
    self.curdir = os.getcwd()
    self.applicationLog = ''
    self.printoutflag = ''
    self.prodid = 0
    self.detector = '' ##needed for backward compatibility
    self.detectormodel = ""
    self.energytouse = ''
    self.energy = 0
    self.nbofeventsperfile = 100
    self.lfns = []
    self.nbfilestoget = 0
    self.BkgEvtType = 'gghad'
    self.BXOverlay = 0
    self.ggtohadint = 3.2
    self.nbsigeventsperfile = 0
    self.nbinputsigfile = 1
    self.NbSigEvtsPerJob = 0
    self.rm = ReplicaManager()
    self.fc = FileCatalogClient()
    self.site = DIRAC.siteName()

    self.machine = 'clic_cdr'

  def applicationSpecificInputs(self):

    if self.step_commons.has_key('Detector'):
      self.detectormodel = self.step_commons['Detector']
    if not self.detectormodel and not self.detector:
      return S_ERROR('Detector model not defined')

    if self.step_commons.has_key('Energy'):
      self.energytouse = self.step_commons['Energy']

    if self.energy:
      fracappen = modf(self.energy / 1000.)
      if fracappen[1] > 0:
        self.energytouse = "%stev" % (Decimal(str(self.energy)) / Decimal("1000."))
      else:
        self.energytouse = "%sgev" % (Decimal(str(self.energy)))
      if self.energytouse.count(".0"):
        self.energytouse = self.energytouse.replace(".0", "")
            
    if not self.energytouse:
      return S_ERROR("Energy not set anywhere!")

    if self.step_commons.has_key('BXOverlay'):
      self.BXOverlay = self.step_commons['BXOverlay']
    if not self.BXOverlay:
      return S_ERROR("BXOverlay parameter not defined")

    if self.step_commons.has_key('ggtohadint'):
      self.ggtohadint = self.step_commons['ggtohadint']

    if self.step_commons.has_key('ProdID'):
      self.prodid = self.step_commons['ProdID']

    if self.step_commons.has_key('NbSigEvtsPerJob'):
      self.NbSigEvtsPerJob = self.step_commons['NbSigEvtsPerJob']

    if self.step_commons.has_key('BkgEvtType'):
      self.BkgEvtType = self.step_commons['BkgEvtType']
      
    res =  allowedBkg(self.BkgEvtType, self.energytouse, detector = self.detector, 
                      detectormodel = self.detectormodel, machine = self.machine) 
    if not res['OK']:
      return res
    if res['Value'] < 0:
      return S_ERROR("No suitable ProdID") 
    #if self.workflow_commons.has_key('Site'):
    #  self.site = self.workflow_commons['Site']

    if len(self.InputData):
      if self.NumberOfEvents:
        self.nbsigeventsperfile = self.NumberOfEvents
      else:
        return S_ERROR("Number of events in the signal file is missing")
      self.nbinputsigfile = len(self.InputData)
      
    if not self.NbSigEvtsPerJob and not self.nbsigeventsperfile:
      return S_ERROR("Could not determine the number of signal events per input file")
    return S_OK("Input variables resolved")

  def __getFilesFromFC(self):
    """ Get the list of files from the FileCatalog.
    """
    meta = {}
    meta['Energy'] = self.energytouse
    meta['EvtType'] = self.BkgEvtType
    meta['Datatype'] = 'SIM'
    if self.detectormodel:
      meta['DetectorModel'] = self.detectormodel
    if self.machine == 'ilc_dbd':
      meta['Machine'] = 'ilc'
    if self.machine == 'clic_cdr':
      meta['Machine'] = 'clic'
    res = None
    if self.detector:
      res = self.ops.getValue("/Overlay/%s/%s/%s/%s/ProdID" % (self.machine, self.detector, 
                                                               self.energytouse, self.BkgEvtType), 0)
      self.nbofeventsperfile = self.ops.getValue("/Overlay/%s/%s/%s/%s/NbEvts" % (self.machine, 
                                                                                  self.energytouse, 
                                                                                  self.detector, 
                                                                                  self.BkgEvtType), 
                                                 100)

    else:
      res = self.ops.getValue("/Overlay/%s/%s/%s/%s/ProdID" % (self.machine, 
                                                               self.energytouse, 
                                                               self.detectormodel, 
                                                               self.BkgEvtType), 
                              0)
      self.nbofeventsperfile = self.ops.getValue("/Overlay/%s/%s/%s/%s/NbEvts" % (self.machine, 
                                                                                  self.energytouse,
                                                                                  self.detectormodel,
                                                                                  self.BkgEvtType), 
                                                 100)
    meta['ProdID'] = res
    if self.prodid:
      meta['ProdID'] = self.prodid
    self.log.info("Using %s as metadata" % (meta))
    #res = self.fc.getCompatibleMetadata(meta)
    #if not res['OK']:
    #  return res
    #compatmeta = res['Value']
    #if not self.prodid:
    #  if compatmeta.has_key('ProdID'):
    #    #take the latest prodID as
    #    list = compatmeta['ProdID']
    #    list.sort()
    #    self.prodid=list[-1]
    #  else:
    #    return S_ERROR("Could not determine ProdID from compatible metadata")
    #meta['ProdID']=self.prodid
    #refetch the compat metadata to get nb of events

    #res = self.fc.getCompatibleMetadata(meta)
    #if not res['OK']:
    #  return res
    #compatmeta = res['Value']
    #if compatmeta.has_key('NumberOfEvents'):
    #  if type(compatmeta['NumberOfEvents'])==type([]):
    #    self.nbofeventsperfile = compatmeta['NumberOfEvents'][0]
    #  elif type(compatmeta['NumberOfEvents']) in types.StringTypes:
    #    self.nbofeventsperfile = compatmeta['NumberOfEvents']
    #else:
    #  return S_ERROR("Number of events could not be determined, cannot proceed.")
    
    ##Below might still be needed
    #if self.site == "LCG.CERN.ch":
    #  return self.__getFilesFromCastor(meta)
#    elif   self.site == "LCG.IN2P3-CC.fr": ##but not this
#      return self.__getFilesFromLyon(meta) ## nor this
    #else:
    return self.fc.findFilesByMetadata(meta)

  def __getFilesFromLyon(self, meta):
    """ List the files present at Lyon, not used.
    """
    ProdID = meta['ProdID']
    prod = str(ProdID).zfill(8)
    energy = meta['Energy']
    bkg = meta["EvtType"]
    detector = meta["DetectorType"]
    path ="/ilc/prod/clic/%s/%s/%s/SIM/%s/" % (energy, bkg, detector, prod)
    comm = ["nsls", "%s" % path]
    res = subprocess.Popen(comm, stdout = subprocess.PIPE).communicate()
    dirlist = res[0].rstrip().split("\n")
    mylist = []
    for mydir in dirlist:
      if mydir.count("dirac_directory"):
        continue
      curdir = path + mydir
      comm2 = ["nsls", curdir]
      res = subprocess.Popen(comm2, stdout = subprocess.PIPE).communicate()
      for f in res[0].rstrip().split("\n"):
        if f.count("dirac_directory"):
          continue
        mylist.append(path + mydir + "/" + f)
    if not mylist:
      return S_ERROR("File list is empty")
    return S_OK(mylist)

  def __getFilesFromCastor(self, meta):
    """ Get the available files (list) from the CERN castor storage
    """ 
    ProdID = meta['ProdID']
    prod = str(ProdID).zfill(8)
    energy = meta['Energy']
    bkg = meta["EvtType"]
    detector = meta["DetectorType"]
    path = "/castor/cern.ch/grid/ilc/prod/%s/%s/%s/%s/SIM/%s/" % (self.machine, energy, bkg, detector, prod)
    comm = ["nsls", "%s" % path]
    res = subprocess.Popen(comm, stdout = subprocess.PIPE).communicate()
    dirlist = res[0].rstrip().split("\n")
    mylist = []
    for mydir in dirlist:
      if mydir.count("dirac_directory"):
        continue
      curdir = path + mydir
      comm2 = ["nsls", curdir]
      res = subprocess.Popen(comm2, stdout = subprocess.PIPE).communicate()
      for f in res[0].rstrip().split("\n"):
        if f.count("dirac_directory"):
          continue
        mylist.append(path + mydir + "/" + f)
    if not mylist:
      return S_ERROR("File list is empty")
    return S_OK(mylist)

  def __getFilesLocaly(self):
    """ Download the files.
    """
    numberofeventstoget = ceil(self.BXOverlay * self.ggtohadint)
    nbfiles = len(self.lfns)
    availableevents = nbfiles * self.nbofeventsperfile
    if availableevents < numberofeventstoget:
      return S_ERROR("Number of %s events available is less than requested" % ( self.BkgEvtType ))

    if not self.NbSigEvtsPerJob:
      ##Compute Nsignal events
      self.NbSigEvtsPerJob = self.nbinputsigfile * self.nbsigeventsperfile
    if not self.NbSigEvtsPerJob:
      return S_ERROR('Could not determine the number of signal events per job')
    self.log.verbose("There are %s signal event" % self.NbSigEvtsPerJob)
    ##Now determine how many files are needed to cover all signal events
    totnboffilestoget = int(ceil(self.NbSigEvtsPerJob * numberofeventstoget / self.nbofeventsperfile))

    ##Limit ourself to some configuration maximum
    maxNbFilesToGet = self.ops.getValue("/Overlay/MaxNbFilesToGet", 20)
    if totnboffilestoget > maxNbFilesToGet:
      totnboffilestoget = maxNbFilesToGet
#    res = self.ops.getOption("/Overlay/MaxConcurrentRunning",200)
#    self.log.verbose("Will allow only %s concurrent running"%res['Value'])
#    max_concurrent_running = res['Value']
#
#    jobpropdict = {}
#    jobpropdict['ApplicationStatus'] = 'Getting overlay files'
#    res = self.ops.getSections("/Overlay/Sites/")
#    sites = []
#    if res['OK']:
#      sites = res['Value']
#      self.log.verbose("Found the following sites to restrain: %s"%sites)
#    if self.site in sites:
#      res = self.ops.getOption("/Overlay/Sites/%s/MaxConcurrentRunning"%self.site,200)
#      self.log.verbose("Will allow only %s concurrent running at %s"%(res['Value'],self.site))
#      jobpropdict['Site']=self.site
#      max_concurrent_running = res['Value']
    if not os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      f = file('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK','w')
      f.write('Dont look at cpu')
      f.close()
    overlaymon = RPCClient('Overlay/Overlay', timeout=60)
    ##Now need to check that there are not that many concurrent jobs getting the overlay at the same time
    error_count = 0
    count = 0
    while 1:
      if error_count > 10 :
        self.log.error('OverlayDB returned too any errors')
        return S_ERROR('Failed to get number of concurrent overlay jobs')
      #jobMonitor = RPCClient('WorkloadManagement/JobMonitoring',timeout=60)
      #res = jobMonitor.getCurrentJobCounters(jobpropdict)
      #if not res['OK']:
      #  error_count += 1
      #  time.sleep(60)
      #  continue
      #running = 0
      #if res['Value'].has_key('Running'):
      #  running = res['Value']['Running']

      res = overlaymon.canRun(self.site)
      if not res['OK']:
        error_count += 1
        time.sleep(60)
        continue
      error_count = 0
      #if running < max_concurrent_running:
      if res['Value']:
        break
      else:
        count += 1
        if count > 300:
          return S_ERROR("Waited too long: 5h, so marking job as failed")
        if count % 10 == 0 :
          self.setApplicationStatus("Overlay standby number %s" % count)
        time.sleep(60)
        
    if os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')

    self.setApplicationStatus('Getting overlay files')

    self.log.info('Will obtain %s files for overlay' % totnboffilestoget)

    os.mkdir("./overlayinput_" + self.BkgEvtType)
    os.chdir("./overlayinput_" + self.BkgEvtType)
    filesobtained = []
    usednumbers = []
    fail = False
    fail_count = 0

    max_fail_allowed = self.ops.getValue("/Overlay/MaxFailedAllowed", 20)
    while not len(filesobtained) == totnboffilestoget:
      if fail_count > max_fail_allowed:
        fail = True
        break

      ##Now wait for a random time around 3 minutes
      ###Actually, waste CPU time !!!
      self.log.verbose("Waste happily some CPU time (on average 3 minutes)")
      res = WasteCPUCycles(60 * random.gauss(3, 0.1))
      if not res['OK']:
        self.log.error("Could not waste as much CPU time as wanted, but whatever!")

      fileindex = random.randrange(nbfiles)
      if fileindex not in usednumbers:
          
        usednumbers.append(fileindex)

        isDefault = False

        if self.site == 'LCG.CERN.ch':
          res = self.getCASTORFile(self.lfns[fileindex])
        elif self.site == 'LCG.IN2P3-CC.fr':
          res = self.getLyonFile(self.lfns[fileindex])
        elif self.site == 'LCG.UKI-LT2-IC-HEP.uk':
          res = self.getImperialFile(self.lfns[fileindex])
        elif  self.site == 'LCG.RAL-LCG2.uk':
          res = self.getRALFile(self.lfns[fileindex])
        else:
          if not os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
            f = file('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w')
            f.write('Dont look at cpu')
            f.close()
          res = self.rm.getFile(self.lfns[fileindex])
          isDefault = True

        # Tue Jun 28 14:21:03 CEST 2011
        # Temporarily for Imperial College site until dCache is fixed

        if (not res['OK']) and (not isDefault) and \
          (self.site in ['LCG.UKI-LT2-IC-HEP.uk', 'LCG.IN2P3-CC.fr', 'LCG.CERN.ch']):
          res = self.rm.getFile(self.lfns[fileindex])

        if not res['OK']:
          self.log.warn('Could not obtain %s' % self.lfns[fileindex])
          fail_count += 1
          continue
        
        if res['Value'].has_key('Failed'):
          if len(res['Value']['Failed']):
            self.log.warn('Could not obtain %s' % self.lfns[fileindex])
            fail_count += 1
            continue
        filesobtained.append(self.lfns[fileindex])
      ##If no file could be obtained, need to make sure the job fails  
      if len(usednumbers) == nbfiles and not len(filesobtained):
        fail = True
        break
      
    #res = self.rm.getFile(filesobtained)
    #failed = len(res['Value']['Failed'])
    #tryagain = []
    #if failed:
    #  self.log.error('Had issues getting %s files, retrying now with new files'%failed)
    #  while len(tryagain) < failed:
    #    fileindex = random.randrange(nbfiles)
    #    if fileindex not in usednumbers:
    #      usednumbers.append(fileindex)
    #      tryagain.append(self.lfns[fileindex])
    #  res = self.rm.getFile(tryagain)
    #  if len(res['Value']['Failed']):
    #    os.chdir(curdir)
    #    return S_ERROR("Could not obtain enough files after 2 attempts")
    ## Remove all scripts remaining
    scripts = glob.glob("*.sh")
    for script in scripts:
      os.remove(script)
      
    ##Print the file list
    mylist = os.listdir(os.getcwd())
    self.log.info("List of Overlay files:")
    self.log.info(string.join(mylist, "\n"))
    os.chdir(self.curdir)
    res = overlaymon.jobDone(self.site)
    if not res['OK']:
      self.log.error("Could not declare the job as finished getting the files")
    if fail:
      self.log.error("Did not manage to get all files needed, too many errors")
      return S_ERROR("Failed to get files")
    self.log.info('Got all files needed.')
    return S_OK()

  def getCASTORFile(self, lfn):
    """ USe xrdcp or rfcp to get the files from castor
    """
    prependpath = "/castor/cern.ch/grid"
    if not lfn.count("castor/cern.ch"):
      lfile = prependpath + lfn
    else:
      lfile = lfn
    self.log.info("Getting %s" % lfile)
    #command = "rfcp %s ./"%file

    basename = os.path.basename(lfile)

    if os.path.exists("overlayinput.sh"):
      os.unlink("overlayinput.sh")
    script = file("overlayinput.sh","w")
    script.write('#!/bin/sh \n')
    script.write('###############################\n')
    script.write('# Dynamically generated scrip #\n')
    script.write('###############################\n')
    script.write("cp %s /tmp/x509up_u%s \n" % (os.environ['X509_USER_PROXY'], os.getuid()))
    script.write('declare -x STAGE_SVCCLASS=ilcdata\n')
    script.write('declare -x STAGE_HOST=castorpublic\n')
    script.write("xrdcp -s root://castorpublic.cern.ch/%s ./ -OSstagerHost=castorpublic\&svcClass=ilcdata\n" % lfile.rstrip())
    #script.write("/usr/bin/rfcp 'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s' %s\n"%(lfile,basename))
    script.write("""
if [ ! -s %s ]; then
  echo "Using rfcp instead"
  rfcp %s ./
fi\n""" % (basename, lfile))
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    os.chmod("overlayinput.sh", 0755)
    comm = 'sh -c "./overlayinput.sh"'
    self.result = shellCall(600, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #comm7=["/usr/bin/rfcp","'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s'"%lfile,"file:%s"%basename]
    #try:
    #  res = subprocess.Popen(comm7,stdout=logfile,stderr=subprocess.STDOUT)
    #except Exception,x:
    #  print ("failed : %s %s"%(Exception,x))
    #logfile.close()
    #print res
    status = 0
    if not os.path.exists(os.path.basename(lfile)):
      status = 1

    mydict = {}
    mydict['Failed'] = []
    mydict['Successful'] = []
    if status:
      mydict['Failed'] = lfn
    else:
      mydict['Successful'] = lfn
      #return S_ERROR("Problem getting %s"%os.path.basename(lfn))
    return S_OK(mydict)

  def getLyonFile(self, lfn):
    """ Use xrdcp to get the files from Lyon
    """
    prependpath = '/pnfs/in2p3.fr/data'
    if not lfn.count('in2p3.fr/data'):
      lfile = prependpath + lfn
    else:
      lfile = lfn
    self.log.info("Getting %s" % lfile)
    #command = "rfcp %s ./"%file
    #comm = []
    #comm.append("cp $X509_USER_PROXY /tmp/x509up_u%s"%os.getuid())

    if os.path.exists("overlayinput.sh"):
      os.unlink("overlayinput.sh")
    script = file("overlayinput.sh", "w")
    script.write('#!/bin/sh \n')
    script.write('###############################\n')
    script.write('# Dynamically generated scrip #\n')
    script.write('###############################\n')
    script.write("cp %s /tmp/x509up_u%s \n" % (os.environ['X509_USER_PROXY'], os.getuid()))
    script.write(". /afs/in2p3.fr/grid/profiles/lcg_env.sh\n")
    script.write("xrdcp root://ccdcacsn179.in2p3.fr:1094%s ./ -s\n" % lfile.rstrip())
    #script.write("/usr/bin/rfcp 'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s' %s\n"%(lfile,basename))
    #script.write("""
#if [ ! -s %s ]; then
#  rfcp %s ./
#fi\n"""%(basename,lfile))
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    os.chmod("overlayinput.sh", 0755)
    comm = 'sh -c "./overlayinput.sh"'
    self.result = shellCall(600, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
#    
#    if os.environ.has_key('X509_USER_PROXY'):
#      comm2 = ["cp", os.environ['X509_USER_PROXY'],"/tmp/x509up_u%s"%os.getuid()]
#      res = subprocess.Popen(comm2,stdout=subprocess.PIPE).communicate()
#      print res
    #comm.append("xrdcp root://ccdcacsn179.in2p3.fr:1094%s ./ -s"%file)
    #command = string.join(comm,";")
    #comm3 = ["xrdcp","root://ccdcacsn179.in2p3.fr:1094%s"%file,"./","-s"]
    #res = subprocess.Popen(comm3,stdout=subprocess.PIPE).communicate()
    #print res
    
    status = 0
    if not os.path.exists(os.path.basename(lfile)):
      status = 1
    #command2  = command.split()
    #res = subprocess.Popen(command2,stdout=subprocess.PIPE).communicate()
    #print res
    #self.result = shellCall(0,command,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #resultTuple = self.result['Value']
    #status = resultTuple[0]
    mydict = {}
    mydict['Failed'] = []
    mydict['Successful'] = []
    if status:
      return S_ERROR("Failed")
    else:
      mydict['Successful'] = lfn
      #return S_ERROR("Problem getting %s"%os.path.basename(lfn))
    return S_OK(mydict)

  def getImperialFile(self, lfn):
    """ USe dccp to get the files from the Imperial SE
    """
    prependpath = '/pnfs/hep.ph.ic.ac.uk/data'
    if not lfn.count('hep.ph.ic.ac.uk/data'):
      lfile = prependpath + lfn
    else:
      lfile = lfn
    self.log.info("Getting %s" % file)
    ###Don't check for CPU time as other wise, job can get killed
    if not os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      f = file('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w')
      f.write('Dont look at cpu')
      f.close()

    if os.path.exists("overlayinput.sh"):
      os.unlink("overlayinput.sh")
    script = file("overlayinput.sh","w")
    script.write('#!/bin/sh \n')
    script.write('###############################\n')
    script.write('# Dynamically generated scrip #\n')
    script.write('###############################\n')
    script.write("dccp dcap://%s%s ./\n" % (os.environ['VO_ILC_DEFAULT_SE'], lfile.rstrip()))
    #script.write("/usr/bin/rfcp 'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s' %s\n"%(lfile,basename))
    #script.write("""
#if [ ! -s %s ]; then
#  rfcp %s ./
#fi\n"""%(basename,lfile))
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    os.chmod("overlayinput.sh", 0755)
    comm = 'sh -c "./overlayinput.sh"'
    self.result = shellCall(600, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
#    
    #command = "rfcp %s ./"%file
    #comm = []
    #comm.append("cp $X509_USER_PROXY /tmp/x509up_u%s"%os.getuid())
    #if os.environ.has_key('X509_USER_PROXY'):
    #  comm2 = ["cp", os.environ['X509_USER_PROXY'],"/tmp/x509up_u%s"%os.getuid()]
    #  res = subprocess.Popen(comm2,stdout=subprocess.PIPE).communicate()
    #  print res
    #comm.append("xrdcp root://ccdcacsn179.in2p3.fr:1094%s ./ -s"%file)
    #command = string.join(comm,";")
    #comm3 = ["dccp","dcap://%s%s" % ( os.environ['VO_ILC_DEFAULT_SE'], file ),"./"]
    #res = subprocess.Popen(comm3,stdout=subprocess.PIPE).communicate()
    #print res
    status = 0
    if not os.path.exists(os.path.basename(lfile)):
      status = 1
    #command2  = command.split()
    #res = subprocess.Popen(command2,stdout=subprocess.PIPE).communicate()
    #print res
    #self.result = shellCall(0,command,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #resultTuple = self.result['Value']
    #status = resultTuple[0]
    mydict = {}
    mydict['Failed'] = []
    mydict['Successful'] = []
    if status:
      return S_ERROR("Failed")
    else:
      mydict['Successful'] = lfn
      #return S_ERROR("Problem getting %s"%os.path.basename(lfn))
    return S_OK(mydict)

  def getRALFile(self, lfn):
    """ Use rfcp to get the files from RAL castor
    """
    prependpath = '/castor/ads.rl.ac.uk/prod'
    if not lfn.count('ads.rl.ac.uk/prod'):
      lfile = prependpath + lfn
    else:
      lfile = lfn
    self.log.info("Getting %s" % lfile)
    ###Don't check for CPU time as other wise, job can get killed
    if not os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      f = file('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK','w')
      f.write('Dont look at cpu')
      f.close()
    
    #command = "rfcp %s ./"%file
    #comm = []
    #comm.append("cp $X509_USER_PROXY /tmp/x509up_u%s"%os.getuid())
    if os.environ.has_key('X509_USER_PROXY'):
      comm2 = ["cp", os.environ['X509_USER_PROXY'],"/tmp/x509up_u%s" % os.getuid()]
      res = subprocess.Popen(comm2, stdout = subprocess.PIPE).communicate()
      print res
    #comm.append("xrdcp root://ccdcacsn179.in2p3.fr:1094%s ./ -s"%file)
    #command = string.join(comm,";")
    #logfile = file(self.applicationLog,"w")
    os.environ['CNS_HOST'] = 'castorns.ads.rl.ac.uk'
    #comm4= ['declare','-x','CNS_HOST=castorns.ads.rl.ac.uk']
    #res = subprocess.Popen(comm4,stdout=logfile,stderr=subprocess.STDOUT)
    #print res
    os.environ['STAGE_SVCCLASS'] = 'ilcTape'
#     comm5= ['declare','-x','STAGE_SVCCLASS=ilcTape']
#      res = subprocess.call(comm5)
#      print res
    os.environ['STAGE_HOST'] = 'cgenstager.ads.rl.ac.uk'
#      comm6=['declare','-x','STAGE_HOST=cgenstager.ads.rl.ac.uk']
#      res = subprocess.call(comm6)
#      print res
    basename = os.path.basename(lfile)

    if os.path.exists("overlayinput.sh"):
      os.unlink("overlayinput.sh")
    script = file("overlayinput.sh","w")
    script.write('#!/bin/sh \n')
    script.write('###############################\n')
    script.write('# Dynamically generated scrip #\n')
    script.write('###############################\n')
    script.write("/usr/bin/rfcp 'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s' %s\n" % (lfile, basename))
    script.write('declare -x appstatus=$?\n')
    script.write('exit $appstatus\n')
    script.close()
    os.chmod("overlayinput.sh", 0755)
    comm = 'sh -c "./overlayinput.sh"'
    self.result = shellCall(600, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #comm7=["/usr/bin/rfcp","'rfio://cgenstager.ads.rl.ac.uk:9002?svcClass=ilcTape&path=%s'"%lfile,"file:%s"%basename]
    #try:
    #  res = subprocess.Popen(comm7,stdout=logfile,stderr=subprocess.STDOUT)
    #except Exception,x:
    #  print ("failed : %s %s"%(Exception,x))
    #logfile.close()
    #print res
    status = 0
    if not os.path.exists(os.path.basename(lfile)):
      status = 1
    #command2  = command.split()
    #res = subprocess.Popen(command2,stdout=subprocess.PIPE).communicate()
    #print res
    #self.result = shellCall(0,command,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #resultTuple = self.result['Value']
    #status = resultTuple[0]
    mydict = {}
    mydict['Failed'] = []
    mydict['Successful'] = []
    if status:
      return S_ERROR("Failed")
    else:
      mydict['Successful'] = lfn
      #return S_ERROR("Problem getting %s"%os.path.basename(lfn))
    return S_OK(mydict)

  def execute(self):
    """ Run the module, called rom Workflow
    """
    self.result = self.resolveInputVariables()
    if not self.result['OK']:
      return self.result

    if not self.applicationLog:
      self.applicationLog = 'Overlay_input.log'
    self.applicationLog = os.path.join(os.getcwd(), self.applicationLog)

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('OverlayInput should not proceed as previous step did not end properly')
    self.setApplicationStatus('Starting up Overlay')
    res = self.__getFilesFromFC()
    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get file list')
      return res

    self.lfns = res['Value']
    if not len(self.lfns):
      self.setApplicationStatus('OverlayProcessor got an empty list')
      return S_ERROR('OverlayProcessor got an empty list')


    res = self.__getFilesLocaly()
    ###Now that module is finished,resume CPU time checks
    if os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')

    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get files locally with message %s' % res['Message'])
      return S_ERROR('OverlayProcessor failed to get files locally')
    self.setApplicationStatus('Overlay processor finished getting all files successfully')
    return S_OK('Overlay input finished successfully')

