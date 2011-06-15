#####################################################
# $HeadURL: $
#####################################################
'''
Created on Jan 27, 2011

@author: sposs
'''

__RCSID__ = "$Id: $"

from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from DIRAC.DataManagementSystem.Client.ReplicaManager        import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalogClient               import FileCatalogClient
from ILCDIRAC.Core.Utilities.InputFilesUtilities             import getNumberOfevents
from DIRAC.Core.DISET.RPCClient                              import RPCClient
from DIRAC.Core.Utilities.Subprocess                         import shellCall

from DIRAC                                                   import S_OK, S_ERROR, gLogger, gConfig
from math import ceil

import os,types,time,random, string, subprocess

class OverlayInput (ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "OverlayInput" )
    self.applicationName = 'OverlayInput'
    self.applicationLog = 'Overlay.log'
    self.printoutflag = ''
    self.prodid = 0
    self.detector = ""
    self.energy='3tev'
    self.nbofeventsperfile = 100
    self.lfns = []
    self.nbfilestoget = 0
    self.evttype= 'gghad'
    self.bxoverlay = 0
    self.ggtohadint = 3.2
    self.nbsigeventsperfile = 0
    self.nbinputsigfile=1
    self.nsigevts = 0
    self.rm = ReplicaManager()
    self.fc = FileCatalogClient()
    self.site = ''


  def applicationSpecificInputs(self):
    for key,val in self.step_commons.items():
      self.log.info("%s=%s" % (key, val))
    if self.step_commons.has_key('Detector'):
      self.detector = self.step_commons['Detector']
    else:
      return S_ERROR('Detector model not defined')
    
    if self.step_commons.has_key('Energy'):
      self.energy = self.step_commons['Energy']
      
    if self.step_commons.has_key('BXOverlay'):
      self.bxoverlay = self.step_commons['BXOverlay']
    else:
      return S_ERROR("BXOverlay parameter not defined")
    
    if self.step_commons.has_key('ggtohadint'):
      self.ggtohadint = self.step_commons['ggtohadint']
      
    if self.step_commons.has_key('ProdID'):
      self.prodid = self.step_commons['ProdID']
    
    if self.step_commons.has_key('NbSigEvtsPerJob'):
      self.nsigevts = self.step_commons['NbSigEvtsPerJob']
    
    if self.step_commons.has_key('BkgEvtType'):
      self.evttype = self.step_commons['BkgEvtType']  
    
    if self.workflow_commons.has_key('Site'):
      self.site = self.workflow_commons['Site']  
      
    if len(self.InputData) > 2 : 
      res = getNumberOfevents(self.InputData)
      if res.has_key("nbevts"):
        self.nbsigeventsperfile = res["nbevts"]
      else:
        return S_ERROR("Could not find number of signal events per input file")
      self.nbinputsigfile = len(self.InputData.split(";"))
    if not self.nsigevts and not self.nbsigeventsperfile:
      return S_ERROR("Could not determine the number of signal events per input file")
    return S_OK("Input variables resolved")

  def __getFilesFromFC(self):
    meta = {}
    meta['Energy']=self.energy
    meta['EvtType']=self.evttype
    meta['Datatype']='SIM'
    meta['DetectorType']=self.detector
    
    res= gConfig.getOption("/Operations/Overlay/%s/%s/ProdID"%(self.detector,self.energy),0)
    meta['ProdID']= res['Value']
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
    res = self.fc.getCompatibleMetadata(meta)
    if not res['OK']:
      return res
    compatmeta = res['Value']      
    if compatmeta.has_key('NumberOfEvents'):
      if type(compatmeta['NumberOfEvents'])==type([]):
        self.nbofeventsperfile = compatmeta['NumberOfEvents'][0]
      elif type(compatmeta['NumberOfEvents']) in types.StringTypes:
        self.nbofeventsperfile = compatmeta['NumberOfEvents']
    else:
      return S_ERROR("Number of events could not be determined, cannot proceed.")    
    if self.site == "LCG.CERN.ch":
      return self.__getFilesFromCastor(meta)
#    elif   self.site == "LCG.IN2P3-CC.fr":
#      return self.__getFilesFromLyon(meta)
    else:
      return self.fc.findFilesByMetadata(meta)

  def __getFilesFromLyon(self,meta):
    list = []
    ProdID= meta['ProdID']
    prod = str(ProdID).zfill(8)
    energy = meta['Energy']
    bkg = meta["EvtType"]
    detector = meta["DetectorType"]
    path ="/ilc/prod/clic/%s/%s/%s/SIM/%s/"%(energy,bkg,detector,prod)
    comm = ["nsls","%s"%path]
    res = subprocess.Popen(comm,stdout=subprocess.PIPE).communicate()
    dirlist = res[0].rstrip().split("\n")
    list = []
    for dir in dirlist:
      if dir.count("dirac_directory"):
        continue
      curdir = path+dir
      comm2 = ["nsls",curdir]
      res = subprocess.Popen(comm2,stdout=subprocess.PIPE).communicate()
      for f in res[0].rstrip().split("\n"):
        if f.count("dirac_directory"):
          continue
        list.append(path+dir+"/"+f)    
    if not list:
      return S_ERROR("File list is empty")        
    return S_OK(list)

  def __getFilesFromCastor(self,meta):
    ProdID= meta['ProdID']
    prod = str(ProdID).zfill(8)
    energy = meta['Energy']
    bkg = meta["EvtType"]
    detector = meta["DetectorType"]
    path = "/castor/cern.ch/grid/ilc/prod/clic/%s/%s/%s/SIM/%s/"%(energy,bkg,detector,prod)
    comm = ["nsls","%s"%path]
    res = subprocess.Popen(comm,stdout=subprocess.PIPE).communicate()
    dirlist = res[0].rstrip().split("\n")
    list = []
    for dir in dirlist:
      if dir.count("dirac_directory"):
        continue
      curdir = path+dir
      comm2 = ["nsls",curdir]
      res = subprocess.Popen(comm2,stdout=subprocess.PIPE).communicate()
      for f in res[0].rstrip().split("\n"):
        if f.count("dirac_directory"):
          continue
        list.append(path+dir+"/"+f)
    if not list:
      return S_ERROR("File list is empty")    
    return S_OK(list)

  def __getFilesLocaly(self):
    
    numberofeventstoget = ceil(self.bxoverlay*self.ggtohadint)
    nbfiles = len(self.lfns)
    availableevents = nbfiles*self.nbofeventsperfile
    if availableevents < numberofeventstoget:
      return S_ERROR("Number of gg->had events available is less than requested")

    if not self.nsigevts:
      ##Compute Nsignal events
      self.nsigevts = self.nbinputsigfile*self.nbsigeventsperfile
    if not self.nsigevts:
      return S_ERROR('Could not determine the number of signal events per job')
    
    ##Now determine how many files are needed to cover all signal events
    totnboffilestoget = int(ceil(self.nsigevts*numberofeventstoget/self.nbofeventsperfile))
        
    ##Limit ourself to some configuration maximum
    res = gConfig.getOption("/Operations/Overlay/MaxNbFilesToGet",20)    
    maxNbFilesToGet = res['Value']
    if totnboffilestoget>maxNbFilesToGet+1:
      totnboffilestoget=maxNbFilesToGet+1
    res = gConfig.getOption("/Operations/Overlay/MaxConcurrentRunning",200)
    self.log.verbose("Will allow only %s concurrent running"%res['Value'])
    max_concurrent_running = res['Value']

    ##Now need to check that there are not that many concurrent jobs getting the overlay at the same time
    error_count = 0
    count = 0
    while 1 and not (self.site=='LCG.CERN.ch' or self.site=="LCG.IN2P3-CC.fr"):
      if error_count > 10 :
        self.log.error('JobDB Content does not return expected dictionary')
        return S_ERROR('Failed to get number of concurrent overlay jobs')
      jobMonitor = RPCClient('WorkloadManagement/JobMonitoring',timeout=60)
      res = jobMonitor.getCurrentJobCounters({'ApplicationStatus':'Getting overlay files'})
      if not res['OK']:
        error_count += 1 
        time.sleep(60)
        continue
      running = 0
      if res['Value'].has_key('Running'):
        running = res['Value']['Running']
      if running < max_concurrent_running:
        break
      else:
        count += 1
        if count>300:
          return S_ERROR("Waited too long: 5h, so marking job as failed")
        self.setApplicationStatus("Overlay standby nb %s"%count)        
        time.sleep(60)
    self.setApplicationStatus('Getting overlay files')

    self.log.info('Will obtain %s files for overlay'%totnboffilestoget)
    
    curdir = os.getcwd()
    os.mkdir("./overlayinput_"+self.evttype)
    os.chdir("./overlayinput_"+self.evttype)
    filesobtained = []
    usednumbers = []
      
    while len(filesobtained) < totnboffilestoget:
      fileindex = random.randrange(nbfiles)
      if fileindex not in usednumbers:        
        usednumbers.append(fileindex)
        if self.site =='LCG.CERN.ch':
          res = self.getCASTORFile(self.lfns[fileindex])
        elif self.site=='LCG.IN2P3-CC.fr':
          res = self.getLyonFile(self.lfns[fileindex])  
        else:  
          res = self.rm.getFile(self.lfns[fileindex])
        if not res['OK']:
          self.log.warn('Could not obtain %s'%self.lfns[fileindex])
          time.sleep(60*random.gauss(3,0.1))     
          continue
        if len(res['Value']['Failed']):
          self.log.warn('Could not obtain %s'%self.lfns[fileindex])
          time.sleep(60*random.gauss(3,0.1))     
          continue
        filesobtained.append(self.lfns[fileindex])
        ##Now wait for a random time around 3 minutes
        time.sleep(60*random.gauss(3,0.1))
        
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
    ##Print the file list
    list = os.listdir(os.getcwd())
    self.log.info("List of Overlay files:")
    self.log.info(string.join(list,"\n"))
    os.chdir(curdir)
    self.log.info('Got all files needed.')
    return S_OK()

  def getCASTORFile(self,lfn):
    prependpath = "/castor/cern.ch/grid"
    if not lfn.count("castor/cern.ch"):
      file = prependpath+lfn
    else: 
      file = lfn
    self.log.info("Getting %s"%file)  
    #command = "rfcp %s ./"%file
    comm = []
    comm.append("cp $X509_USER_PROXY /tmp/x509up_u%s"%os.getuid())
    comm.append("xrdcp root://castorpublic.cern.ch/%s ./ -OSstagerHost=castorpublic&svcClass=ilcdata -s"%file)
    command = string.join(comm,";")
    self.result = shellCall(0,command,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    if status:
      comm = []
      comm.append('declare -x STAGE_SVCCLASS=ilcdata')      
      comm.append('declare -x STAGE_HOST=castorpublic')
      comm.append('rfcp %s ./'%file)
      command = string.join(comm,";")

      self.result = shellCall(0,command,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
      resultTuple = self.result['Value']
      status = resultTuple[0]
      
    dict = {}
    dict['Failed'] = []
    dict['Successful'] = []
    if status:
      dict['Failed']=lfn 
    else:
      dict['Successful']=lfn  
      #return S_ERROR("Problem getting %s"%os.path.basename(lfn))
    return S_OK(dict)

  def getLyonFile(self,lfn):
    prependpath = '/pnfs/in2p3.fr/data'
    if not lfn.count('in2p3.fr/data'):
      file = prependpath+lfn
    else: 
      file = lfn
    self.log.info("Getting %s"%file)  
    #command = "rfcp %s ./"%file
    comm = []
    comm.append("cp $X509_USER_PROXY /tmp/x509up_u%s"%os.getuid())
    comm.append("xrdcp root://ccdcacsn179.in2p3.fr:1094%s ./ -s"%file)
    command = string.join(comm,";")
    self.result = shellCall(0,command,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]  
    dict = {}
    dict['Failed'] = []
    dict['Successful'] = []
    if status:
      dict['Failed']=lfn 
    else:
      dict['Successful']=lfn  
      #return S_ERROR("Problem getting %s"%os.path.basename(lfn))
    return S_OK(dict)

  def execute(self):
    self.result =self.resolveInputVariables()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('OverlayInput should not proceed as previous step did not end properly')
    self.setApplicationStatus('Starting up Overlay')
    res = self.__getFilesFromFC()
    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get file list')
      return res

    self.lfns=  res['Value']
    if not len(self.lfns):
      self.setApplicationStatus('OverlayProcessor got an empty list')
      return S_ERROR('OverlayProcessor got an empty list')
    
    ###Don't check for CPU time as other wise, job can get killed
    if os.path.exists('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK'):
      os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')
    f = file('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK','w')
    f.write('Dont look at cpu')
    f.close()
    
    res = self.__getFilesLocaly()
    ###Now that module is finished,resume CPU time checks
    os.remove('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK')
    
    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get files locally with message %s'%res['Message'])
      return S_ERROR('OverlayProcessor failed to get files locally')
    self.setApplicationStatus('Overlay processor finished getting all files successfully')
    return S_OK('Overlay input finished successfully')
  