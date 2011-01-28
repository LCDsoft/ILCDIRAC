'''
Created on Jan 27, 2011

@author: sposs
'''
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from ILCDIRAC.Core.Utilities.InputFilesUtilities import getNumberOfevents

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
from math import ceil
from random import randrange

import os,types

class OverlayInput (ModuleBase):
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.log = gLogger.getSubLogger( "OverlayInput" )
    self.result = S_ERROR()
    self.jobID = None
    self.applicationName = 'OverlayInput'
    self.printoutflag = ''
    self.prodid = 0
    self.detector = ""
    self.energy='3tev'
    self.nbofeventsperfile = 100
    self.lfns = []
    self.nbfilestoget = 0
    self.bxoverlay = 0
    self.ggtohadint = 3.3
    self.InputData = ''
    self.nbsigeventsperfile = 0
    self.nbinputsigfile=1
    self.rm = ReplicaManager()
    self.fc = FileCatalogClient()
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

  def resolveInputVariables(self):
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
      
    if self.workflow_commons.has_key('InputData'):
      self.InputData = self.workflow_commons['InputData']

    if self.InputData:
      res = getNumberOfevents(self.InputData)
      if res.has_key("nbevts"):
        self.nbsigeventsperfile=res["nbevts"]
      else:
        return S_ERROR("Could not find number of signal events per input file")
      self.nbinputsigfile = len(self.InputData.split(";"))
    return S_OK("Input variables resolved")

  def __getFilesFromFC(self):
    meta = {}
    meta['Energy']=self.energy
    meta['EvtType']='gghad'
    meta['Datatype']='SIM'
    meta['DetectorType']=self.detector
    res = self.fc.getCompatibleMetadata(meta)
    if not res['OK']:
      return res
    compatmeta = res['Value']
    if not self.prodid:
      if compatmeta.has_key('ProdID'):
        #take the latest prodID as 
        sortedlist = compatmeta['ProdID'].sort()
        self.prodid=sortedlist[-1]
      else:
        return S_ERROR("Could not determine ProdID from compatible metadata")  
    meta['ProdID']=self.prodid
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
    return self.fc.findFilesByMetadata(meta)

  def __getFilesLocaly(self):
    
    numberofeventstoget = ceil(self.bxoverlay*self.ggtohadint)
    nbfiles = len(self.lfns)
    availableevents = nbfiles*self.nbofeventsperfile
    if availableevents < numberofeventstoget:
      return S_ERROR("Number of gg->had events available is less than requested")
    nboffilestogetpersigevt = ceil(numberofeventstoget/self.nbofeventsperfile)
    
    ##Compute Nsignal events
    nsigevts = self.nbinputsigfile*self.nbsigeventsperfile
    
    ##Get Number of files to get to cover all signal events
    totnboffilestoget = nsigevts*nboffilestogetpersigevt
    ##Limit ourself to 15 files
    if totnboffilestoget>15:
      totnboffilestoget=15
 
    curdir = os.getcwd()
    os.mkdir("./overlayinput")
    os.chdir("./overlayinput")
    filesobtained = []
    for i in range(totnboffilestoget):
      filesobtained.append(self.lfns[randrange(nbfiles)])
    res = self.rm.getFile(filesobtained)
    failed = len(res['Value']['Failed'])
    tryagain = []
    if failed:
      for i in failed:
        tryagain.append(self.lfns[randrange(nbfiles)])
      res = self.rm.getFile(tryagain)
      if len(res['Value']['Failed']):
        os.chdir(curdir)
        return S_ERROR("Could not obtain enough files after 2 attempts")
    os.chdir(curdir)
    return S_OK()

  def execute(self):
    self.result =self.resolveInputVariables()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('OverlayInput should not proceed as previous step did not end properly')
    self.setApplicationStatus('Getting file list')
    res = self.__getFilesFromFC()
    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get file list')
      return S_ERROR('OverlayProcessor failed to get file list')

    self.lfns=  res['Value']
    if not len(self.lfns):
      self.setApplicationStatus('OverlayProcessor got an empty list')
      return S_ERROR('OverlayProcessor got an empty list')
    
    
    res = self.__getFilesLocaly()
    if not res['OK']:
      self.setApplicationStatus('OverlayProcessor failed to get files locally with message %s'%res['Message'])
      return S_ERROR('OverlayProcessor failed to get files locally')
    
    return S_OK('Overlay input finished successfully')
  