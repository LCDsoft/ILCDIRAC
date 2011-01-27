'''
Created on Jan 27, 2011

@author: sposs
'''
from ILCDIRAC.Workflow.Modules.ModuleBase                    import ModuleBase
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
from math import ceil

import os

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
    self.lfns = []
    self.nbfilestoget = 0
    self.bxoverlay = 0
    self.ggtohadint = 3.3
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
    if len(compatmeta['ProdID'])>1:
      meta['ProdID']=compatmeta['ProdID'][0]
      
    return self.fc.findFilesByMetadata(meta)

  def __getFilesLocaly(self):
    
    numberofeventstoget = ceil(self.bxoverlay*self.ggtohadint)
    
    
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
    
    
    
    
    return S_OK('Overlay input finished successfully')
  