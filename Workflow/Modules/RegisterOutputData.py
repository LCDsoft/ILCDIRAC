#####################################################
# $HeadURL$
#####################################################
'''
Register the meta data of the production files

@since: Sep 8, 2010

@author: sposs
'''
__RCSID__ = "$Id$"

from ILCDIRAC.Workflow.Modules.ModuleBase         import ModuleBase
from DIRAC.Resources.Catalog.FileCatalogClient    import FileCatalogClient
from DIRAC.Core.Utilities                         import DEncode
from DIRAC import S_OK, gLogger
import string, os

class RegisterOutputData( ModuleBase ):
  """ At the end of a production Job, we need to register meta data info for the files. 
  """
  def __init__(self):
    super(RegisterOutputData, self).__init__()
    self.version = "RegisterOutputData v1"
    self.log = gLogger.getSubLogger( "RegisterOutputData" )
    self.commandTimeOut = 10*60
    self.enable = True
    self.prodOutputLFNs = []
    self.swpackages = []
    self.nbofevents = 0
    self.luminosity = 0
    self.add_info = ''
    self.filecatalog = FileCatalogClient()

  def applicationSpecificInputs(self):
    if self.step_commons.has_key('Enable'):
      self.enable = self.step_commons['Enable']
      if not type(self.enable) == type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False
        
    if self.workflow_commons.has_key('ProductionOutputData'):
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData'].split(";")
    else:
      self.prodOutputLFNs = []
      
    if self.workflow_commons.has_key('SoftwarePackages'):
      self.swpackages = self.workflow_commons['SoftwarePackages'].split(";")

    if self.workflow_commons.has_key('NbOfEvts'):
      self.nbofevents = self.workflow_commons[ 'NbOfEvts']    
    if self.workflow_commons.has_key('NbOfEvents'):
      self.nbofevents = self.workflow_commons['NbOfEvents']
    if self.workflow_commons.has_key('Luminosity'):
      self.luminosity = self.workflow_commons['Luminosity']
    
    ##Additional info: cross section only for the time being
    if self.workflow_commons.has_key('Info'):
      self.add_info = DEncode.encode(self.workflow_commons['Info'])
    
      
    return S_OK('Parameters resolved')
  
  def execute(self):
    """ Run the module
    """
    self.log.info('Initializing %s' % self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('No registration of output data metadata attempted')

    if len(self.prodOutputLFNs) == 0:
      self.log.info('No production data found, so no metadata registration to be done')  
      return S_OK("No files' metadata to be registered")
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s" % string.join(self.prodOutputLFNs, 
                                                                                                 "\n"))

    for files in self.prodOutputLFNs:
      elements = files.split("/")
      metaprodid = {}
      metafiles = {}
      meta = {}  
      metamach = {}
      metamach['Machine'] = elements[3]
      meta.update(metamach)
      machine = string.join(elements[0:4], "/")
      res = S_OK()
      if self.enable:
        #res = self.filecatalog.setMetadata(machine,metamach)
        if not res['OK']:
          self.log.error('Could not register metadata Machine, with value %s for %s' % (elements[3], machine))
          return res
      metaen = {}
      metaen['Energy'] = elements[4]
      meta.update(metaen)
      energy = string.join(elements[0:5], "/")
      if self.enable:
        #res = self.filecatalog.setMetadata(energy,metaen)
        if not res['OK']:
          self.log.error('Could not register metadata Energy, with value %s for %s' % (elements[4], energy))
          return res      
      metaevt = {}
      metaevt['EvtType'] = elements[5]
      meta.update(metaevt)
      evttype = string.join(elements[0:6], "/")
      if self.enable:
        #res = self.filecatalog.setMetadata(evttype,metaevt)
        if not res['OK']:
          self.log.error('Could not register metadata EvtType, with value %s for %s' % (elements[5], evttype))
          return res
      prodid = ''
      if elements[6].lower() == 'gen':
        metadat = {}
        metadat['Datatype'] = elements[6]
        meta.update(metadat)
        datatype = string.join(elements[0:7], "/")
        if self.enable:
          #res = self.filecatalog.setMetadata(datatype,metadat)
          if not res['OK']:
            self.log.error('Could not register metadata Datatype, with value %s for %s' % (elements[6], datatype))
            return res
        metaprodid['ProdID'] = elements[7]
        meta.update(metaprodid)
        prodid = string.join(elements[0:8], "/")
        if self.enable:
          #res = self.filecatalog.setMetadata(prodid,metaprodid)
          if not res['OK']:
            self.log.error('Could not register metadata ProdID, with value %s for %s' % (elements[7], prodid))
            return res
        
      else:
        metadet = {}
        metadet['DetectorType'] = elements[6]
        detectortype = string.join(elements[0:7], "/")
        if self.enable:
          #res = self.filecatalog.setMetadata(detectortype,metadet)
          if not res['OK']:
            self.log.error('Could not register metadata DetectorType, with value %s for %s' % (elements[6], 
                                                                                               detectortype))
            return res
        metadat = {}
        metadat['Datatype'] = elements[7]
        datatype = string.join(elements[0:8], "/")
        if self.enable:
          #res = self.filecatalog.setMetadata(datatype,metadat)
          if not res['OK']:
            self.log.error('Could not register metadata Datatype, with value %s for %s' % (elements[7], datatype))
            return res 
        metaprodid['ProdID'] = elements[8]
        prodid = string.join(elements[0:9], "/")
        if self.enable:
          #res = self.filecatalog.setMetadata(prodid,metaprodid)
          if not res['OK']:
            self.log.error('Could not register metadata ProdID, with value %s for %s' % (elements[8], prodid))
            return res
        
      if self.nbofevents:
        nbevts = {}
        nbevts['NumberOfEvents'] = self.nbofevents
        if self.workflow_commons.has_key('file_number_of_event_relation'):
          if self.workflow_commons['file_number_of_event_relation'].has_key(os.path.basename(files)):
            nbevts['NumberOfEvents'] = self.workflow_commons['file_number_of_event_relation'][os.path.basename(files)]
        if self.enable:
          res = self.filecatalog.setMetadata(files, nbevts)
          if not res['OK']:
            self.log.error('Could not register metadata NumberOfEvents, with value %s for %s' % (self.nbofevents, 
                                                                                                 files))
            return res
      if self.luminosity:
        lumi = {}
        lumi['Luminosity'] = self.luminosity
        if self.enable:
          res = self.filecatalog.setMetadata(files, lumi)
          if not res['OK']:
            self.log.error('Could not register metadata Luminosity, with value %s for %s' % (self.luminosity, 
                                                                                             files))
            return res
      if self.add_info:
        info = {}
        info['AdditionalInfo'] = self.add_info
        if self.enable:
          res = self.filecatalog.setMetadata(files, info)
          if not res['OK']:
            self.log.error('Could not register metadata Info, with value %s for %s' % (self.add_info, files))
            return res
      meta.update(metaprodid)
      meta.update(metafiles)
      self.log.info("Registered %s with tags %s" % (files, meta))
      
      ###Now, set the ancestors
      if self.InputData:
        if self.enable:
          res = self.filecatalog.addFileAncestors({ files : {'Ancestors' : self.InputData } })
          if not res['OK']:
            self.log.error('Registration of Ancestors for %s failed' % files)
            return res
      # FIXME: in next DIRAC release, remove loop and replace key,value below by meta  
      #res = self.filecatalog.setMetadata(os.path.dirname(files),meta)
      #if not res['OK']:
      #  self.log.error('Could not register metadata %s for %s'%(meta, files))
      #  return res
    
    return S_OK('Output data metadata registered in catalog')
  
  