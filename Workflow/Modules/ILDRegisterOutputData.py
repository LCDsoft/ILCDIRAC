#####################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/ILCDIRAC/trunk/ILCDIRAC/Workflow/Modules/RegisterOutputData.py $
#####################################################
'''
ILD specific registration of file meta data

Created on Mar 21, 2013

@author: sposs
'''
__RCSID__ = "$Id: ILDRegisterOutputData.py 44185 2011-10-24 08:17:07Z sposs $"

from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.Resources.Catalog.FileCatalogClient             import FileCatalogClient

from DIRAC import S_OK, gLogger
import os

class ILDRegisterOutputData(ModuleBase):
  """ Register output data in the FC for the ILD productions 
  """
  def __init__(self):
    super(ILDRegisterOutputData, self).__init__()
    self.version = "ILDRegisterOutputData v1"
    self.log = gLogger.getSubLogger( "ILDRegisterOutputData" )
    self.commandTimeOut = 10 * 60
    self.enable = True
    self.prodOutputLFNs = []
    self.swpackages = []
    self.nbofevents = 0
    self.luminosity = 0
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

    self.nbofevents = self.NumberOfEvents #comes from ModuleBase
    
    if self.workflow_commons.has_key('Luminosity'):
      self.luminosity = self.workflow_commons['Luminosity']
    return S_OK('Parameters resolved')
  
  def execute(self):
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
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s"% "\n".join(self.prodOutputLFNs))

    #TODO: What meta data should be stored at file level?

    for files in self.prodOutputLFNs:
      meta = {}  

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
      
      if ['CrossSection'] in self.inputdataMeta:
        xsec = {'CrossSection':self.inputdataMeta['CrossSection']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, xsec)
          if not res['OK']:
            self.log.error('Could not register metadata CrossSection, with value %s for %s' % (self.inputdataMeta['CrossSection'],
                                                                                   files))
            return res
        meta.update(xsec)
        
      if ['CrossSectionError'] in self.inputdataMeta:
        xsec = {'CrossSectionError':self.inputdataMeta['CrossSectionError']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, xsec)
          if not res['OK']:
            self.log.error('Could not register metadata CrossSectionError, with value %s for %s' % (self.inputdataMeta['CrossSectionError'],
                                                                                  files))
            return res
        meta.update(xsec)
      if ['GenProcessID'] in self.inputdataMeta:
        fmeta = {'GenProcessID':self.inputdataMeta['GenProcessID']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata GenProcessID, with value %s for %s' % (self.inputdataMeta['GenProcessID'],
                                                                                  files))
            return res
        meta.update(fmeta)
      if ['GenProcessType'] in self.inputdataMeta:
        fmeta = {'GenProcessType':self.inputdataMeta['GenProcessType']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata GenProcessType, with value %s for %s' % (self.inputdataMeta['GenProcessType'],
                                                                                  files))
            return res
        meta.update(fmeta)
      if ['GenProcessType'] in self.inputdataMeta:
        fmeta = {'GenProcessType':self.inputdataMeta['GenProcessType']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata GenProcessType, with value %s for %s' % (self.inputdataMeta['GenProcessType'],
                                                                                  files))
            return res
        meta.update(fmeta)
      if ['GenProcessName'] in self.inputdataMeta:
        fmeta = {'GenProcessName':self.inputdataMeta['GenProcessName']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata GenProcessName, with value %s for %s' % (self.inputdataMeta['GenProcessName'],
                                                                                  files))
            return res
        meta.update(fmeta)
      if ['Luminosity'] in self.inputdataMeta:
        fmeta = {'Luminosity':self.inputdataMeta['Luminosity']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata Luminosity, with value %s for %s' % (self.inputdataMeta['Luminosity'],
                                                                                  files))
            return res
        meta.update(fmeta)
      if ['BeamParticle1'] in self.inputdataMeta:
        fmeta = {'BeamParticle1':self.inputdataMeta['BeamParticle1'],'BeamParticle2':self.inputdataMeta['BeamParticle2']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata BeamParticle' )
            return res
        meta.update(fmeta)
      if ['PolarizationB1'] in self.inputdataMeta:
        fmeta = {'PolarizationB1':self.inputdataMeta['PolarizationB1'],'PolarizationB2':self.inputdataMeta['PolarizationB2']}
        if self.enable:
          res = self.filecatalog.setMetadata(files, fmeta)
          if not res['OK']:
            self.log.error('Could not register metadata Polarization')
            return res
        meta.update(fmeta)
     
      
      self.log.info("Registered %s with tags %s"%(files, meta))
      
      ###Now, set the ancestors
      if self.InputData:
        inputdata = self.InputData
        if self.enable:
          res = self.filecatalog.addFileAncestors({files : {'Ancestors' : inputdata}})
          if not res['OK']:
            self.log.error('Registration of Ancestors for %s failed' % files)
            return res

    return S_OK('Output data metadata registered in catalog')
  
  