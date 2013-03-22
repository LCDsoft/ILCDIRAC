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
import string

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

    if self.workflow_commons.has_key('NbOfEvents'):
      self.nbofevents = self.workflow_commons['NbOfEvents']
    if self.workflow_commons.has_key('NbOfEvts'):
      self.nbofevents = self.workflow_commons[ 'NbOfEvts']    
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
    
    self.log.verbose("Will try to set the metadata for the following files: \n %s"% string.join(self.prodOutputLFNs, 
                                                                                                "\n"))

    #TODO: What meta data should be stored at file level?

    for files in self.prodOutputLFNs:
#      elements = files.split("/")
#      metaprodid = {}
#      metaforfiles = {}
      meta = {}  

      if self.nbofevents:
        nbevts = {}
        nbevts['NumberOfEvents'] = self.nbofevents
        if self.enable:
          res = self.filecatalog.setMetadata(files, nbevts)
          if not res['OK']:
            self.log.error('Could not register metadata NumberOfEvents, with value %s for %s' % (self.nbofevents, 
                                                                                                 files))
            return res
        meta.update(nbevts)
      if self.luminosity:
        lumi = {}
        lumi['Luminosity'] = self.luminosity
        if self.enable:
          res = self.filecatalog.setMetadata(files, lumi)
          if not res['OK']:
            self.log.error('Could not register metadata Luminosity, with value %s for %s'%(self.luminosity, files))
            return res
        meta.update(lumi)
#      meta.update(metaprodid)
      
      
      
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
  
  