#####################################################
# $HeadURL: $
#####################################################
'''
Module that handles production data: not used

@since: May 31, 2010

@author: sposs
'''

__RCSID__ = "$Id: $"

import os,string, random
from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC.Resources.Catalog.FileCatalogClient            import FileCatalogClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer

from DIRAC                                                import S_OK, S_ERROR, gLogger

class HandleProdOutputData(ModuleBase):
  def __init__(self):
    super(HandleProdOutputData, self).__init__()
    self.result = S_ERROR()
    self.fc = FileCatalogClient()
    self.rm = ReplicaManager()
    self.destination = ''
    self.basepath = '/ilc/prod'
    self.dataattributes = ''
    self.attributesdict = {}
    self.generatorfile = ''
    self.mokkafile = ''
    self.marlinfiles = ''
    self.slicfile = ''
    self.lcsimfiles = ''
    self.request = None
    self.failoverTest=False
    self.log = gLogger.getSubLogger( "HandleOutputData" )

    
  def applicationSpecificInputs(self):
    """ Resolve all input variables for the module here.
    @return: S_OK()
    """
    if self.step_commons.has_key('DataAttributes'):
      self.dataattributes = self.step_commons['DataAttributes']
    else:
      return S_ERROR('No data attributes found, cannot proceed with registration in Catalog, ABORT!')
    
    for attribute in self.dataattributes.split(";"):
      if self.step_commons.has_key(attribute):
        self.attributesdict[attribute] = self.step_commons[attribute]
    if self.step_commons.has_key("destination"):
      self.destination = self.step_commons['destination']
    if self.step_commons.has_key('GENFile'):
      self.generatorfile = self.step_commons['GENFile']
    if self.step_commons.has_key('MokkaFile'):
      self.mokkafile = self.step_commons['MokkaFile']
    if self.step_commons.has_key('MarlinFiles'):
      self.marlinfiles = self.step_commons['MarlinFiles'].split(';')
    if self.step_commons.has_key('SLICFile'):
      self.slicfile = self.step_commons['SLICFile']
    if self.step_commons.has_key('LCSIMFiles'):
      self.lcsimfiles = self.step_commons['LCSIMFiles'].split(';')
      
    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)
    
      
    return S_OK('Parameters resolved')
  
  def execute(self):
    #Have to work out if the module is part of the last step i.e. 
    #user jobs can have any number of steps and we only want 
    #to run the finalization once.
    currentStep = int(self.step_commons['STEP_NUMBER'])
    totalSteps = int(self.workflow_commons['TotalSteps'])
    if currentStep==totalSteps:
      self.lastStep=True
    else:
      self.log.verbose('Current step = %s, total steps of workflow = %s, HandleProdOutputData will enable itself only at the last workflow step.' %(currentStep,totalSteps))            
        
    if not self.lastStep:
      return S_OK()
       
    self.result =self.resolveInputVariables()
    if not self.result['OK']:
      self.log.error(self.result['Message'])
      return self.result
    
    ###Instantiate object that will ensure that the files are registered properly
    failoverTransfer = FailoverTransfer(self.request)
    datatohandle = {}
    if self.generatorfile:
      if not os.path.exists(self.generatorfile):
        return S_ERROR("File %s does not exist, something went wrong before !"%(self.generatorfile))
      self.attributesdict['DataType'] = 'gen'
      lfnpath = string.join([self.basepath,self.attributesdict['Machine'],self.attributesdict['Energy'],
                                  self.attributesdict['DataType'],self.attributesdict['EvtType'],self.attributesdict['ProdID'],
                                  self.generatorfile],"/")
      datatohandle[self.generatorfile]={'lfn':lfnpath,'type':'gen','workflowSE':self.destination}
    if self.mokkafile or self.slicfile:
      recofile = ''
      if self.mokkafile and not os.path.exists(self.mokkafile):
        return S_ERROR("File %s does not exist, something went wrong before !"%(self.mokkafile))
      else:
        recofile = self.mokkafile
      if self.slicfile and not os.path.exists(self.slicfile):
        return S_ERROR("File %s does not exist, something went wrong before !"%(self.slicfile))
      else:
        recofile = self.slicfile
      self.attributesdict['DataType'] = 'SIM'
      lfnpath = string.join([self.basepath,self.attributesdict['Machine'],self.attributesdict['Energy'],
                                  self.attributesdict['DetectorModel'],self.attributesdict['DataType'],self.attributesdict['EvtType'],
                                  self.attributesdict['ProdID'],recofile],"/")
      datatohandle[recofile]={'lfn':lfnpath,'type':'gen','workflowSE':self.destination}


    ##Below, look in file name if it contain REC or DST, to determine the data type.
    if self.marlinfiles:
      for file in self.marlinfiles:
        if file.find("REC")>-1:
          self.attributesdict['DataType'] = 'REC'
        if file.find("DST")>-1:
          self.attributesdict['DataType'] = 'DST'
        lfnpath = string.join([self.basepath,self.attributesdict['Machine'],self.attributesdict['Energy'],
                                    self.attributesdict['DetectorModel'],self.attributesdict['DataType'],self.attributesdict['EvtType'],
                                    self.attributesdict['ProdID'],file],"/")
        datatohandle[file]={'lfn':lfnpath,'type':'gen','workflowSE':self.destination}

        
    if self.lcsimfiles:
      for file in self.lcsimfiles:
        if file.find("DST")>-1:
          self.attributesdict['DataType'] = 'DST'
        lfnpath = string.join([self.basepath,self.attributesdict['Machine'],self.attributesdict['Energy'],
                                    self.attributesdict['DetectorModel'],self.attributesdict['DataType'],self.attributesdict['EvtType'],
                                    self.attributesdict['ProdID'],file],"/")
        datatohandle[file]={'lfn':lfnpath,'type':'gen','workflowSE':self.destination}
        
    result = self.getFileMetadata(datatohandle)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return S_OK()
    fileMetadata = result['Value']

    final = {}
    for fileName,metadata in fileMetadata.items():
      final[fileName]=metadata
      final[fileName]['resolvedSE']=self.destination
    #One by one upload the files with failover if necessary
    replication = {}
    failover = {}
    uploaded = []
    if not self.failoverTest:
      for fileName,metadata in final.items():
        self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, string.join(metadata['resolvedSE'],', ')))
        result = failoverTransfer.transferAndRegisterFile(fileName,metadata['localpath'],metadata['lfn'],metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog=self.userFileCatalog)
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
          failover[fileName]=metadata
        else:
          #Only attempt replication after successful upload
          lfn = metadata['lfn']
          uploaded.append(lfn)          
          seList = metadata['resolvedSE']
          replicateSE = ''
          if result['Value'].has_key('uploadedSE'):
            uploadedSE = result['Value']['uploadedSE']            
            for se in seList:
              if not se == uploadedSE:
                replicateSE = se
                break
          
          if replicateSE and lfn:
            self.log.info('Will attempt to replicate %s to %s' %(lfn,replicateSE))    
            replication[lfn]=replicateSE            
    else:
      failover = final

    cleanUp = False
    for fileName,metadata in failover.items():
      random.shuffle(self.failoverSEs)
      targetSE = metadata['resolvedSE'][0]
      metadata['resolvedSE']=self.failoverSEs
      result = failoverTransfer.transferAndRegisterFileFailover(fileName,metadata['localpath'],metadata['lfn'],targetSE,metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog=self.userFileCatalog)
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
        cleanUp = True
        continue #for users can continue even if one completely fails
      else:
        lfn = metadata['lfn']
        uploaded.append(lfn)

    #For files correctly uploaded must report LFNs to job parameters
    if uploaded:
      report = string.join( uploaded, ', ' )
      self.jobReport.setJobParameter( 'UploadedOutputData', report )

    #Now after all operations, retrieve potentially modified request object
    result = failoverTransfer.getRequestObject()
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could Not Retrieve Modified Request')

    self.request = result['Value']

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      self.workflow_commons['Request']=self.request
      #Leave any uploaded files just in case it is useful for the user
      #do not try to replicate any files.
      return S_ERROR('Failed To Upload Output Data')

    
    return S_OK()
  