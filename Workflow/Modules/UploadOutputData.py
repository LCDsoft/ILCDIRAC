""" 
Module to upload specified job output files according to the parameters
defined in the production workflow.

:author: S. Poss
:since: Sep 01, 2010
"""

import os
import random
import time

from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations   import Operations
import DIRAC

from ILCDIRAC.Core.Utilities.ResolveSE                     import getDestinationSEList
from ILCDIRAC.Core.Utilities.resolvePathsAndNames          import getProdFilename
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

__RCSID__ = "$Id$"

class UploadOutputData(ModuleBase):
  """ As name suggest: upload output data. For Production only: See :mod:`~ILCDIRAC.Workflow.Modules.UserJobFinalization` for User job upload.
  """
  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    super(UploadOutputData, self).__init__()
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "UploadOutputData" )
    self.commandTimeOut = 10*60
    self.enable = True
    self.failoverTest = False #flag to put file to failover SE by default
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover', [])
    self.ops = Operations()

    #List all parameters here
    self.outputDataFileMask = ''
    self.outputMode = 'Any' #or 'Local' for reco case
    self.outputList = []
    self.productionID = 0
    self.prodOutputLFNs = []
    self.experiment = "CLIC"
    
  #############################################################################
  def applicationSpecificInputs(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.debug("Workflow commons: %s" % self.workflow_commons)

    self.enable = self.step_commons.get('Enable', self.enable)
    if not isinstance( self.enable, bool ):
      self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
      self.enable = False
    
    self.failoverTest = self.step_commons.get('TestFailover', self.failoverTest)
    if not isinstance( self.failoverTest, bool ):
      self.log.warn('Test failover flag set to non-boolean value %s, setting to False' % self.failoverTest)
      self.failoverTest = False

    self.productionID = self.workflow_commons.get("PRODUCTION_ID", self.productionID)

    self.jobID = os.environ.get('JOBID', self.jobID)
    if self.jobID:
      self.log.verbose('Found WMS JobID = %s' % self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    ##This is the thing that is used to establish the list of outpufiles to treat:
    ## Make sure that all that is in the : "outputList" and also in the ProductionData
    ## is treated properly. Needed as whatever is in outputList does not contain any reference to the 
    ## prodID and task ID. Also if for some reason a step failed, then the corresponding data will not be there
    self.outputList = self.workflow_commons.get('outputList', self.outputList)
    if self.outputList:
      if 'ProductionOutputData' in self.workflow_commons:
        productionData = self.workflow_commons['ProductionOutputData'].split(";")
        self.log.verbose("prod data : %s" % productionData )
        treatedOutputlist = {}
        for expectedOutputfile in self.outputList:
          self.log.debug("Treating file: %s" % expectedOutputfile['outputFile'])
          self.getTreatedOutputlistNew(productionData, treatedOutputlist, expectedOutputfile)
        self.outputList = treatedOutputlist.values()
      else:
        olist = []
        for expectedOutputfile in self.outputList:
          appdict = expectedOutputfile
          appdict['outputFile'] = getProdFilename(expectedOutputfile['outputFile'],
                                                  int(self.workflow_commons["PRODUCTION_ID"]),
                                                  int(self.workflow_commons["JOB_ID"]))
          olist.append(appdict)
        self.outputList = olist

      self.log.verbose("OutputList : %s" % self.outputList)

    self.outputMode = self.workflow_commons.get('outputMode', self.outputMode)

    self.outputDataFileMask = self.workflow_commons.get('outputDataFileMask', self.outputDataFileMask)
    if not isinstance( self.outputDataFileMask, list ):
      self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split(';')]

    #result = constructProductionLFNs(self.workflow_commons)
    #if not result['OK']:
    #  self.log.error('Could not create production LFNs',result['Message'])
    #  return result
    #self.prodOutputLFNs=result['Value']['ProductionOutputData']

    tempOutputLFNs = self.workflow_commons.get('ProductionOutputData', self.prodOutputLFNs)
    if isinstance( tempOutputLFNs, basestring ):
      self.prodOutputLFNs = tempOutputLFNs.split(";")
    else:
      self.prodOutputLFNs = tempOutputLFNs
    
    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    self.log.info('Initializing %s' % self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error("Failed to resolve input parameters:", result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      return S_OK('No output data upload attempted')

    ##determine the experiment
    example_file = self.prodOutputLFNs[0]
    if "/ilc/prod/clic" in example_file:
      self.experiment = "CLIC"
    elif "/ilc/prod/ilc/sid" in example_file:
      self.experiment = 'ILC_SID'
    elif "/ilc/prod/ilc/mc-dbd" in example_file:
      self.experiment = 'ILC_ILD' 
    else:
      self.log.warn("Failed to determine experiment, reverting to default")
      
    #Determine the final list of possible output files for the
    #workflow and all the parameters needed to upload them.
    result = self.getCandidateFiles(self.outputList, self.prodOutputLFNs, self.outputDataFileMask)
    if not result['OK']:
      self.log.error(result['Message'])
      self.setApplicationStatus(result['Message'])
      return result
    
    fileDict = result['Value']      
    result = self.getFileMetadata(fileDict)
    if not result['OK']:
      self.log.error(result['Message'])
      self.setApplicationStatus(result['Message'])
      return result

    if not result['Value']:
      self.log.info('No output data files were determined to be uploaded for this workflow')
      return S_OK()

    fileMetadata = result['Value']

    #Get final, resolved SE list for files
    final = {}
    for fileName, metadata in fileMetadata.items():
      result = getDestinationSEList(metadata['workflowSE'], DIRAC.siteName(), self.outputMode)
      if not result['OK']:
        self.log.error('Could not resolve output data SE', result['Message'])
        self.setApplicationStatus('Failed To Resolve OutputSE')
        return result
      
      resolvedSE = result['Value']
      final[fileName] = metadata
      final[fileName]['resolvedSE'] = resolvedSE

    self.log.info('The following files will be uploaded: %s' % (', '.join(final.keys() )))
    for fileName, metadata in final.items():
      self.log.info('--------%s--------' % fileName)
      for metaName, metaValue in metadata.items():
        self.log.info('%s = %s' % (metaName, metaValue))

    #At this point can exit and see exactly what the module would have uploaded
    if not self.enable:
      self.log.info('Module is disabled by control flag, would have attempted to upload the \
      following files %s' % ', '.join(final.keys()))
      return S_OK('Module is disabled by control flag')

    #Disable the watchdog check in case the file uploading takes a long time
    self.log.info('Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog prior to upload')
    fopen = open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK','w')
    fopen.write('%s' % time.asctime())
    fopen.close()
    
    #Instantiate the failover transfer client with the global request object
    failoverTransfer = FailoverTransfer(self._getRequestContainer())

    catalogs = self.ops.getValue('Production/%s/Catalogs' % self.experiment,
                                 ['FileCatalog', 'LcgFileCatalog'])



    #One by one upload the files with failover if necessary
    failover = {}
    if not self.failoverTest:
      for fileName, metadata in final.iteritems():
        self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, 
                                                                                   ', '.join(metadata['resolvedSE'])))
        result = failoverTransfer.transferAndRegisterFile(fileName, 
                                                          metadata['localpath'], 
                                                          metadata['lfn'], 
                                                          metadata['resolvedSE'], 
                                                          fileMetaDict = metadata['filedict'],
                                                          fileCatalog = catalogs)
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' % (fileName, metadata['filedict']))
          failover[fileName] = metadata
        else:
          #lfn = metadata['lfn']
          pass
    else:
      failover = final

    self.failoverSEs = self.ops.getValue("Production/%s/FailOverSE" % self.experiment, self.failoverSEs)  

    cleanUp = False
    for fileName, metadata in failover.iteritems():
      self.log.info('Setting default catalog for failover transfer to FileCatalog')
      failovers = self.failoverSEs
      targetSE = metadata['resolvedSE'][0]
      try:#remove duplicate site, otherwise it will do nasty things where processing the request
        failovers.remove(targetSE)
      except ValueError:
        pass
      random.shuffle(failovers)
      metadata['resolvedSE'] = failovers
      result = failoverTransfer.transferAndRegisterFileFailover(fileName, 
                                                                metadata['localpath'],
                                                                metadata['lfn'], 
                                                                targetSE, 
                                                                metadata['resolvedSE'],
                                                                fileMetaDict = metadata['filedict'],
                                                                fileCatalog = catalogs)
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' % (fileName, metadata['filedict']))
        cleanUp = True
        break #no point continuing if one completely fails

    os.remove("DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK") #cleanup the mess

    self.workflow_commons['Request'] = failoverTransfer.request

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      lfns = []
      for fileName, metadata in final.items():
        lfns.append(metadata['lfn'])

      result = self._cleanUp(lfns)
      return S_ERROR('Failed to upload output data')

    return S_OK('Output data uploaded')

  def _expectedExtension(self, filename):
    """return the expected extension based on the production type hinted in the filename"""
    extension = ''
    if any( ext in filename for ext in ('_sim', '_dst', '_rec') ):
      self.log.debug("expecting slcio file")
      extension = ".slcio"
    elif any( ext in filename for ext in ('_gen',) ):
      self.log.debug("expecting stdhep file")
      extension = ".stdhep"
    else:
      self.log.warn("Unknown production file type: %s" % filename)

    return extension

  def getTreatedOutputlistNew(self, producedData, treatedOutputlist, outputfileObject):
    """returns properly formated outputList"""
    expectedOutputFile, dummy_ext = self.getBasenameAndExtension(outputfileObject['outputFile'].lower())
    for productionFile in producedData:
      self.log.debug("Prodfile %s; outFile %s" %(productionFile, expectedOutputFile))
      productionFile, extension = self.getBasenameAndExtension(productionFile)
      self.log.debug("Removed extension: %s" % productionFile)
      if productionFile in treatedOutputlist:
        ## This has already been treated, no need to come back to it.
        continue
      appdict = {}
      for fType in ('_gen', '_sim', '_rec', '_dst'):
        ### No idea why the second thing is necessary, but it is there in the original function
        if fType in expectedOutputFile and ( fType != '_dst' or '_dst_' in productionFile.lower() ):
          filePrototype = outputfileObject['outputFile'].split(fType)[0]
          if filePrototype in productionFile and fType in productionFile:
            appdict.update(outputfileObject)
            appdict['outputFile'] = productionFile+extension
            treatedOutputlist[productionFile] = appdict
            if fType in ('_rec', '_dst'): #there will only be one _rec or _dst file...
              return


  def getBasenameAndExtension(self, filepath):
    """returns tuple of basename and extenion"""
    baseFileName = os.path.basename(filepath)
    extension = self._expectedExtension(baseFileName)
    baseFileNameWoExtension = baseFileName.replace(extension,"")
    return baseFileNameWoExtension, extension

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
