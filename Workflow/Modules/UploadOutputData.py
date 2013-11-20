########################################################################
# $HeadURL$
########################################################################
""" 
Module to upload specified job output files according to the parameters
defined in the production workflow.

@author: S. Poss
@since: Sep 01, 2010
"""

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from ILCDIRAC.Core.Utilities.ResolveSE                     import getDestinationSEList
from ILCDIRAC.Core.Utilities.resolvePathsAndNames          import getProdFilename
from ILCDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase
from DIRAC.ConfigurationSystem.Client.Helpers.Operations     import Operations

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

import os, random, time

class UploadOutputData(ModuleBase):
  """ As name suggest: upload output data. For Production only: See L{UserJobFinalization} for User job upload.
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
    self.request = None
    self.PRODUCTION_ID = ""
    self.prodOutputLFNs = []
    self.experiment = "CLIC"

  #############################################################################
  def applicationSpecificInputs(self):
    """ By convention the module parameters are resolved here.
    """

    if self.step_commons.has_key('Enable'):
      self.enable = self.step_commons['Enable']
      if not type(self.enable) == type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False

    if self.step_commons.has_key('TestFailover'):
      self.enable = self.step_commons['TestFailover']
      if not type(self.failoverTest) == type(True):
        self.log.warn('Test failover flag set to non-boolean value %s, setting to False' % self.failoverTest)
        self.failoverTest = False

    if self.workflow_commons.has_key("PRODUCTION_ID"):
      self.PRODUCTION_ID = self.workflow_commons["PRODUCTION_ID"]

    if os.environ.has_key('JOBID'):
      self.log.verbose('Found WMS JobID = %s' % self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    ##This is the thing that is used to establish the list of outpufiles to treat:
    ## Make sure that all that is in the : "listoutput" and also in the ProductionData
    ## is treated properly. Needed as whatever is in listoutput does not contain any reference to the 
    ## prodID and task ID. Also if for some reason a step failed, then the corresponding data will not be there
    if self.workflow_commons.has_key('outputList'):
      self.outputList = self.workflow_commons['outputList']
      if self.workflow_commons.has_key('ProductionOutputData'):
        proddata = self.workflow_commons['ProductionOutputData'].split(";")
        self.log.verbose("prod data : %s" % proddata )
        olist = {}
        for obj in self.outputList:
          fname_in_outputlist = obj['outputFile'].lower()
          extension = ''
          if fname_in_outputlist.count("_sim") or fname_in_outputlist.count("_rec") or fname_in_outputlist.count("_dst"):
            extension = ".slcio"  
          elif fname_in_outputlist.count("_gen"):
            extension = ".stdhep"
          fname_in_outputlist = fname_in_outputlist.replace(extension,"")
          for prodfile in proddata:
            prodfile = os.path.basename(prodfile)
            extension = ''
            if prodfile.count("_sim") or prodfile.count("_rec") or prodfile.count("_dst"):
              extension = ".slcio"  
            elif prodfile.count("_gen"):
              extension = ".stdhep"
            prodfile = prodfile.replace(extension,"")   
            if olist.has_key(prodfile):
              ## This has already been treated, no need to come back to it.
              continue
            appdict = {}
            if (fname_in_outputlist.count("_gen")):# and prodfile.lower().count("_gen_")) :
              genf = obj['outputFile'].split("_gen")[0]
              genf += "_gen"
              if (prodfile.count(genf)):
                appdict.update(obj)
                appdict['outputFile'] = prodfile+extension
                olist[prodfile] = appdict
            if (fname_in_outputlist.count("_sim")):
              simf = obj['outputFile'].split("_sim")[0]
              simf += "_sim"
              if (prodfile.count(simf)):
                appdict.update(obj)
                appdict['outputFile'] = prodfile+extension
                olist[prodfile] = appdict
                self.log.verbose('olist %s'%olist)
            if (fname_in_outputlist.count("_rec")):
              recf = obj['outputFile'].split("_rec")[0]
              recf += "_rec"
              if (prodfile.count(recf)):
                appdict.update(obj)
                appdict['outputFile'] = prodfile+extension
                olist[prodfile] = appdict
                break
            if  (fname_in_outputlist.count("_dst") and prodfile.lower().count("_dst_")):
              dstf = obj['outputFile'].split("_dst")[0]
              dstf += "_dst"
              if (prodfile.count(dstf)):
                appdict.update(obj)
                appdict['outputFile'] = prodfile+extension
                olist[prodfile] = appdict
                break
        self.outputList = olist.values()
      else:
        olist = []
        for obj in self.outputList:
          appdict = obj
          appdict['outputFile'] = getProdFilename(obj['outputFile'],
                                                  int(self.workflow_commons["PRODUCTION_ID"]),
                                                  int(self.workflow_commons["JOB_ID"]))
          olist.append(appdict)
        self.outputList = olist
      self.log.verbose("OutputList : %s" % self.outputList)  

    if self.workflow_commons.has_key('outputMode'):
      self.outputMode = self.workflow_commons['outputMode']

    if self.workflow_commons.has_key('outputDataFileMask'):
      self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
      if not type(self.outputDataFileMask) == type([]):
        self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split(';')]

    #result = constructProductionLFNs(self.workflow_commons)
    #if not result['OK']:
    #  self.log.error('Could not create production LFNs',result['Message'])
    #  return result
    #self.prodOutputLFNs=result['Value']['ProductionOutputData']
    if self.workflow_commons.has_key('ProductionOutputData'):
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData'].split(";")
    else:
      self.prodOutputLFNs = []

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
      for n, v in metadata.items():
        self.log.info('%s = %s' % (n, v))

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
    failoverTransfer = FailoverTransfer(self.request)

    self.catalogs = self.ops.getValue('Production/%s/Catalogs' % self.experiment,
                                     ['FileCatalog', 'LcgFileCatalog'])



    #One by one upload the files with failover if necessary
    failover = {}
    if not self.failoverTest:
      for fileName, metadata in final.items():
        self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, 
                                                                                   ', '.join(metadata['resolvedSE'])))
        result = failoverTransfer.transferAndRegisterFile(fileName, 
                                                          metadata['localpath'], 
                                                          metadata['lfn'], 
                                                          metadata['resolvedSE'], 
                                                          fileMetaDict = metadata, 
                                                          fileCatalog = self.catalogs)
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' % (fileName, metadata))
          failover[fileName] = metadata
        else:
          lfn = metadata['lfn']
    else:
      failover = final

    self.failoverSEs = self.ops.getValue("Production/%s/FailOverSE" % self.experiment, self.failoverSEs)  

    cleanUp = False
    for fileName, metadata in failover.items():
      self.log.info('Setting default catalog for failover transfer to FileCatalog')
      failovers = self.failoverSEs
      targetSE = metadata['resolvedSE'][0]
      try:#remove duplicate site, otherwise it will do nasty things where processing the request
        failovers.remove(targetSE)
      except:
        pass
      random.shuffle(failovers)
      metadata['resolvedSE'] = failovers
      result = failoverTransfer.transferAndRegisterFileFailover(fileName, 
                                                                metadata['localpath'],
                                                                metadata['lfn'], 
                                                                targetSE, 
                                                                metadata['resolvedSE'],
                                                                fileMetaDict = metadata, 
                                                                fileCatalog = self.catalogs)
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' % (fileName, metadata))
        cleanUp = True
        break #no point continuing if one completely fails

    os.remove("DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK") #cleanup the mess

    self.request = failoverTransfer.request

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      lfns = []
      for fileName, metadata in final.items():
        lfns.append(metadata['lfn'])

      result = self.__cleanUp(lfns)
      self.workflow_commons['Request'] = self.request
      return S_ERROR('Failed to upload output data')

#    #Can now register the successfully uploaded files in the BK
#    if not performBKRegistration:
#      self.log.info('There are no files to perform the BK registration for, all could be saved to failover')
#    else:
#      rm = ReplicaManager()
#      result = rm.addCatalogFile(performBKRegistration,catalogs=['BookkeepingDB'])
#      self.log.verbose(result)
#      if not result['OK']:
#        self.log.error(result)
#        return S_ERROR('Could Not Perform BK Registration')
#      if result['Value']['Failed']:
#        for lfn,error in result['Value']['Failed'].items():
#          self.log.info('BK registration for %s failed with message: "%s" setting failover request' %(lfn,error))
#          result = self.request.addSubRequest({'Attributes':{'Operation':'registerFile','ExecutionOrder':0, 'Catalogue':'BookkeepingDB'}},'register')
#          if not result['OK']:
#            self.log.error('Could not set registerFile request:\n%s' %result)
#            return S_ERROR('Could Not Set BK Registration Request')
#          fileDict = {'LFN':lfn,'Status':'Waiting'}
#          index = result['Value']
#          self.request.setSubRequestFiles(index,'register',[fileDict])

    self.workflow_commons['Request'] = self.request
    return S_OK('Output data uploaded')

  #############################################################################
  def __cleanUp(self, lfnList):
    """ Clean up uploaded data for the LFNs in the list
    """
    # Clean up the current request
    for req_type in ['transfer', 'register']:
      for lfn in lfnList:
        result = self.request.getNumSubRequests(req_type)
        if result['OK']:
          nreq = result['Value']
          if nreq:
            # Go through subrequests in reverse order in order not to spoil the numbering
            ind_range = [0]
            if nreq > 1:
              ind_range = range(nreq-1, -1, -1)
            for i in ind_range:
              result = self.request.getSubRequestFiles(i, req_type)
              if result['OK']:
                fileList = result['Value']
                if fileList[0]['LFN'] == lfn:
                  result = self.request.removeSubRequest(i, req_type)

    # Set removal requests just in case
    for lfn in lfnList:
      result = self.request.addSubRequest({'Attributes': {'Operation' : 'removeFile', 'TargetSE' : '',
                                                          'ExecutionOrder' : 1}}, 'removal')
      index = result['Value']
      fileDict = {'LFN':lfn, 'PFN':'', 'Status':'Waiting'}
      self.request.setSubRequestFiles(index, 'removal', [fileDict])

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#