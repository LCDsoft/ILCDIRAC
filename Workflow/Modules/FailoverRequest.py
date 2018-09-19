""" 
Create and send a combined request for any pending operations at
the end of a job.

:author: S. Poss
:author: S. Paterson

"""

import os

from DIRAC.TransformationSystem.Client.FileReport          import FileReport
from DIRAC                                                 import S_OK, S_ERROR, gLogger

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

__RCSID__ = '$Id$'
LOG = gLogger.getSubLogger(__name__)

class FailoverRequest(ModuleBase):
  """ Handle the failover requests issued by previous steps. Used in production. 
  """
  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    super(FailoverRequest, self).__init__()
    self.version = __RCSID__
    #Internal parameters
    self.enable = True
    self.jobID = ''
    self.productionID = None
    self.prodJobID = None
    #Workflow parameters
    self.jobReport  = None
    self.fileReport = None
    self.jobType = ''

  #############################################################################
  def applicationSpecificInputs(self):
    """ By convention the module input parameters are resolved here.
    """
    if 'JOBID' in os.environ:
      self.jobID = os.environ['JOBID']
      LOG.verbose('Found WMS JobID = %s' % self.jobID)
    else:
      LOG.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    self.enable = self.step_commons.get('Enable', self.enable)
    if not isinstance( self.enable, bool ):
      LOG.error('Enable flag set to non-boolean value %s, setting to False' % self.enable)
      self.enable = False

    #Earlier modules will have populated the report objects
    self.jobReport = self.workflow_commons.get('JobReport', self.jobReport)

    self.fileReport = self.workflow_commons.get('FileReport', self.fileReport)

    self.productionID = self.workflow_commons.get('PRODUCTION_ID', self.productionID)

    self.prodJobID = self.workflow_commons.get('JOB_ID', self.prodJobID)

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    LOG.info('Initializing %s' % self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      LOG.error("Failed to resolve input parameters:", result['Message'])
      return result

    if not self.enable:
      LOG.info('Module is disabled by control flag')
      return S_OK('Module is disabled by control flag')

    self.fileReport = self.fileReport if self.fileReport else FileReport('Transformation/TransformationManager')

    if self.InputData:
      inputFiles = self.fileReport.getFiles()
      for lfn in self.InputData:
        if lfn not in inputFiles:
          LOG.verbose('No status populated for input data %s, setting to "Unused"' % lfn)
          self.fileReport.setFileStatus(int(self.productionID), lfn, 'Unused')

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.info('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
      inputFiles = self.fileReport.getFiles()
      for lfn in inputFiles:
        if inputFiles[lfn] != 'ApplicationCrash' and self.jobType != "Split":
          LOG.info('Forcing status to "Unused" due to workflow failure for: %s' % (lfn))
          self.fileReport.setFileStatus(int(self.productionID), lfn, 'Unused')
    else:
      inputFiles = self.fileReport.getFiles()
      if inputFiles:
        LOG.info('Workflow status OK, setting input file status to Processed')
      for lfn in inputFiles:
        LOG.info('Setting status to "Processed" for: %s' % (lfn))
        self.fileReport.setFileStatus(int(self.productionID), lfn, 'Processed')

    fileReportCommitResult = self.fileReport.commit()
    if fileReportCommitResult['OK']:
      LOG.info('Status of files have been properly updated in the ProcessingDB')
    else:
      LOG.error('Failed to report file status to ProductionDB:', fileReportCommitResult['Message'])
      LOG.error('Request will be generated.')
      disetResult = self.fileReport.generateForwardDISET()
      if not disetResult['OK']:
        LOG.warn("Could not generate Operation for file report with result:\n%s" % disetResult['Message'])
      else:
        if disetResult['Value'] is None:
          LOG.info("Files correctly reported to TransformationDB")
        else:
          request = self._getRequestContainer()
          request.addOperation( disetResult['Value'] )
          self.workflow_commons['Request'] = request

    # Must ensure that the local job report instance is used to report the final status
    # in case of failure and a subsequent failover operation
    if self.workflowStatus['OK'] and self.stepStatus['OK']:
      self.jobReport.setApplicationStatus('Job Finished Successfully')

    resFF = self.generateFailoverFile()
    if not resFF['OK']:
      LOG.error(resFF['Message'])

    return self.finalize()

  #############################################################################
  def finalize(self):
    """ Finalize and report correct status for the workflow based on the workflow
        or step status.
    """
    LOG.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      LOG.warn('Workflow status is not ok, will not overwrite status')
      LOG.info('Workflow failed, end of FailoverRequest module execution.')
      return S_ERROR('Workflow failed, FailoverRequest module completed')

    LOG.info('Workflow successful, end of FailoverRequest module execution.')
    return S_OK('FailoverRequest module completed')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
