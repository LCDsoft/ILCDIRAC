########################################################################
# $HeadURL$
########################################################################
""" 
Create and send a combined request for any pending operations at
the end of a job.

@author: S. Poss
@author: S. Paterson

"""

__RCSID__ = "$Id$"

from ILCDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.TransformationSystem.Client.FileReport          import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport       import JobReport
from DIRAC                                                 import S_OK, S_ERROR, gLogger

import os

class FailoverRequest(ModuleBase):
  """ Handle the failover requests issued by previous steps. Used in production. 
  """
  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    super(FailoverRequest, self).__init__()
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "FailoverRequest" )
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
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      self.log.verbose('Found WMS JobID = %s' %self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable = False

    if self.step_commons.has_key('Enable'):
      self.enable = self.step_commons['Enable']
      if not type(self.enable) == type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' % self.enable)
        self.enable = False

    #Earlier modules will have populated the report objects
    if self.workflow_commons.has_key('JobReport'):
      self.jobReport = self.workflow_commons['JobReport']

    if self.workflow_commons.has_key('FileReport'):
      self.fileReport = self.workflow_commons['FileReport']

    if self.workflow_commons.has_key('PRODUCTION_ID'):
      self.productionID = self.workflow_commons['PRODUCTION_ID']

    if self.workflow_commons.has_key('JOB_ID'):
      self.prodJobID = self.workflow_commons['JOB_ID']

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

    if not self.enable:
      self.log.info('Module is disabled by control flag')
      return S_OK('Module is disabled by control flag')

    request = self._getRequestContainer()
    
    if not self.fileReport:
      self.fileReport =  FileReport('Transformation/TransformationManager')

    if self.InputData:
      inputFiles = self.fileReport.getFiles()
      for lfn in self.InputData:
        if not lfn in inputFiles:
          self.log.verbose('No status populated for input data %s, setting to "Unused"' % lfn)
          result = self.fileReport.setFileStatus(int(self.productionID), lfn, 'Unused')

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'], self.stepStatus['OK']))
      inputFiles = self.fileReport.getFiles()
      for lfn in inputFiles:
        if inputFiles[lfn] != 'ApplicationCrash' and self.jobType != "Split":
          self.log.info('Forcing status to "Unused" due to workflow failure for: %s' % (lfn))
          self.fileReport.setFileStatus(int(self.productionID), lfn, 'Unused')
    else:
      inputFiles = self.fileReport.getFiles()
      if inputFiles:
        self.log.info('Workflow status OK, setting input file status to Processed')                
      for lfn in inputFiles:
        self.log.info('Setting status to "Processed" for: %s' % (lfn))
        self.fileReport.setFileStatus(int(self.productionID), lfn, 'Processed')  

    result = self.fileReport.commit()
    if not result['OK']:
      self.log.error('Failed to report file status to ProductionDB, request will be generated', result['Message'])
      result = self.fileReport.generateForwardDISET()
      if not result['OK']:
        self.log.warn( "Could not generate Operation for file report with result:\n%s" % ( result['Value'] ) )
      else:
        if result['Value'] is None:
          self.log.info( "Files correctly reported to TransformationDB" )
        else:
          result = request.addOperation( result['Value'] )
    else:
      self.log.info('Status of files have been properly updated in the ProcessingDB')

    # Must ensure that the local job report instance is used to report the final status
    # in case of failure and a subsequent failover operation
    if self.workflowStatus['OK'] and self.stepStatus['OK']: 
      self.jobReport.setApplicationStatus('Job Finished Successfully')
      
    self.generateFailoverFile()


    return self.finalize()

  #############################################################################
  def finalize(self):
    """ Finalize and report correct status for the workflow based on the workflow
        or step status.
    """
    self.log.verbose('Workflow status = %s, step status = %s' % (self.workflowStatus['OK'], self.stepStatus['OK']))
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.warn('Workflow status is not ok, will not overwrite status')
      self.log.info('Workflow failed, end of FailoverRequest module execution.')
      return S_ERROR('Workflow failed, FailoverRequest module completed')

    self.log.info('Workflow successful, end of FailoverRequest module execution.')
    return S_OK('FailoverRequest module completed')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
