"""
RequestOperation to Tar and Upload a list of Files.

Download a list of files to local storage, then tars it and uploads it to a StorageElement

Environment Variable:

DIRAC_ARCHIVE_CACHE
  Folder where to store the downloaded files

"""

import os
import shutil
from pprint import pformat

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

__RCSID__ = '$Id$'


class ArchiveFiles(OperationHandlerBase):
  """ArchiveFiles operation handler."""

  def __init__(self, operation=None, csPath=None):
    """Initialize the ArchifeFiles handler.

    :param self: self reference
    :param Operation operation: Operation instance
    :param string csPath: CS path for this handler
    """
    OperationHandlerBase.__init__(self, operation, csPath)
    gMonitor.registerActivity('ArchiveFilesAtt', 'Request attempt',
                              'RequestExecutingAgent', 'Files/min', gMonitor.OP_SUM)
    gMonitor.registerActivity('ArchiveFilesOK', 'Requests successful',
                              'RequestExecutingAgent', 'Files/min', gMonitor.OP_SUM)
    gMonitor.registerActivity('ArchiveFilesFail', 'Requests failed',
                              'RequestExecutingAgent', 'Files/min', gMonitor.OP_SUM)
    self.workDirectory = os.environ.get('DIRAC_ARCHIVE_CACHE',
                                        os.environ.get('AGENT_WORKDIRECTORY', './ARCHIVE_TMP'))
    self.parameterDict = {}
    self.cacheFolder = None
    self.waitingFiles = []
    self.lfns = []

  def __call__(self):
    """Process the ArchiveFiles operation."""
    try:
      gMonitor.addMark('ArchiveFilesAtt', 1)
      self._run()
      gMonitor.addMark('ArchiveFilesOK', 1)
    except Exception as e:
      self.log.exception('Failed to execute ArchiveFiles', repr(e), lException=e)
      gMonitor.addMark('ArchiveFilesFail', 1)
      return S_ERROR(str(e))
    finally:
      self._cleanup()
    return S_OK()

  def _run(self):
    """Execute the download and tarring."""
    self.parameterDict = DEncode.decode(self.operation.Arguments)[0]  # tuple: dict, number of characters
    self.cacheFolder = os.path.join(self.workDirectory, self.request.RequestName)
    self._checkArchiveLFN()
    self.log.info('Parameters: %s' % pformat(self.parameterDict))
    self.log.info('Cache folder: %r' % self.cacheFolder)
    self.waitingFiles = self.getWaitingFilesList()
    self.lfns = [opFile.LFN for opFile in self.waitingFiles]
    self._checkReplicas()
    self._downloadFiles()
    self._tarFiles()
    self._uploadTarBall()
    self._markFilesDone()

  def _checkArchiveLFN(self):
    """Make sure the archive LFN does not exist yet."""
    archiveLFN = self.parameterDict['ArchiveLFN']
    exists = returnSingleResult(self.fc.isFile(archiveLFN))
    self.log.debug('Checking for Tarball existance %r' % exists)
    if exists['OK'] and exists['Value']:
      raise RuntimeError('Tarball %r already exists' % archiveLFN)

  def _checkReplicas(self):
    """Make sure the source files are at the sourceSE."""
    resReplica = self.fc.getReplicas(self.lfns)
    if not resReplica['OK']:
      self.log.error('Failed to get replica information:', resReplica['Message'])
      raise RuntimeError('Failed to get replica information')

    atSource = []
    notAt = []
    failed = []
    sourceSE = self.parameterDict.get('SourceSE', 'CERN-DST-EOS')
    for lfn, replInfo in resReplica['Value']['Successful'].iteritems():
      if sourceSE in replInfo:
        atSource.append(lfn)
      else:
        self.log.warn('LFN %r not found at source, only at: %s' % (lfn, ','.join(replInfo.keys())))
        notAt.append(lfn)

    for lfn, errorMessage in resReplica['Value']['Failed'].iteritems():
      self.log.warn('Failed to get replica info', '%s: %s' % (lfn, errorMessage))
      if 'No such file or directory' in errorMessage:
        continue
      failed.append(lfn)

    if failed:
      self.log.error('LFNs failed to get replica info:', '%r' % ' '.join(failed))
      raise RuntimeError('Failed to get some replica information')
    if notAt:
      self.log.error('LFNs not at sourceSE:', '%r' % ' '.join(notAt))
      raise RuntimeError('Some replicas are not at the source')

  def _downloadFiles(self):
    """Download the files."""
    self._checkFileSizes()
    self._checkFilePermissions()

    for index, opFile in enumerate(self.waitingFiles):
      lfn = opFile.LFN
      self.log.info('Processing file (%d/%d) %r' % (index, len(self.waitingFiles), lfn))
      sourceSE = self.parameterDict['SourceSE']

      attempts = 0
      destFolder = os.path.join(self.cacheFolder, os.path.dirname(lfn)[1:])
      self.log.debug('Local Cache Folder: %s' % destFolder)
      if not os.path.exists(destFolder):
        os.makedirs(destFolder)
      while True:
        attempts += 1
        download = returnSingleResult(self.dm.getFile(lfn, destinationDir=destFolder, sourceSE=sourceSE))
        if download['OK']:
          self.log.info('Downloaded file %r to %r' % (lfn, destFolder))
          break
        errorString = download['Message']
        self.log.error('Failed to download file:', errorString)
        opFile.Error = errorString
        opFile.Attempt += 1
        self.operation.Error = opFile.Error
        if 'No such file or directory' in opFile.Error:
          # The File does not exist, we just ignore this and continue, otherwise we never archive the other files
          opFile.Status = 'Done'
          download = S_OK()
          break
        if attempts > 10:
          self.log.error('Completely failed to download file:', errorString)
          raise RuntimeError('Completely failed to download file: %s' % errorString)

      if not download['OK']:
        raise RuntimeError('Failed to download file: %s' % attempts)

    return

  def _checkFileSizes(self):
    """Check the files for total file size and return error if too large."""
    return

  def _checkFilePermissions(self):
    """Check that the request owner has permission to read and remove the files."""
    permissions = self.fc.hasAccess(self.lfns, 'removeFile')
    if not permissions['OK']:
      raise RuntimeError('Could not resolve permissions')
    if permissions['Value']['Failed']:
      for lfn in permissions['Value']['Failed']:
        self.log.error('Cannot archive file:', lfn)
        for opFile in self.waitingFiles:
          if opFile.LFN == lfn:
            opFile.Status = 'Failed'
            opFile.Error = 'Permission denied'
            break
      raise RuntimeError('Do not have sufficient permissions')
    return

  def _tarFiles(self):
    """Tar the files."""
    tarFileName = os.path.splitext(os.path.basename(self.parameterDict['ArchiveLFN']))[0]
    baseDir = self.parameterDict['ArchiveLFN'].strip('/').split('/')[0]
    shutil.make_archive(tarFileName, format='tar', root_dir=self.cacheFolder, base_dir=baseDir,
                        dry_run=False, logger=self.log)

  def _uploadTarBall(self):
    """Upload the tarball to specified LFN."""
    lfn = self.parameterDict['ArchiveLFN']
    self.log.info('Uploading tarball to %r' % lfn)
    localFile = os.path.basename(lfn)
    tarballSE = self.parameterDict['TarballSE']
    upload = returnSingleResult(self.dm.putAndRegister(lfn, localFile, tarballSE))
    if not upload['OK']:
      raise RuntimeError('Failed to upload tarball: %s' % upload['Message'])
    self.log.debug('Uploading finished')

  def _markFilesDone(self):
    """Mark all the files as done."""
    self.log.info('Marking files as done')
    for opFile in self.waitingFiles:
      opFile.Status = 'Done'

  def _cleanup(self):
    """Remove the tarball and the downloaded files."""
    self.log.info('Cleaning files and tarball')
    try:
      os.remove(os.path.basename(self.parameterDict['ArchiveLFN']))
    except OSError as e:
      self.log.warn('Error when removing tarball: %s' % str(e))
    try:
      shutil.rmtree(self.cacheFolder, ignore_errors=True)
    except OSError as e:
      self.log.warn('Error when removing cacheFolder: %s' % str(e))

  def setOperation(self, operation):  # pylint: disable=useless-super-delegation
    """Set Operation and request setter.

    :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: operation instance
    :raises TypeError: if ``operation`` in not an instance
        of :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`
    """
    super(ArchiveFiles, self).setOperation(operation)
