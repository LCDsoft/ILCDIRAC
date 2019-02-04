"""
RequestOperation to Tar and Upload a list of Files

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

__RCSID__ = "$Id$"


class ArchiveFiles(OperationHandlerBase):
  """
  ArchiveFiles operation handler
  """

  def __init__(self, operation=None, csPath=None):
    """Constructor for ArchifeFiles.

    :param self: self reference
    :param Operation operation: Operation instance
    :param string csPath: CS path for this handler
    """
    OperationHandlerBase.__init__(self, operation, csPath)
    # gMonitor stuff
    gMonitor.registerActivity("ArchiveFilesAtt", "Download file attempts",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("ArchiveFilesOK", "Downloads successful",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("ArchiveFilesFail", "Downloads failed",
                              "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM)
    self.workDirectory = os.environ.get('DIRAC_ARCHIVE_CACHE', os.environ.get('AGENT_WORKDIRECTORY', './ARCHIVE_TMP'))
    self.parameterDict = {}
    self.cacheFolder = None
    self.waitingFiles= []

  def __call__(self):
    """ArchiveFiles operation processing."""

    try:
      self._run()
    except Exception as e:
      self.log.exception("Failed to execute ArchiveFiles", repr(e), lException=e)
      return S_ERROR(str(e))
    finally:
      self._cleanup()
    return S_OK()

  def _run(self):
    """Execute the download and tarring."""
    self.parameterDict = DEncode.decode(self.operation.Arguments)[0]  # tuple: dict, number of characters
    self.cacheFolder = os.path.join(self.workDirectory, self.request.RequestName)
    self.log.info("Parameters: %s" % pformat(self.parameterDict))
    self.waitingFiles = self.getWaitingFilesList()
    self._downloadFiles()
    self._tarFiles()
    self._uploadTarBall()
    self._markFilesDone()

  def _downloadFiles(self):
    """Download the files"""

    self._checkFileSizes()
    self._checkFilePermissions()
    
    for opFile in self.waitingFiles:
      lfn = opFile.LFN
      self.log.info("processing file %s" % lfn)
      gMonitor.addMark("ArchiveFilesAtt", 1)

      sourceSE = self.parameterDict['SourceSE']

      attempts = 0
      destFolder= os.path.join(self.cacheFolder, os.path.dirname(lfn)[1:])
      self.log.info("destFolder: %s" % destFolder)
      if not os.path.exists(destFolder):
        os.makedirs(destFolder)
      while True:
        attempts += 1
        download = returnSingleResult(self.dm.getFile(lfn, destinationDir=destFolder, sourceSE=sourceSE))
        if download["OK"]:
          self.log.info("Downloaded file: %s" % lfn)
          gMonitor.addMark("ArchiveFilesOK", 1)
          break
        errorString = download['Message']
        self.log.error("Failed to download file:", errorString)
        opFile.Error = errorString
        opFile.Attempt += 1
        self.operation.Error = opFile.Error
        if 'No such file or directory' in opFile.Error:
          opFile.Status = 'Failed'
          break
        if attempts > 10:
          self.log.error("Completely failed to download file:", errorString)
          raise RuntimeError("Completely failed to download file: %s" % errorString)

      if not download['OK']:
        raise RuntimeError('Failed to download file: %s' % attempts)

      gMonitor.addMark("ArchiveFilesOK", 1)
      self.log.info("Downloaded %s to %s" % (lfn, destFolder))

    return

  def _checkFileSizes(self):
    """Check the files for total file size and return error if too large."""
    return

  def _checkFilePermissions(self):
    """Check that the request owner has permission to read and remove the files."""
    return

  def _tarFiles(self):
    """Tar the files."""
    tarFileName = os.path.splitext(os.path.basename(self.parameterDict['ArchiveLFN']))[0]
    baseDir = self.parameterDict['ArchiveLFN'].strip('/').split('/')[0]
    shutil.make_archive(tarFileName, format='tar', root_dir=self.cacheFolder, base_dir=baseDir, dry_run=False, logger=self.log)

  def _uploadTarBall(self):
    """Upload the tarball to specified LFN."""
    lfn = self.parameterDict['ArchiveLFN']
    localFile = os.path.basename(lfn)
    tarballSE = self.parameterDict['TarballSE']
    upload = returnSingleResult(self.dm.putAndRegister(lfn, localFile, tarballSE))
    if not upload['OK']:
      raise RuntimeError("Failed to upload tarball: %s" % upload['Message'])

  def _markFilesDone(self):
    """Mark all the files as done."""
    for opFile in self.waitingFiles:
      opFile.Status = 'Done'

  def _cleanup(self):
    """Remove the tarball and the downloaded files."""
    os.remove(os.path.basename(self.parameterDict['ArchiveLFN']))
    shutil.rmtree(self.cacheFolder)

  def setOperation(self, operation):  # pylint: disable=useless-super-delegation
    """ operation and request setter

      :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: operation instance
      :raises TypeError: if ``operation`` in not an instance of :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`
    """
    super(ArchiveFiles, self).setOperation(operation)
