"""
RequestOperation to Tar and Upload a list of Files

Download a list of files to local storage, then tars it and uploads it to a StorageElement

Environment Variable:

DIRAC_ARCHIVE_CACHE
  Folder where to store the downloaded files

"""

import os
import tarfile
from pprint import pformat

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
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
    self.targetSE = ''
    self.cacheFolder = None

  def __call__(self):
    """ArchiveFiles operation processing."""

    try:
      self._run()
    except Exception as e:
      self.log.error("Failed to execute ArchiveFiles", repr(e))
      raise
      return S_ERROR(str(e))
    return S_OK()

  def _run(self):
    """Execute the download and tarring."""
    self.parameterDict = DEncode.decode(self.operation.Arguments)[0]  # tuple: dict, number of characters
    self.cacheFolder = os.path.join(self.workDirectory, self.request.RequestName)
    self.log.info("Parameters: %s" % pformat(self.parameterDict))
    self._getTargetSE()
    self._downloadFiles()
    self._tarFiles()
    self._uploadTarBall()


  def _getTargetSE(self):
    """Get the targetSE."""
    targetSEs = self.operation.targetSEList
    if len(targetSEs) != 1:
      self.log.error("wrong value for TargetSE list = %s, should contain only one target!" % targetSEs)
      self.operation.Error = "Wrong parameters: TargetSE should contain only one targetSE"
      for opFile in self.operation:

        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: TargetSE should contain only one targetSE"

        gMonitor.addMark("ArchiveFilesAtt", 1)
        gMonitor.addMark("ArchiveFilesFail", 1)

      raise ValueError("TargetSE should contain only one target, got %s" % targetSEs)

    self.targetSE = targetSEs[0]
    targetWrite = self.rssSEStatus(self.targetSE, "WriteAccess")
    if not targetWrite["OK"]:
      self.log.error(targetWrite["Message"])
      for opFile in self.operation:
        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: %s" % targetWrite["Message"]
        gMonitor.addMark("ArchiveFilesAtt", 1)
        gMonitor.addMark("ArchiveFilesFail", 1)
      self.operation.Error = targetWrite["Message"]
      raise ValueError("Wrong parameter value")

    if not targetWrite["Value"]:
      self.operation.Error = "TargetSE %s is banned for writing"
      raise RuntimeError(self.operation.Error)

    return

  def _downloadFiles(self):
    """Download the files"""
    waitingFiles = self.getWaitingFilesList()

    self._checkFileSizes(waitingFiles)
    self._checkFilePermissions(waitingFiles)
    
    for opFile in waitingFiles:
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
        download = self.dm.getFile(lfn, destinationDir=destFolder, sourceSE=sourceSE)
        if download["OK"] and lfn in download['Value']['Successful']:
          self.log.info("Downloaded file: %s" % lfn)
          gMonitor.addMark("ArchiveFilesOK", 1)
          break
        errorString = ''
        if not download['OK']:
          errorString = download['Message']
          self.log.error("Failed to download file:", errorString)
        elif lfn in download['Value']['Failed']:
          errorString = download['Value']['Failed'][lfn]
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

      if not download['OK'] or lfn in download['Value']['Failed']:
        raise RuntimeError('Failed to download file: %s' % attempts)

      if lfn in download['Value']:
        gMonitor.addMark("ArchiveFilesOK", 1)
        self.log.info("Downloaded %s to %s" % lfn)

    return

  def _checkFileSizes(self, waitingFiles):
    """Check the files for total file size and return error if too large."""
    return

  def _checkFilePermissions(self, waitingFiles):
    """Check that the request owner has permission to read and remove the files."""
    return

  def _tarFiles(self):
    """Tar the files."""
    tarFileName = self.parameterDict['ArchiveLFN']
    with tarfile.TarFile(os.path.basename(tarFileName), mode='w') as archive:
      archive.add(self.cacheFolder+'/ilc', arcname='ilc/', recursive=True)
      archive.list(verbose=True)

  def setOperation(self, operation):  # pylint: disable=useless-super-delegation
    """ operation and request setter

      :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: operation instance
      :raises TypeError: if ``operation`` in not an instance of :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`
    """
    super(ArchiveFiles, self).setOperation(operation)
