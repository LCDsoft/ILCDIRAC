"""
RequestOperation to Extract files from a previously Archived Tarball (see :mod:`ArchiveFiles`)

Download the tarball to local storage, extract files from local storage and put and register the files

Parameter:

* Tarball LFN
* StorageElement for put and register of the files

Execution Steps

* the tarball is downloaded
* the files contained in the tarball are added to request operation. Files are put and
  registered and marked as done, once they have been successfully uploaded.

  *  The tarball contains the full LFN of the files as they were in the filecatalog
  *  In case there are "Archive" Replicas we only have to registerReplica, otherwise we have to register the File

Environment Variable:

DIRAC_ARCHIVE_CACHE
  Folder where to extrac the files from the downloaded tarball

"""

import os
import shutil
import tarfile
from pprint import pformat

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = '$Id$'


class ExtractFiles(OperationHandlerBase):
  """ExtractFiles operation handler."""

  def __init__(self, operation=None, csPath=None):
    """Initialize the ExtractFiles handler.

    :param self: self reference
    :param Operation operation: Operation instance
    :param string csPath: CS path for this handler
    """
    OperationHandlerBase.__init__(self, operation, csPath)
    gMonitor.registerActivity('ExtractFilesAtt', 'Request attempt',
                              'RequestExecutingAgent', 'Files/min', gMonitor.OP_SUM)
    gMonitor.registerActivity('ExtractFilesOK', 'Requests successful',
                              'RequestExecutingAgent', 'Files/min', gMonitor.OP_SUM)
    gMonitor.registerActivity('ExtractFilesFail', 'Requests failed',
                              'RequestExecutingAgent', 'Files/min', gMonitor.OP_SUM)
    self.workDirectory = os.environ.get('DIRAC_ARCHIVE_CACHE',
                                        os.environ.get('AGENT_WORKDIRECTORY', './ARCHIVE_TMP'))
    self.parameterDict = {}
    self.targetSE = None
    self.cacheFolder = None
    self.lfnFolder = None
    self.waitingFiles = []
    self.lfns = []

  def __call__(self):
    """Process the ArchiveFiles operation."""
    try:
      gMonitor.addMark('ExtractFilesAtt', 1)
      self._run()
      gMonitor.addMark('ExtractFilesOK', 1)
    except Exception as e:
      self.log.exception('Failed to execute ExtractFiles', repr(e), lException=e)
      gMonitor.addMark('ExtractFilesFail', 1)
      return S_ERROR(str(e))
    finally:
      self._cleanup()
    return S_OK()

  def _run(self):
    """Execute the download, untar, and registration."""
    self.parameterDict = DEncode.decode(self.operation.Arguments)[0]  # tuple: dict, number of characters
    self.cacheFolder = os.path.join(self.workDirectory, self.request.RequestName)
    self.lfnFolder = os.path.join(self.cacheFolder, 'LFNs')
    self.targetSE = self.parameterDict['TargetSE']

    self.log.info('Parameters', pformat(self.parameterDict))
    self.log.info('Cache folder', '%r' % self.cacheFolder)

    self._downloadArchive()
    self._attachLFNs()

    self.waitingFiles = self.getWaitingFilesList()
    self.lfns = [opFile.LFN for opFile in self.waitingFiles]

    self._uploadAndRegisterLFNs()

  def _downloadArchive(self):
    """Download and extract archive."""
    tarballLFN = self.parameterDict['TarballLFN']
    tarballName = os.path.basename(tarballLFN)

    resGet = self.dm.getFile(self.parameterDict['TarballLFN'], destinationDir=self.cacheFolder)
    if not resGet['OK']:
      self.log.error('Failed to download tarball', resGet['Message'])
      raise RuntimeError('Failed to download tarball')

    with tarfile.open(os.path.join(self.cacheFolder, tarballName), mode='r:*') as tarArchive:
      tarArchive.extractAll(path=self.lfnFolder)

  def _attachLFNs(self):
    """List extracted LFNs and attach to operation."""
    files = set()
    for dirpath, _directories, filenames in os.walk(self.lfnFolder):
      for filename in filenames:
        files.add(os.path.join(dirpath, filename))
    self.log.info('Found files in tarball:', '%s' % len(files))

    lfns = set()
    for opFile in self.operation:
      lfns.add(opFile.LFN)
    missing = files - lfns

    for filename in missing:
      lfn = '/' + filename.replace(self.lfnFolder, 1)
      self.log.debug('Add lfn to operation', lfn)
      opFile = File()
      opFile.LFN = lfn
      self.operation.addFile(opFile)

  def _uploadAndRegisterLFNs(self):
    """Upload LFNs, register, and mark as done."""  
    for opFile in self.waitingFiles:
      resUp = self._uploadAndRegister(opFile.LFN)
      if resUp['OK']:
        self._markFileDone(opFile)

  def _cleanup(self):
    """Remove the tarball and the downloaded files."""
    self.log.info('Cleaning files and tarball')
    try:
      os.remove(os.path.basename(self.parameterDict['ArchiveLFN']))
    except OSError as e:
      self.log.warn('Error when removing tarball', repr(e))
    try:
      shutil.rmtree(self.cacheFolder, ignore_errors=True)
    except OSError as e:
      self.log.warn('Error when removing cacheFolder', repr(e))

  def _uploadAndRegister(self, lfn):
    """Upload and register the lfn."""
    lfnExists = False
    localFile = os.path.join(self.cacheFolder, lfn.lstrip('/'))

    # check if lfn is in FC or not
    exists = returnSingleResult(self.fc.isFile(lfn))
    if exists['OK'] and exists['Value']:
      lfnExists = True
      # check if file is already at target -> mark file done
      resReplica = returnSingleResult(self.fc.getReplicas(lfn))  # checkme
      if resReplica['OK'] and resReplica['Value']:
        self.log.verbose('LFN is already at TargetSE', lfn)
        # otherwise continue with upload and replica registration
        if self.targetSE in resReplica['Value']:
          return S_OK()

    #lfn is registered
    if lfnExists:
      self.log.verbose('LFN is registered -> putFile and register replica', lfn)
      theSE = StorageElement(self.targetSE)
      resPut = theSE.putFile({lfn: localFile})
      if not resPut['OK']:
        self.log.error('Failed to put file to storage', resPut['Message'])
        return resPut
      # register Replica in FC
      resReg = self.dm.registerReplica((lfn, 'dummy_pfn', self.targetSE))
      if not resReg['OK']:
        self.log.error('Failed to register lfn at SE:', '%s to %s: %s' % (lfn, self.targetSE, resReg['Message']))
        return resReg
      return S_OK()

    # lfn does not exist, simple DM.putAndRegister
    self.log.verbose('LFN is new -> putAndRegister', lfn)
    upload = returnSingleResult(self.dm.putAndRegister(lfn, localFile, self.targetSE))
    if not upload['OK']:
      self.log.error('Failed to putAndRegister LFN', '%s to %s: %s' % (lfn, self.targetSE, upload['Message']))
      return upload

    return S_OK()

  def _markFileDone(self, opFile):
    """Mark the LFN as done."""
    self.log.info('Marking file as done', opFile.LFN)
    opFile.Status = 'Done'
    return
