"""Tests for the ArchiveFiles Operation"""

import functools
import os

import pytest

from DIRAC import S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request

from ILCDIRAC.DataManagementSystem.Agent.RequestOperations import ArchiveFiles

MODULE = 'ILCDIRAC.DataManagementSystem.Agent.RequestOperations.ArchiveFiles'
FILE_NAME = 'fileName'
N_FILES = 10
DEST_DIR = '/Some/Local/Folder'


@pytest.fixture
def listOfLFNs():
  """Return a list of LFNs"""
  lfns = []
  for index, name in enumerate([FILE_NAME] * N_FILES):
    lfns.append('/vo/%s_%d' % (name, index))
  return lfns


@pytest.fixture
def myMocker(mocker):
  """Mock call to external libraries."""
  mocker.patch(MODULE + '.shutil.make_archive')
  mocker.patch(MODULE + '.shutil.rmtree')
  mocker.patch(MODULE + '.os.makedirs')
  mocker.patch(MODULE + '.os.remove')
  mocker.patch(MODULE + '.gMonitor')
  return None


@pytest.fixture
def multiRetValOK(listOfLFNs):
  """Return a return structure for multiple values"""
  retVal = {'OK': True, 'Value':
            {'Failed': {},
             'Successful': {},
             }}
  for lfn in listOfLFNs:
    retVal['Value']['Successful'][lfn] = True
  return retVal


def multiRetValFail(*args, **kwargs):
  """Return a return structure for multiple values"""
  retVal = {'OK': True, 'Value':
            {'Failed': {},
             'Successful': {},
             }}
  lfns = args[0]
  if isinstance(lfns, basestring):
    lfns = [lfns]
  for index, lfn in enumerate(lfns):
    print index, lfn
    if str(kwargs.get('Index', 5)) in lfn:
      retVal['Value']['Failed'][lfn] = kwargs.get('Error', 'Failed to do X')
    else:
      retVal['Value']['Successful'][lfn] = True
  return retVal


@pytest.fixture
def archiveRequestAndOp(listOfLFNs):
  """Return a tuple of the request and operation."""
  req = Request()
  req.RequestName = 'MyRequest'
  op = Operation()
  switches = {}
  archiveLFN = '/vo/tars/myTar.tar'
  op.Arguments = DEncode.encode({'SourceSE': switches.get('SourceSE', 'SOURCE-SE'),
                                 'TarballSE': switches.get('TarballSE', 'TARBALL-SE'),
                                 'ArchiveSE': switches.get('ArchiveSE', 'ARCHIVE-SE'),
                                 'FinalSE': switches.get('FinalSE', 'FINAL-SE'),
                                 'ArchiveLFN': archiveLFN})
  op.Type = 'ArchiveFiles'
  for index, lfn in enumerate(listOfLFNs):
    oFile = File()
    oFile.LFN = lfn
    oFile.Size = index
    oFile.Checksum = '01130a%0d' % index
    oFile.ChecksumType = 'adler32'
    op.addFile(oFile)

  req.addOperation(op)
  return req, op


@pytest.fixture
def archiveFiles(mocker, archiveRequestAndOp, multiRetValOK):
  """Return the ArchiveFiles operation instance."""
  mocker.patch.dict(os.environ, {'DIRAC_ARCHIVE_CACHE': DEST_DIR})
  af = ArchiveFiles.ArchiveFiles(archiveRequestAndOp[1])
  af.fc = mocker.MagicMock('FileCatalogMock')
  af.fc.hasAccess = mocker.MagicMock()
  af.fc.hasAccess.return_value = multiRetValOK
  af.dm = mocker.MagicMock('DataManagerMock')
  af.dm.getFile = mocker.MagicMock(return_value=multiRetValOK)
  af.dm.putAndRegister = mocker.MagicMock(return_value=multiRetValOK)
  return af


def test_constructor(archiveFiles, mocker):
  assert archiveFiles.cacheFolder is None
  assert archiveFiles.parameterDict == {}
  assert archiveFiles.lfns == []
  assert archiveFiles.waitingFiles == []
  assert archiveFiles.workDirectory == '/Some/Local/Folder'


def test_run_OK(archiveFiles, myMocker, listOfLFNs):
  archiveFiles._run()
  archiveFiles.dm.getFile.assert_called_with(listOfLFNs[9],
                                             destinationDir=os.path.join(DEST_DIR, 'MyRequest', 'vo'),
                                             sourceSE='SOURCE-SE')
  for opFile in archiveFiles.operation:
    assert opFile.Status == 'Done'


def test_run_Fail(archiveFiles, myMocker, listOfLFNs):
  archiveFiles.dm.getFile.side_effect = functools.partial(multiRetValFail, Index=5)
  with pytest.raises(RuntimeError, match='Completely failed to download file'):
    archiveFiles._run()
  archiveFiles.dm.getFile.assert_called_with(listOfLFNs[5],
                                             destinationDir=os.path.join(DEST_DIR, 'MyRequest', 'vo'),
                                             sourceSE='SOURCE-SE')
  for opFile in archiveFiles.operation:
    assert opFile.Status == 'Waiting'


def test_run_FailForEver(archiveFiles, myMocker, listOfLFNs):
  archiveFiles.dm.getFile.side_effect = functools.partial(multiRetValFail, Index=5, Error='No such file or directory')
  with pytest.raises(RuntimeError, match='Failed to download file: 1$'):
    archiveFiles._run()
  archiveFiles.dm.getFile.assert_called_with(listOfLFNs[5],
                                             destinationDir=os.path.join(DEST_DIR, 'MyRequest', 'vo'),
                                             sourceSE='SOURCE-SE')
  for index, opFile in enumerate(archiveFiles.operation):
    print opFile
    if index == 5:
      assert opFile.Status == 'Failed'
    else:
      assert opFile.Status == 'Waiting'


def test_checkFilePermissions(archiveFiles, myMocker):
  archiveFiles.waitingFiles = archiveFiles.getWaitingFilesList()
  archiveFiles.lfns = [opFile.LFN for opFile in archiveFiles.waitingFiles]
  assert len(archiveFiles.lfns) == N_FILES
  archiveFiles.fc.hasAccess.side_effect = functools.partial(multiRetValFail, Index=3, Error='Permission denied')
  with pytest.raises(RuntimeError, match='^Do not have sufficient permissions$'):
    archiveFiles._checkFilePermissions()
  for index, opFile in enumerate(archiveFiles.operation):
    if index == 3:
      assert opFile.Status == 'Failed'
    else:
      assert opFile.Status == 'Waiting'


def test_checkFilePermissions_breaks(archiveFiles, myMocker):
  archiveFiles.waitingFiles = archiveFiles.getWaitingFilesList()
  archiveFiles.lfns = [opFile.LFN for opFile in archiveFiles.waitingFiles]
  assert len(archiveFiles.lfns) == N_FILES
  archiveFiles.fc.hasAccess.return_value = S_ERROR('Break')
  with pytest.raises(RuntimeError, match='^Could not resolve permissions$'):
    archiveFiles._checkFilePermissions()
  for index, opFile in enumerate(archiveFiles.operation):
    assert opFile.Status == 'Waiting'


def test_uploadTarBall_breaks(archiveFiles, myMocker, listOfLFNs):
  archiveFiles.dm.putAndRegister.return_value = S_ERROR('Break')
  with pytest.raises(RuntimeError, match='^Failed to upload tarball: Break$'):
    archiveFiles._run()
  for index, opFile in enumerate(archiveFiles.operation):
    assert opFile.Status == 'Waiting'
  archiveFiles.dm.getFile.assert_called_with(listOfLFNs[9],
                                             destinationDir=os.path.join(DEST_DIR, 'MyRequest', 'vo'),
                                             sourceSE='SOURCE-SE')
  archiveFiles.dm.putAndRegister.assert_called_with('/vo/tars/myTar.tar',
                                                    'myTar.tar',
                                                    'TARBALL-SE')


def test_call(archiveFiles, myMocker, listOfLFNs):
  archiveFiles()
  for index, opFile in enumerate(archiveFiles.operation):
    assert opFile.Status == 'Done'
  archiveFiles.dm.getFile.assert_called_with(listOfLFNs[9],
                                             destinationDir=os.path.join(DEST_DIR, 'MyRequest', 'vo'),
                                             sourceSE='SOURCE-SE')
  archiveFiles.dm.putAndRegister.assert_called_with('/vo/tars/myTar.tar',
                                                    'myTar.tar',
                                                    'TARBALL-SE')


def test_call_withError(archiveFiles, myMocker, listOfLFNs):
  archiveFiles.dm.putAndRegister.return_value = S_ERROR('Break')
  archiveFiles()
  for index, opFile in enumerate(archiveFiles.operation):
    assert opFile.Status == 'Waiting'
  archiveFiles.dm.getFile.assert_called_with(listOfLFNs[9],
                                             destinationDir=os.path.join(DEST_DIR, 'MyRequest', 'vo'),
                                             sourceSE='SOURCE-SE')
  archiveFiles.dm.putAndRegister.assert_called_with('/vo/tars/myTar.tar',
                                                    'myTar.tar',
                                                    'TARBALL-SE')
