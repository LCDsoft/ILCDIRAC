"""Test the Transformationinfo."""

from collections import OrderedDict
from contextlib import contextmanager

import pytest
from mock import MagicMock as Mock, patch

from DIRAC import S_OK, S_ERROR
import DIRAC
import DIRAC.TransformationSystem.Client.TransformationClient
import DIRAC.Resources.Catalog.FileCatalogClient
import DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient
from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import JobInfo
from ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo import TransformationInfo

from ILCDIRAC.Tests.Utilities.GeneralUtils import MatchStringWith

__RCSID__ = "$Id$"

# pylint: disable=W0212, redefined-outer-name


@pytest.fixture
def userProxyFixture(mocker):
  """Mock UserProxy."""
  @contextmanager
  def _mockedCM(*args, **kwargs):
    try:
      yield S_OK()
    finally:
      pass
  mocker.patch('ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo.UserProxy', new=_mockedCM)


@pytest.fixture
def failingUserProxyFixture(mocker):
  """Mock UserProxy."""
  @contextmanager
  def _mockedCM(*args, **kwargs):
    try:
      yield S_ERROR("Failed to set up proxy")
    finally:
      pass
  mocker.patch('ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo.UserProxy', new=_mockedCM)


@pytest.fixture
def tiFixture():
  """Fixture for TransformationInfo object."""
  tMock = Mock(name="transMock", spec=DIRAC.TransformationSystem.Client.TransformationClient.TransformationClient)
  tMock.setFileStatusForTransformation = Mock(name="setFileStat")
  fcMock = Mock(name="fcMock", spec=DIRAC.Resources.Catalog.FileCatalogClient.FileCatalogClient)
  jmMock = Mock(name="jobMonMock", spec=DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient.JobMonitoringClient)

  transInfoDict = dict(TransformationID=1234, TransformationName="TestProd12", Type="TestProd",
                       AuthorDN='/some/cert/owner', AuthorGroup='Test_Prod')

  tri = TransformationInfo(transformationID=1234, transInfoDict=transInfoDict,
                           enabled=False,
                           tClient=tMock, fcClient=fcMock, jobMon=jmMock)
  tri.log = Mock(name="LogMock")
  return tri


@pytest.fixture
def tdFixture():
  """Fixture for tasksDicts."""
  taskDicts = [dict(TaskID=123, LFN="lfn123", Status="Assigned", FileID=987001, ErrorCount=9),
               dict(TaskID=124, LFN="lfn124", Status="Processed", FileID=987002, ErrorCount=8),
               ]
  return taskDicts


def test_init(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo init..........................."""
  assert isinstance(tiFixture, TransformationInfo)
  assert not tiFixture.enabled


def test_checkTasksStatus(tiFixture, tdFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo checkTasksStatus..............."""
  # error getting files
  tiFixture.tClient.getTransformationFiles.return_value = S_ERROR("nope")
  with pytest.raises(RuntimeError) as re:
    tiFixture.checkTasksStatus()
  assert "Failed to get transformation tasks: nope" in str(re)

  # success getting files
  tiFixture.tClient.getTransformationFiles.return_value = S_OK(tdFixture)
  retDict = tiFixture.checkTasksStatus()
  assert len(retDict) == 2
  assert 123 in retDict
  assert 124 in retDict
  assert "FileID" in retDict[124]


def test_setJob_Status(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo setJob functions..............."""
  job = Mock(spec=JobInfo)
  job.jobID = 5678
  tiFixture.enabled = False
  tiFixture._TransformationInfo__setTaskStatus = Mock()
  tiFixture._TransformationInfo__updateJobStatus = Mock()

  # setJobDone
  tiFixture.setJobDone(job)
  tiFixture._TransformationInfo__setTaskStatus.assert_not_called()
  tiFixture._TransformationInfo__updateJobStatus.assert_not_called()

  tiFixture.enabled = True
  job.status = "Done"
  tiFixture.setJobDone(job)
  tiFixture._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Done")
  tiFixture._TransformationInfo__updateJobStatus.assert_not_called()

  tiFixture.enabled = True
  job.status = "Failed"
  tiFixture._TransformationInfo__setTaskStatus.reset_mock()
  tiFixture.setJobDone(job)
  tiFixture._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Done")
  tiFixture._TransformationInfo__updateJobStatus.assert_called_once_with(job.jobID, 'Done', "Job forced to Done")

  # setJobFailed
  tiFixture.enabled = False
  tiFixture._TransformationInfo__setTaskStatus.reset_mock()
  tiFixture._TransformationInfo__updateJobStatus.reset_mock()
  tiFixture.setJobFailed(job)
  tiFixture._TransformationInfo__setTaskStatus.assert_not_called()
  tiFixture._TransformationInfo__updateJobStatus.assert_not_called()

  tiFixture.enabled = True
  job.status = "Failed"
  tiFixture._TransformationInfo__setTaskStatus.reset_mock()
  tiFixture._TransformationInfo__updateJobStatus.reset_mock()
  tiFixture.setJobFailed(job)
  tiFixture._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Failed")
  tiFixture._TransformationInfo__updateJobStatus.assert_not_called()

  tiFixture.enabled = True
  job.status = "Done"
  tiFixture._TransformationInfo__setTaskStatus.reset_mock()
  tiFixture._TransformationInfo__updateJobStatus.reset_mock()
  tiFixture.setJobFailed(job)
  tiFixture._TransformationInfo__setTaskStatus.assert_called_once_with(job, "Failed")
  tiFixture._TransformationInfo__updateJobStatus.assert_called_once_with(job.jobID, "Failed", "Job forced to Failed")


def test_setInputStatusFuncs(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo setInput functions............."""
  tiFixture._TransformationInfo__setInputStatus = Mock()
  tiFixture.enabled = False
  job = Mock(spec=JobInfo)

  tiFixture._TransformationInfo__setInputStatus.reset_mock()
  tiFixture.setInputUnused(job)
  tiFixture._TransformationInfo__setInputStatus.assert_called_once_with(job, "Unused")

  tiFixture._TransformationInfo__setInputStatus.reset_mock()
  tiFixture.setInputProcessed(job)
  tiFixture._TransformationInfo__setInputStatus.assert_called_once_with(job, "Processed")

  tiFixture._TransformationInfo__setInputStatus.reset_mock()
  tiFixture.setInputDeleted(job)
  tiFixture._TransformationInfo__setInputStatus.assert_called_once_with(job, "Deleted")

  tiFixture._TransformationInfo__setInputStatus.reset_mock()
  tiFixture.setInputMaxReset(job)
  tiFixture._TransformationInfo__setInputStatus.assert_called_once_with(job, "MaxReset")


def test_setInputStatus(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo setInputStatus................."""
  job = Mock(spec=JobInfo)
  job.inputFile = "dummylfn"
  status = "Unused"

  tiFixture.enabled = False
  tiFixture._TransformationInfo__setInputStatus(job, status)

  tiFixture.enabled = True
  tiFixture.tClient.setFileStatusForTransformation.return_value = S_ERROR("Failed to set")
  with pytest.raises(RuntimeError) as re:
    tiFixture._TransformationInfo__setInputStatus(job, status)
  assert "Failed updating file status" in str(re)

  tiFixture.enabled = True
  tiFixture.tClient.setFileStatusForTransformation.return_value = S_OK("All Good")
  tiFixture._TransformationInfo__setInputStatus(job, status)


def test_setTaskStatus(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo setTaskStatus.................."""
  job = Mock(spec=JobInfo)
  job.taskID = 1234
  tiFixture.tClient.setTaskStatus = Mock(return_value=S_OK("Done"))
  tiFixture._TransformationInfo__setTaskStatus(job, "Processed")
  tiFixture.tClient.setTaskStatus.assert_called_once_with(tiFixture.transName, 1234, "Processed")

  tiFixture.tClient.setTaskStatus = Mock(return_value=S_ERROR("NotDone"))
  with pytest.raises(RuntimeError) as re:
    tiFixture._TransformationInfo__setTaskStatus(job, "Processed")
  tiFixture.tClient.setTaskStatus.assert_called_once_with(tiFixture.transName, 1234, "Processed")
  assert "Failed updating task status: NotDone" in str(re)


def test_updateJobStatus(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo updateJobStatus................"""
  dbMock = Mock()
  dbMock.setJobAttribute.return_value = S_OK()
  dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))

  logMock = Mock()
  logMock.addLoggingRecord.return_value = S_OK("added record")

  jobDBMock = Mock()
  jobDBMock.return_value = dbMock
  logDBMock = Mock()
  logDBMock.return_value = logMock
  with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
      tiFixture.enabled = False
      res = tiFixture._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
  assert res['OK']
  assert res['Value'] == "DisabledMode"

  with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
      tiFixture.enabled = True
      res = tiFixture._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
  assert res['OK']
  assert res['Value'] == "added record"

  dbMock.setJobAttribute.return_value = S_ERROR("Error setting job status")
  dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))
  jobDBMock.return_value = dbMock
  logDBMock.return_value = logMock
  with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
      tiFixture.enabled = True
      with pytest.raises(RuntimeError) as re:
        tiFixture._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
      assert "Failed to update job status" in str(re)

  dbMock.setJobAttribute.return_value = S_OK("SetTheStatus")
  dbMock.getJobAttributes.return_value = S_ERROR("Failed to get status")
  jobDBMock.return_value = dbMock
  logDBMock.return_value = logMock
  with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
      tiFixture.enabled = True
      with pytest.raises(RuntimeError) as re:
        tiFixture._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
      assert "Failed to get Minorstatus" in str(re)

  dbMock.setJobAttribute.return_value = S_OK("SetTheStatus")
  dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))
  logMock.addLoggingRecord.reset_mock()
  logMock.addLoggingRecord.return_value = S_ERROR("Did not add record")
  jobDBMock.return_value = dbMock
  logDBMock.return_value = logMock
  with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
      tiFixture.enabled = True
      res = tiFixture._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus=None)
      assert not res['OK']
      assert res['Message'] == "Did not add record"
      logMock.addLoggingRecord.assert_called_once_with(
          1234, status="Failed", minor="Statussed", source='DataRecoveryAgent')

  dbMock.setJobAttribute.return_value = S_OK("SetTheStatus")
  dbMock.getJobAttributes.return_value = S_OK(dict(MinorStatus="Statussed"))
  logMock.addLoggingRecord.reset_mock()
  logMock.addLoggingRecord.return_value = S_OK("added record")
  jobDBMock.return_value = dbMock
  logDBMock.return_value = logMock
  with patch("DIRAC.WorkloadManagementSystem.DB.JobDB.JobDB", new=jobDBMock):
    with patch("DIRAC.WorkloadManagementSystem.DB.JobLoggingDB.JobLoggingDB", new=logDBMock):
      tiFixture.enabled = True
      res = tiFixture._TransformationInfo__updateJobStatus(1234, "Failed", minorstatus="minorstatus")
  assert res['OK']
  assert res['Value'] == "added record"
  logMock.addLoggingRecord.assert_called_once_with(
      1234, status="Failed", minor="minorstatus", source='DataRecoveryAgent')


def test_findAllDescendants(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo findAllDescendents............."""
  tiFixture.fcClient.getFileDescendents = Mock(return_value=S_OK({"Successful": {"lfn1": ["lfnD1", "lfnD2"],
                                                                                 "lfnD1": ["lfnDD1", "lfnDD2"],
                                                                                 }}))
  descList = tiFixture._TransformationInfo__findAllDescendants(lfnList=[])
  assert descList == ["lfnDD1", "lfnDD2", "lfnD1", "lfnD2"]

  tiFixture.fcClient.getFileDescendents = Mock(return_value=S_ERROR("Cannot get descendants"))
  descList = tiFixture._TransformationInfo__findAllDescendants(lfnList=[])
  assert descList == []


def test_cleanOutputs_proxyFail(tiFixture, failingUserProxyFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo cleanOutputs..................."""
  descList = ["lfnDD1", "lfnDD2", "lfnD1", "lfnD2"]

  jobInfo = Mock(spec=JobInfo)
  jobInfo.outputFiles = ["lfn1", "lfn2"]
  jobInfo.outputFileStatus = ["Exists", "Missing"]

  tiFixture.enabled = True
  tiFixture._TransformationInfo__findAllDescendants = Mock(return_value=descList)
  with pytest.raises(RuntimeError) as re:
    tiFixture.cleanOutputs(jobInfo)
  assert "Failed to get a proxy" in str(re)


def test_cleanOutputs(tiFixture, userProxyFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo cleanOutputs..................."""
  descList = ["lfnDD1", "lfnDD2", "lfnD1", "lfnD2"]

  jobInfo = Mock(spec=JobInfo)
  jobInfo.outputFiles = ["lfn1", "lfn2"]
  jobInfo.outputFileStatus = ["Exists", "Missing"]

  tiFixture.enabled = False
  tiFixture._TransformationInfo__findAllDescendants = Mock(return_value=descList)

  tiFixture.cleanOutputs(jobInfo)
  tiFixture.log.notice.assert_any_call(MatchStringWith("Would have removed these files"))
  tiFixture.log.notice.assert_any_call(MatchStringWith("lfn1"))
  for _name, args, _kwargs in tiFixture.log.notice.mock_calls:
    assert "lfn2" not in str(args)

  remMock = Mock(name="remmock")
  remMock.removeFile.return_value = S_ERROR("arg")

  tiFixture.enabled = True
  tiFixture._TransformationInfo__findAllDescendants = Mock(return_value=descList)
  with patch("ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo.DataManager",
             return_value=remMock,
             autospec=True):
    with pytest.raises(RuntimeError) as re:
      tiFixture.cleanOutputs(jobInfo)
    assert "Failed to remove LFNs: arg" in str(re)

  remMock = Mock(name="remmock")
  remMock.removeFile.return_value = S_OK({"Successful": {"lfn1": "OK", "lfn2": "OK"},
                                          "Failed": {"lfnD2": "SomeReason",
                                                     "lfnD3": "SomeReason",
                                                     "lfnDD2": "SomeOtherReason"}})

  tiFixture.enabled = True
  with patch("ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo.DataManager",
             autospec=True,
             return_value=remMock):
    tiFixture.cleanOutputs(jobInfo)
    tiFixture.log.notice.assert_any_call(MatchStringWith("Successfully removed 2 files"))

  # nothing to remove
  jobInfo = Mock(spec=JobInfo)
  jobInfo.outputFiles = []
  tiFixture._TransformationInfo__findAllDescendants = Mock(return_value=descList)
  tiFixture.cleanOutputs(jobInfo)
  tiFixture._TransformationInfo__findAllDescendants.assert_not_called()

  # nothing to remove
  jobInfo = Mock(spec=JobInfo)
  jobInfo.outputFiles = ["lfn1", "lfn2"]
  jobInfo.outputFileStatus = ["Missing", "Missing"]
  tiFixture._TransformationInfo__findAllDescendants = Mock(return_value=[])
  tiFixture.cleanOutputs(jobInfo)
  tiFixture._TransformationInfo__findAllDescendants.assert_called_once_with(jobInfo.outputFiles)


def test_getJobs(tiFixture):
  """ILCDIRAC.ILCTransformationSystem.Utilities.TransformationInfo getJobs........................"""
  tiFixture.jobMon.getJobs = Mock()
  tiFixture.jobMon.getJobs.side_effect = (S_OK([123, 456, 789]), S_OK([1123, 1456, 1789]))

  # All OK, just Done
  jobs, ndone, nfailed = tiFixture.getJobs(statusList=["Done"])
  attrDict = dict(Status=["Done"], JobGroup="00001234")
  tiFixture.jobMon.getJobs.assert_called_once_with(attrDict)
  assert ndone == 3
  assert nfailed == 0
  assert isinstance(jobs, OrderedDict)

  # All OK, just Done
  tiFixture.jobMon.getJobs.reset_mock()
  tiFixture.jobMon.getJobs.side_effect = (S_OK([123, 456, 789]), S_OK([1123, 1456, 1789]))
  jobs, ndone, nfailed = tiFixture.getJobs(statusList=["Failed"])
  attrDict = dict(Status=["Failed"], JobGroup="00001234")
  tiFixture.jobMon.getJobs.assert_called_once_with(attrDict)
  assert ndone == 0
  assert nfailed == 3
  assert isinstance(jobs, OrderedDict)

  # All OK, None
  tiFixture.jobMon.getJobs.reset_mock()
  tiFixture.jobMon.getJobs.side_effect = (S_OK([123, 456, 789]), S_OK([1123, 56, 89]))
  jobs, ndone, nfailed = tiFixture.getJobs(statusList=None)
  attrDict = dict(Status=["Done"], JobGroup="00001234")
  attrDict2 = dict(Status=["Failed"], JobGroup="00001234")
  tiFixture.jobMon.getJobs.assert_any_call(attrDict)
  tiFixture.jobMon.getJobs.assert_any_call(attrDict2)
  assert ndone == 3
  assert nfailed == 3
  assert isinstance(jobs, OrderedDict)
  assert [56, 89, 123, 456, 789, 1123] == jobs.keys()

  # All ERROR
  tiFixture.jobMon.getJobs = Mock()
  tiFixture.jobMon.getJobs.side_effect = (S_ERROR("Not Done"), S_ERROR("Not Failed"))
  with pytest.raises(RuntimeError) as re:
    jobs, ndone, nfailed = tiFixture.getJobs(statusList=None)
  attrDict = dict(Status=["Done"], JobGroup="00001234")
  tiFixture.jobMon.getJobs.assert_called_once_with(attrDict)
  assert "Failed to get jobs" in str(re)
