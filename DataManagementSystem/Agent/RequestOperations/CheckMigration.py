"""Ensure the files have been migrated to tape."""


from pprint import pformat

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = '$Id$'


class CheckMigration(OperationHandlerBase):
  """CheckMigration operation handler."""

  def __init__(self, operation=None, csPath=None):
    """Initialize the ArchifeFiles handler.

    :param self: self reference
    :param Operation operation: Operation instance
    :param string csPath: CS path for this handler
    """
    OperationHandlerBase.__init__(self, operation, csPath)
    self.waitingFiles = []

  def __call__(self):
    """Process the CheckMigration operation."""
    try:
      self._run()
    except Exception as e:
      self.log.exception('Failed to execute CheckMigration', repr(e), lException=e)
      return S_ERROR(str(e))
    return S_OK()

  def _run(self):
    """Check for migration bit, set file done when migrated."""
    self.waitingFiles = self.getWaitingFilesList()
    self.log.notice('Have %d waiting files' % len(self.waitingFiles))
    targetSESet = set(self.operation.targetSEList)
    self.log.notice('Target SEs: %s' % ','.join(targetSESet))
    migrated = True
    for opFile in self.waitingFiles:
      self.log.notice('Checking %r' % opFile.LFN)
      for targetSE in targetSESet:
        se = StorageElement(targetSE)
        metaData = returnSingleResult(se.getFileMetadata(opFile.LFN))
        self.log.notice('MetaData: %s' % pformat(metaData))
        if not metaData['OK']:
          self.log.error('Failed to get metadata:', metaData['Message'])
          migrated = False
          continue
        migrated = metaData['Value'].get('Migrated', 0) == 1 and migrated
      if migrated:
        self.log.notice('File %r has been migrated.' % opFile.LFN)
        opFile.Status = 'Done'

  def setOperation(self, operation):  # pylint: disable=useless-super-delegation
    """Set Operation and request setter.

    :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: operation instance
    :raises TypeError: if ``operation`` in not an instance
        of :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`
    """
    super(CheckMigration, self).setOperation(operation)
