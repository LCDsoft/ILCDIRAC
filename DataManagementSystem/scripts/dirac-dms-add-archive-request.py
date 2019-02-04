"""Create and put Requests to archive files.

List of operations:

#. ArchiveFiles
#. ReplicateAndRegister Tarball
#. Add ArchiveSE replica for all files
#. Check for Tarball Migration
#. Remove all other replicas for these files
#. Remove original replica of Tarball

Will copy all the respective files and place them in to tarballs. Then the tarballs are migrated to
another storage element. Once the file is migrated to tape the original files will be
removed. Optionally the original files can be registered in a special archive SE, so that their
metadata is preserved.

"""
import os

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Base import Script

from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File


LOG = gLogger.getSubLogger('AddArchive')
__RCSID__ = '$Id$'
MAX_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB
MAX_FILES = 2000

class CreateArchiveRequest(object):
  """Create the request to archive files."""

  def __init__(self):
    """Constructor."""
    self._fcClient = None
    self._reqClient = None

    self.switches = {}
    self.lfnList = []
    self.registerSwitches()
    self.getLFNList()
    self.lfnChunks = []
    self.metaData = None
    self.replicaSEs = []
    self.requests = []

  @property
  def fcClient(self):
    """Return FileCatalogClient."""
    if not self._fcClient:
      from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
      self._fcClient = FileCatalog()
    return self._fcClient

  @property
  def reqClient(self):
    """Return RequestClient."""
    if not self._reqClient:
      from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
      self._reqClient = ReqClient()
    return self._reqClient

  def registerSwitches(self):
    """Set flags and options."""
    options = [('S', 'SourceSE', 'Source SE to use'),
               ('F', 'FinalSE', 'Final SE for tarball'),
               ('A', 'ArchiveSE', 'SE for registering archive files at'),
               ('T', 'TarballSE', 'SE to initially upload tarball'),
               ('P', 'Path', 'LFN path to folder, all files in the folder will be archived'),
               ('N', 'Name', 'Name of the Tarball, if not given Path_Tars/Path_N.tar will be used to store tarballs'),
               ('L', 'List', 'File containing list of LFNs to archive, requires Name to be given'),
               ]
    flags = [('R', 'RegisterArchiveReplica', 'Register archived files in ArchiveSE'),
             ('C', 'ReplicateTarball', 'Replicate the tarball'),
             ('D', 'RemoveReplicas', 'Remove Replicas from non-ArchiveSE'),
             ('U', 'RemoveFiles', 'Remove Archived files completely'),
             ('X', 'Execute', 'Put Requests, else dryrun'),
             ]
    for short, longOption, doc in options:
      Script.registerSwitch(short + ':', longOption + '=', doc)
    for short, longOption, doc in flags:
      Script.registerSwitch(short, longOption, doc)
    Script.setUsageMessage('\n'.join([__doc__,
                                      'Usage:',
                                      ' %s [option|cfgfile] LFNs tarBallName' % Script.scriptName,
                                      ]))

    Script.parseCommandLine()
    if Script.getPositionalArgs():
      Script.showHelp()
      DIRAC.exit(1)

    for switch in Script.getUnprocessedSwitches():
      for short, longOption, doc in options:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          self.switches[longOption] = switch[1]
          break
      for short, longOption, doc in flags:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          self.switches[longOption] = True
          break
    self.switches['DryRun'] = not self.switches.get('Execute', False)

    if self.switches.get('List') and not self.switches.get('Name'):
      raise RuntimeError('Have to set "Name" with "List"')
    if not self.switches.get('List') and not self.switches.get('Path'):
      raise RuntimeError('Have to set "List" or "Path"')

  def getLFNList(self):
    """Get list of LFNs.

    Either read the provided file, or get the files found beneath the proviede folder.
    Also Set TarBall name if given folder and not given it already.

    :param dict switches: options from command line
    :returns: list of lfns
    :raises: RuntimeError, ValueError
    """
    if self.switches.get('List'):
      if os.path.exists(self.switches.get('List')):
        self.lfnList = list(set([line.split()[0]
                                 for line in open(self.switches.get('List')).read().splitlines()]))
      else:
        raise ValueError('%s not a file' % self.switches.get('List'))
    if self.switches.get('Path'):
      path = self.switches.get('Path')
      LOG.debug('Check if %r is a directory' % path)
      isDir = returnSingleResult(self.fcClient.isDirectory(path))
      LOG.debug('Result: %r' % isDir)
      if not isDir['OK'] or not isDir['Value']:
        LOG.error('Path is not a directory', isDir.get('Message', ''))
        raise RuntimeError('Path %r is not a directory' % path)
      LOG.notice('Looking for files in %r' % path)
      lfns = self.fcClient.findFilesByMetadata(metaDict={}, path=self.switches.get('Path'))
      if not lfns['OK']:
        LOG.error('Could not find files')
        raise RuntimeError(lfns['Message'])
      if not self.switches.get('Name'):
        self.switches['AutoName'] = os.path.join(os.path.dirname(path), os.path.basename(path) + '.tar')
        LOG.notice('Using %r for tarball' % self.switches.get('AutoName'))
      self.lfnList = lfns['Value']

    if self.lfnList:
      LOG.notice('Will create request(s) with %d lfns' % len(self.lfnList))
      return

    raise ValueError('"Path" or "List" need to be provided!')

  def splitLFNsBySize(self):
    """Split LFNs into MAX_SIZE chunks of at most MAX_FILES length.

    :return: list of list of lfns
    """
    LOG.notice('Splitting files by Size')
    metaData = self.fcClient.getFileMetadata(self.lfnList)
    error = False
    if not metaData['OK']:
      LOG.error('Unable to read metadata for lfns: %s' % metaData['Message'])
      raise RuntimeError('Could not read metadata: %s' % metaData['Message'])

    self.metaData = metaData['Value']
    for failedLFN, reason in self.metaData['Failed'].items():
      LOG.error('skipping %s: %s' % (failedLFN, reason))
      error = True
    if error:
      raise RuntimeError('Could not read all metadata')

    lfnChunk = []
    totalSize = 0
    for lfn, info in self.metaData['Successful'].iteritems():
      if (totalSize > MAX_SIZE or len(lfnChunk) >= MAX_FILES) and not self.switches.get('List'):
        self.lfnChunks.append(lfnChunk)
        LOG.notice('Created Chunk of %s lfns with %s bytes' % (len(lfnChunk), totalSize))
        lfnChunk = []
        totalSize = 0
      lfnChunk.append(lfn)
      totalSize += info['Size']

    self.lfnChunks.append(lfnChunk)
    LOG.notice('Created Chunk of %s lfns with %s bytes' % (len(lfnChunk), totalSize))

    self.replicaSEs = set([seItem for se in self.fcClient.getReplicas(self.lfnList)['Value']['Successful'].values()
                           for seItem in se.keys()])

  def run(self):
    """Perform checks and create the request."""
    if self.switches.get('AutoName'):
      baseArchiveLFN = archiveLFN = self.switches['AutoName']
      tarballName = os.path.basename(archiveLFN)
    else:
      archiveLFN = self.switches['Name']
      tarballName = os.path.basename(archiveLFN)
    baseRequestName = requestName = 'Archive_%s' % tarballName.rsplit('.', 1)[0]

    from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator

    self.splitLFNsBySize()

    for count, lfnChunk in enumerate(self.lfnChunks):
      if not lfnChunk:
        LOG.error('LFN list is empty!!!')
        return 1

      if len(self.lfnChunks) > 1:
        requestName = '%s_%d' % (baseRequestName, count)
        baseName = os.path.split(baseArchiveLFN.rsplit('.', 1)[0])
        archiveLFN = '%s/%s_Tars/%s_%d.tar' % (baseName[0], baseName[1], baseName[1], count)

      self.checkArchive(archiveLFN)

      request = self.createRequest(requestName, archiveLFN, lfnChunk)

      valid = RequestValidator().validate(request)
      if not valid['OK']:
        LOG.error('putRequest: request not valid', '%s' % valid['Message'])
        return 1
      else:
        self.requests.append(request)

    self.putOrRunRequests()
    return 0

  def createRequest(self, requestName, archiveLFN, lfnChunk):
    """Create the Request."""
    request = Request()
    request.RequestName = requestName

    self._checkReplicaSites(request, lfnChunk)

    archiveFiles = Operation()
    archiveFiles.Type = 'ArchiveFiles'
    archiveFiles.Arguments = DEncode.encode({'SourceSE': self.switches.get('SourceSE', 'CERN-DST-EOS'),
                                             'TarballSE': self.switches.get('TarballSE', 'CERN-DST-EOS'),
                                             'ArchiveSE': self.switches.get('ArchiveSE', 'CERN-ARCHIVE'),
                                             'FinalSE': self.switches.get('FinalSE', 'CERN-SRM'),
                                             'ArchiveLFN': archiveLFN})
    self.addLFNs(archiveFiles, lfnChunk, self.metaData)
    request.addOperation(archiveFiles)

    # Replicate the Tarball, ArchiveFiles will upload it
    if self.switches.get('ReplicateTarball'):
      replicateAndRegisterTarBall = Operation()
      replicateAndRegisterTarBall.Type = 'ReplicateAndRegister'
      replicateAndRegisterTarBall.TargetSE = self.switches.get('FinalSE', 'CERN-SRM')
      opFile = File()
      opFile.LFN = archiveLFN
      replicateAndRegisterTarBall.addFile(opFile)
      request.addOperation(replicateAndRegisterTarBall)

      checkMigrationTarBall = Operation()
      checkMigrationTarBall.Type = 'CheckMigration'
      checkMigrationTarBall.TargetSE = self.switches.get('FinalSE', 'CERN-SRM')
      opFile = File()
      opFile.LFN = archiveLFN
      checkMigrationTarBall.addFile(opFile)
      request.addOperation(checkMigrationTarBall)

    # Register Archive Replica for LFNs
    if self.switches.get('RegisterArchiveReplica'):
      registerArchived = Operation()
      registerArchived.Type = 'RegisterReplica'
      registerArchived.TargetSE = 'CERN-ARCHIVE'
      self.addLFNs(registerArchived, lfnChunk, self.metaData, addPFN=True)
      request.addOperation(registerArchived)

      # Remove all Other Replicas for LFNs
      if self.switches.get('RemoveReplicas'):
        removeArchiveReplicas = Operation()
        removeArchiveReplicas.Type = 'RemoveReplica'
        removeArchiveReplicas.TargetSE = ','.join(self.replicaSEs)
        self.addLFNs(removeArchiveReplicas, lfnChunk, self.metaData)
        request.addOperation(removeArchiveReplicas)

    # Remove all Other Replicas for LFNs
    if self.switches.get('RemoveFiles'):
      removeArchiveFiles = Operation()
      removeArchiveFiles.Type = 'RemoveFile'
      self.addLFNs(removeArchiveFiles, lfnChunk, self.metaData)
      request.addOperation(removeArchiveFiles)

    # Remove Original tarball replica
    if self.switches.get('ReplicateTarball'):
      removeTarballOrg = Operation()
      removeTarballOrg.Type = 'RemoveReplica'
      removeTarballOrg.TargetSE = 'CERN-DST-EOS'
      opFile = File()
      opFile.LFN = archiveLFN
      removeTarballOrg.addFile(opFile)
      request.addOperation(removeTarballOrg)
    return request

  def putOrRunRequests(self):
    """Run or put requests."""
    handlerDict = {}
    handlerDict['ArchiveFiles'] = 'ILCDIRAC.DataManagementSystem.Agent.RequestOperations.ArchiveFiles'
    handlerDict['CheckMigration'] = 'ILCDIRAC.DataManagementSystem.Agent.RequestOperations.CheckMigration'
    handlerDict['ReplicateAndRegister'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister'
    handlerDict['RemoveFile'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveFile'
    requestIDs = []
    for request in self.requests:
      if self.switches.get('DryRun'):
        from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask
        rq = RequestTask(request.toJSON()['Value'],
                         handlerDict,
                         '/Systems/RequestManagement/Development/Agents/RequestExecutingAgents',
                         'RequestManagement/RequestExecutingAgent', standalone=True)
        rq()
      else:
        putRequest = self.reqClient.putRequest(request)
        if not putRequest['OK']:
          LOG.error('unable to put request %r: %s' % (request.RequestName, putRequest['Message']))
          continue
        requestIDs.append(str(putRequest['Value']))
        LOG.always('Request %r has been put to ReqDB for execution.' % request.RequestName)

    if requestIDs:
      LOG.always('%d requests have been put to ReqDB for execution' % len(requestIDs))
      LOG.always('RequestID(s): %s' % ' '.join(requestIDs))
      LOG.always('You can monitor the request status using the command: dirac-rms-request <requestName/ID>')
      return 0

    LOG.error('No requests created')
    return 1

  @staticmethod
  def addLFNs(operation, lfns, metaData, addPFN=False):
    """Add lfns to operation."""
    from DIRAC.RequestManagementSystem.Client.File import File
    for lfn in lfns:
      metaDict = metaData['Successful'][lfn]
      opFile = File()
      opFile.LFN = lfn
      if addPFN:
        opFile.PFN = lfn
      opFile.Size = metaDict['Size']
      if 'Checksum' in metaDict:
        # should check checksum type, now assuming Adler32 (metaDict['ChecksumType'] = 'AD')
        opFile.Checksum = metaDict['Checksum']
        opFile.ChecksumType = 'ADLER32'
      operation.addFile(opFile)

  def checkArchive(self, archiveLFN):
    """Check that archiveLFN does not exist yet."""
    LOG.notice('Using Tarball: %s' % archiveLFN)
    exists = returnSingleResult(self.fcClient.isFile(archiveLFN))
    LOG.debug('Checking for Tarball existance %r' % exists)
    if exists['OK'] and exists['Value']:
      raise RuntimeError('Tarball %r already exists' % archiveLFN)

    LOG.debug('Checking permissions for %r' % archiveLFN)
    hasAccess = returnSingleResult(self.fcClient.hasAccess(archiveLFN, 'addFile'))
    if not archiveLFN or not hasAccess['OK'] or not hasAccess['Value']:
      LOG.error('Error checking tarball location: %r' % hasAccess)
      raise ValueError('%s is not a valid path, parameter "Name" must be correct' % archiveLFN)

  def _checkReplicaSites(self, request, lfnChunk):
    """Ensure that all lfns can be found at the sourceSE, otherwise add replication operation to request.

    Abort if too many files are not at the source?
    """
    resReplica = self.fcClient.getReplicas(lfnChunk)
    if not resReplica['OK']:
      LOG.error('Failed to get replica information:', resReplica['Message'])
      raise RuntimeError('Failed to get replica information')

    atSource = []
    notAt = []
    failed = []
    sourceSE = self.switches.get('SourceSE', 'CERN-DST-EOS')
    for lfn, replInfo in resReplica['Value']['Successful'].iteritems():
      if sourceSE in replInfo:
        atSource.append(lfn)
      else:
        LOG.warn('LFN %r not found at source, only at: %s' % ','.join(replInfo.keys()))
        notAt.append(lfn)

    for lfn, errorMessage in resReplica['Value']['Failed'].iteritems():
      LOG.error('Failed to get replica info', '%s: %s' % (lfn, errorMessage))
      failed.append(lfn)

    if failed:
      raise RuntimeError('Failed to get replica information')

    self._replicateSourceFiles(request, notAt)

  def _replicateSourceFiles(self, request, lfns):
    """Create the replicateAndRegisterRequest.

    :param request: The request to add the operation to
    :param lfns: list of LFNs
    """
    registerSource = Operation()
    registerSource.Type = 'RegisterReplica'
    registerSource.TargetSE = self.switches.get('SourceSE', 'CERN-DST-EOS')
    self.addLFNs(registerSource, lfns, self.metaData, addPFN=True)
    request.addOperation(registerSource)


if __name__ == '__main__':
  CAR = CreateArchiveRequest()
  CAR.run()
