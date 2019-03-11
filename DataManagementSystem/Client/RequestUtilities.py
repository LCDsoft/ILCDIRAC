"""Module containing utilities to create Requests."""

import os

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.RequestManagementSystem.Client.File import File

LOG = gLogger.getSubLogger(__name__)


class BaseRequest(object):
  """Base class for creating Request Creator scripts."""
  def __init__(self):
    """Initialise default switches etc."""
    self._fcClient = None
    self._reqClient = None
    self.switches = {}
    self.requests = []
    self.lfnList = []
    self.metaData = None

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

  @property
  def targetSE(self):
    """Return the list of targetSE."""
    return self.switches['TargetSE']

  @property
  def sourceSEs(self):
    """Return the list of sourceSEs."""
    return self.switches['SourceSE']

  @property
  def name(self):
    """Return the name of the Request."""
    return self.switches.get('Name', None)

  @property
  def lfnFolderPath(self):
    """Return the lfn folder Path where to find the files of the Request."""
    return self.switches.get('Path', None)

  @property
  def dryRun(self):
    """Return dry run flag"""
    return self.switches['DryRun']

  def registerSwitchesAndParseCommandLine(self, script):
    """Set flags and options."""
    options = [('S', 'SourceSE', 'Where to remove the LFNs from'),
               ('T', 'TargetSE', 'Where to move the LFNs'),
               ('P', 'Path', 'LFN path to folder, all files in the folder will be archived'),
               ('N', 'Name', 'Name of the Tarball, if not given Path_Tars/Path_N.tar will be used to store tarballs'),
               ('L', 'List', 'File containing list of LFNs to archive, requires Name to be given'),
               ]
    flags = [('X', 'Execute', 'Put Requests, else dryrun'),
             ('', 'RunLocal', 'Run Requests locally'),
             ]
    for short, longOption, doc in options:
      script.registerSwitch(short + ':', longOption + '=', doc)
    for short, longOption, doc in flags:
      script.registerSwitch(short, longOption, doc)

    script.parseCommandLine()
    if script.getPositionalArgs():
      script.showHelp()
      DIRAC.exit(1)

    for switch in script.getUnprocessedSwitches():
      for short, longOption, doc in options:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          self.switches[longOption] = switch[1]
          break
      for short, longOption, doc in flags:
        if switch[0] == short or switch[0].lower() == longOption.lower():
          self.switches[longOption] = True
          break

    self.checkSwitches()

    self.switches['DryRun'] = not self.switches.get('Execute', False)
    self.switches['SourceSE'] = self.switches.get('SourceSE', 'CERN-DST-EOS').split(',')

  def checkSwitches(self):
    """Check consistency of given command line."""
    if not self.switches.get('SourceSE'):
      raise RuntimeError('Have to set "SourceSE"')
    if self.switches.get('List') and not self.name:
      raise RuntimeError('Have to set "Name" with "List"')
    if not self.switches.get('List') and not self.switches.get('Path'):
      raise RuntimeError('Have to set "List" or "Path"')

  def getLFNList(self):
    """Get list of LFNs.

    Either read the provided file, or get the files found beneath the provided folder.
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
    elif self.lfnFolderPath:
      path = self.lfnFolderPath
      LOG.debug('Check if %r is a directory' % path)
      isDir = returnSingleResult(self.fcClient.isDirectory(path))
      LOG.debug('Result: %r' % isDir)
      if not isDir['OK'] or not isDir['Value']:
        LOG.error('Path is not a directory', isDir.get('Message', ''))
        raise RuntimeError('Path %r is not a directory' % path)
      LOG.notice('Looking for files in %r' % path)
      lfns = self.fcClient.findFilesByMetadata(metaDict={}, path=path)
      if not lfns['OK']:
        LOG.error('Could not find files')
        raise RuntimeError(lfns['Message'])
      self.lfnList = lfns['Value']

    if self.lfnList:
      LOG.notice('Will create request(s) with %d lfns' % len(self.lfnList))
      return

    raise ValueError('"Path" or "List" need to be provided!')

  def getLFNMetadata(self):
    """Get the metadata for all the LFNs."""
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

    for lfn in self.metaData['Successful'].keys():
      LOG.verbose('found %s' % lfn)

  def putOrRunRequests(self):
    """Run or put requests."""
    handlerDict = {}
    handlerDict['ArchiveFiles'] = 'ILCDIRAC.DataManagementSystem.Agent.RequestOperations.ArchiveFiles'
    handlerDict['CheckMigration'] = 'ILCDIRAC.DataManagementSystem.Agent.RequestOperations.CheckMigration'
    handlerDict['ReplicateAndRegister'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.ReplicateAndRegister'
    handlerDict['RemoveFile'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveFile'
    handlerDict['RemoveReplica'] = 'DIRAC.DataManagementSystem.Agent.RequestOperations.RemoveReplica'
    requestIDs = []
    dryRun = self.switches['DryRun']
    runLocal = self.switches['RunLocal']

    if dryRun or runLocal:
      LOG.notice('Would have created %d requests' % len(self.requests))
      if not runLocal:
        return 0
      for request in self.requests:
        from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask
        rq = RequestTask(request.toJSON()['Value'],
                         handlerDict,
                         '/Systems/RequestManagement/Development/Agents/RequestExecutingAgents',
                         'RequestManagement/RequestExecutingAgent', standalone=True)
        rq()
      return 0
    for request in self.requests:
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
