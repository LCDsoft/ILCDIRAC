"""Create and put Requests to archive files.

List of operations:

#. ArchiveFiles
#. ReplicateAndRegister Tarball
#. Add ArchiveSE replica for all files
#. Check for Tarball Migration (TODO)
#. Remove all other replicas for these files
#. Remove original replica of Tarball

"""
import os
from pprint import pprint

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base import Script

__RCSID__ = "$Id$"
MAX_SIZE = 2 * 1024 * 1024 * 1024

def registerSwitches():
  switches = {}
  options = [("S", "SourceSE", "Source SE to use"),
             ("F", "FinalSE", "Final SE for tarball"),
             ("A", "ArchiveSE", "SE for registering archive files at"),
             ("T", "TarballSE", "SE to initially upload arball"),
            ]
  flags = [("R", "RegisterArchiveReplica", "Register archived files in ArchiveSE"),
           ("D", "RemoveReplicas", "Remove Replicas from non-ArchiveSE"),
           ("U", "RemoveFiles", "Remove Archived files completely"),
           ("X", "Execute", "Put Requests, else dryrun"),
           ]
  for short, longOption, doc in options:
    Script.registerSwitch(short + ':', longOption + '=', doc)
  for short, longOption, doc in flags:
    Script.registerSwitch(short, longOption, doc)
  Script.setUsageMessage('\n'.join([__doc__,
                                    'Usage:',
                                    ' %s [option|cfgfile] LFNs tarBallName' % Script.scriptName,
                                    'Arguments:',
                                    '         LFNs: file with LFNs',
                                    '  tarBallName: LFN of the tarball',
                                   ]))

  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  if len(args) != 2:
    Script.showHelp()
    DIRAC.exit(1)

  for switch in Script.getUnprocessedSwitches():
    for short, longOption, doc in options:
      if switch[0] == short or switch[0].lower() == longOption.lower():
        switches[longOption] = switch[1]
        break
    for short, longOption, doc in flags:
      if switch[0] == short or switch[0].lower() == longOption.lower():
        switches[longOption] = True
        break
  switches['DryRun'] = not switches.get('Execute', False)
  return args, switches

def getLFNList(arg):
  """ get list of LFNs """
  lfnList = []
  if os.path.exists(arg):
    lfnList = [line.split()[0] for line in open(arg).read().splitlines()]
  else:
    raise ValueError('%s not a file'% arg)
  return list(set(lfnList))


def splitLFNsBySize(lfns):
  """Split LFNs into 2GB chunks.

  :return: list of list of lfns
  """
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  metaData = FileCatalog().getFileMetadata(lfns)
  error = False
  if not metaData["OK"]:
    gLogger.error("Unable to read metadata for lfns: %s" % metaData["Message"])
    raise RuntimeError("Could not read metadata: %s" % metaData['Message'])

  metaData = metaData["Value"]
  for failedLFN, reason in metaData["Failed"].items():
    gLogger.error("skipping %s: %s" % (failedLFN, reason))
    error = True
  if error:
    raise RuntimeError("Could not read all metadata")

  lfnChunks = []
  lfnChunk = []
  totalSize = 0
  for lfn, info in metaData['Successful'].iteritems():
    if totalSize > MAX_SIZE:
      lfnChunks.append(lfnChunk)
      gLogger.info("Created Chunk of %s lfns with %s bytes" % (len(lfnChunk), totalSize))
      lfnChunk = []
      totalSize = 0
    lfnChunk.append(lfn)
    totalSize += info['Size']

  lfnChunks.append(lfnChunk)
  gLogger.info("Created Chunk of %s lfns with %s bytes" % (len(lfnChunk), totalSize))

  replicaSEs = set([seItem for se in FileCatalog().getReplicas(lfns)['Value']['Successful'].values()
                    for seItem in se.keys()])

  return lfnChunks, metaData, replicaSEs


def run(args, switches):
  """Perform checks andcreate the request."""

  lfnList = getLFNList(args[0])
  baseArchiveLFN = archiveLFN = args[1]

  tarballName = os.path.basename(archiveLFN)
  baseRequestName = requestName = 'Archive_%s' % tarballName.rsplit('.', 1)[0]

  gLogger.info("Will create request '%s' with %s lfns" % (requestName, len(lfnList)))

  from DIRAC.RequestManagementSystem.Client.Request import Request
  from DIRAC.RequestManagementSystem.Client.Operation import Operation
  from DIRAC.RequestManagementSystem.Client.File import File
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

  count = 0
  reqClient = ReqClient()
  requestIDs = []

  lfnChunks, metaData, replicaSEs = splitLFNsBySize(lfnList)
  multiRequests = len(lfnChunks) > 1

  for lfnChunk in lfnChunks:
    if not lfnChunk:
      gLogger.error("LFN list is empty!!!")
      return 1

    count += 1
    request = Request()

    if multiRequests:
      requestName = '%s_%d' % (baseRequestName, count)
      archiveLFN = '%s_%d.tar' % (baseArchiveLFN.rsplit('.', 1)[0] , count)

    request.RequestName = requestName
    request.OwnerGroup = 'ilc_user'

    archiveFiles = Operation()
    archiveFiles.Type = "ArchiveFiles"
    archiveFiles.Arguments = DEncode.encode({'SourceSE': switches.get('SourceSE', 'CERN-DST-EOS'),
                                             'TarballSE': switches.get('TarballSE', 'CERN-DST-EOS'),
                                             'ArchiveSE': switches.get('ArchiveSE', 'CERN-ARCHIVE'),
                                             'FinalSE': switches.get('FinalSE', 'CERN-SRM'),
                                             'ArchiveLFN': archiveLFN})
    addLFNs(archiveFiles, lfnChunk, metaData)
    request.addOperation(archiveFiles)

    # Replicate the Tarball, ArchiveFiles will upload it
    replicateAndRegisterTarBall = Operation()
    replicateAndRegisterTarBall.Type = "ReplicateAndRegister"
    replicateAndRegisterTarBall.TargetSE = switches.get('FinalSE', 'CERN-SRM')
    opFile = File()
    opFile.LFN = archiveLFN
    replicateAndRegisterTarBall.addFile(opFile)
    request.addOperation(replicateAndRegisterTarBall)

    # Register Archive Replica for LFNs
    if switches.get("RegisterArchiveReplica"):
      registerArchived = Operation()
      registerArchived.Type = "RegisterReplica"
      registerArchived.TargetSE = 'CERN-ARCHIVE'
      addLFNs(registerArchived, lfnChunk, metaData, addPFN=True)
      request.addOperation(registerArchived)

      if switches.get("RemoveReplicas"):
        # Remove all Other Replicas for LFNs
        removeArchiveReplicas = Operation()
        removeArchiveReplicas.Type = "RemoveReplica"
        removeArchiveReplicas.TargetSE = ','.join(replicaSEs)
        addLFNs(removeArchiveReplicas, lfnChunk, metaData)
        request.addOperation(removeArchiveReplicas)

    if switches.get("RemoveFiles"):
      # Remove all Other Replicas for LFNs
      removeArchiveFiles = Operation()
      removeArchiveFiles.Type = "RemoveFile"
      addLFNs(removeArchiveFiles, lfnChunk, metaData)
      request.addOperation(removeArchiveFiles)

    # Remove Original tarball replica
    removeTarballOrg = Operation()
    removeTarballOrg.Type = "RemoveReplica"
    removeTarballOrg.TargetSE = 'CERN-DST-EOS'
    opFile = File()
    opFile.LFN = archiveLFN
    removeTarballOrg.addFile(opFile)
    request.addOperation(removeTarballOrg)

    pprint(eval(str(request).replace('null', 'None')))
    from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
    valid = RequestValidator().validate(request)
    if not valid["OK"]:
      gLogger.error("putRequest: request not valid", "%s" % valid["Message"])
      return valid

    if switches.get("DryRun"):
      from DIRAC.RequestManagementSystem.private.RequestTask import RequestTask
      handlerDict = {}
      handlerDict['ArchiveFiles'] = 'ILCDIRAC.DataManagementSystem.Agent.RequestOperations.ArchiveFiles'
      rq = RequestTask(request.toJSON()['Value'], handlerDict, '/Systems/RequestManagement/Development/Agents/RequestExecutingAgents',
                       'RequestManagement/RequestExecutingAgent', standalone=True)
      rq()
    else:
      putRequest = reqClient.putRequest(request)
      if not putRequest["OK"]:
        gLogger.error("unable to put request '%s': %s" % (request.RequestName, putRequest["Message"]))
        error = -1
        continue
      requestIDs.append(str(putRequest["Value"]))
      if not multiRequests:
        gLogger.always("Request '%s' has been put to ReqDB for execution." % request.RequestName)

      if multiRequests:
        gLogger.always("%d requests have been put to ReqDB for execution, with name %s_<num>" % (count, requestName))
      if requestIDs:
        gLogger.always("RequestID(s): %s" % " ".join(requestIDs))
      gLogger.always("You can monitor requests' status using command: 'dirac-rms-request <requestName/ID>'")
      DIRAC.exit(error)

def addLFNs(operation, lfns, metaData, addPFN=False):
  """Add lfns to operation."""
  from DIRAC.RequestManagementSystem.Client.File import File
  for lfn in lfns:
    metaDict = metaData["Successful"][lfn]
    opFile = File()
    opFile.LFN = lfn
    if addPFN:
      opFile.PFN = lfn
    opFile.Size = metaDict["Size"]
    if "Checksum" in metaDict:
      # should check checksum type, now assuming Adler32 (metaDict["ChecksumType"] = 'AD')
      opFile.Checksum = metaDict["Checksum"]
      opFile.ChecksumType = "ADLER32"
    operation.addFile(opFile)


if __name__ == "__main__":
  ARGS, SW = registerSwitches()
  run(ARGS, SW)
