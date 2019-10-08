#!/bin/env python
""" script to obtain release notes from iLCDirac MRs from GitLab
"""


from collections import defaultdict
from datetime import datetime, timedelta
import argparse
import json
import logging
import subprocess
from pprint import pformat

import requests

# ILCDIRAC Gitlab project ID
ILCDIRAC_ID = 320

LOGGER = logging.getLogger()
logging.basicConfig(level=logging.WARNING, format='%(levelname)-5s - %(name)-8s: %(message)s')


try:
  from GitTokens import GITLABTOKEN
except ImportError:
  raise ImportError("""Failed to import GITLABTOKEN please point the pythonpath to your GitTokens.py file which contains your "Personal Access Token" for Github

                    I.e.:
                    Filename: GitTokens.py
                    Content:
                    ```
                    GITLABTOKEN = "e0b83063396fc632646603f113437de9"
                    ```
                    (without the triple quotes)
                    """
                   )

SESSION = requests.Session()
SESSION.headers.update({"PRIVATE-TOKEN": GITLABTOKEN})


def _parsePrintLevel(level):
  """Translate debug count to logging level."""
  level = level if level <= 2 else 2
  return [logging.INFO,
          logging.INFO,
          logging.DEBUG,
          ][level]


def req2Json(url, parameterDict=None, requestType='GET'):
  """Call to gitlab API using requests package."""
  log = LOGGER.getChild("Requests")
  log.debug("Running %s with %s ", requestType, parameterDict)
  req = getattr(SESSION, requestType.lower())(url, json=parameterDict)
  if req.status_code not in (200, 201):
    log.error("Unable to access API: %s", req.text)
    raise RuntimeError("Failed to access API")

  log.debug("Result obtained:\n %s", pformat(req.json()))
  return req.json()


def getCommands( *args ):
  """ create a flat list

  :param *args: list of strings or tuples/lists
  :returns: flattened list of strings
  """
  comList = []
  for arg in args:
    if isinstance( arg, (tuple, list) ):
      comList.extend( getCommands( *arg ) )
    else:
      comList.append(arg)
  return comList

def gitlab( action ):
  """ return URL for gitlab using proper ID and action needed

  :param str action: command to use in the gitlab API, see documentation there
  :returns: url to be used by curl
  """
  return "https://gitlab.cern.ch/api/v4/projects/%d/%s" % (ILCDIRAC_ID, action)

def glHeaders():
  """ returns header with the private token needed to acces Gitlab

  :returns: tuple to be used in commands list
  """
  return '--header','PRIVATE-TOKEN: %s' % GITLABTOKEN


def curl2Json( *commands, **kwargs ):
  """ return the json object from calling curl with the given commands

  :param *commands: list of strings or tuples/lists, will be passed to `getCommands` to be flattend
  :returns: json object returned from the github or gitlab API
  """
  commands = getCommands( *commands )
  commands.insert( 0, 'curl' )
  commands.insert( 1, '-s' )
  jsonText = subprocess.check_output( commands )
  try:
    jsonList = json.loads( jsonText )
  except ValueError:
    if kwargs.get("checkStatusOnly", False):
      return jsonText
    raise
  return jsonList

def getGitlabPRs( state="opened" ):
  """ get PRs in the gitlab repository """
  glURL = gitlab('merge_requests?state=%s&order_by=updated_at' % state)
  return req2Json(glURL)


def getFullSystemName( name ):
  name = {
    'API': 'Interfaces',
    'AS': 'AccountingSystem',
    'CS': 'ConfigurationSystem',
    'Config': 'ConfigurationSystem',
    'Configuration': 'ConfigurationSystem',
    'DMS': 'DataManagementSystem',
    'DataManagement': 'DataManagementSystem',
    'FS': 'FrameworkSystem',
    'Framework': 'FrameworkSystem',
    'MS': 'MonitoringSystem',
    'Monitoring': 'MonitoringSystem',
    'RMS': 'RequestManagementSystem',
    'RequestManagement': 'RequestManagementSystem',
    'RSS': 'ResourceStatusSystem',
    'ResourceStatus': 'ResourceStatusSystem',
    'SMS': 'StorageManagamentSystem',
    'StorageManagement': 'StorageManagamentSystem',
    'TS': 'TransformationSystem',
    'TMS': 'TransformationSystem',
    'Transformation': 'TransformationSystem',
    'WMS': 'WorkloadManagementSystem',
    'Workload': 'WorkloadManagementSystem',
    'ITS': 'ILCTransformationSystem',
  }.get( name, name )

  return name

def parseForReleaseNotes( commentBody ):
  """ will look for "BEGINRELEASENOTES / ENDRELEASENOTES" and extend releaseNoteList if there are entries """

  relNotes = ''
  if not all( tag in commentBody for tag in ("BEGINRELEASENOTES", "ENDRELEASENOTES") ):
    return relNotes

  releaseNotes=commentBody.split("BEGINRELEASENOTES")[1].split("ENDRELEASENOTES")[0]
  relNotes = releaseNotes

  return relNotes

def collateReleaseNotes( prs ):
  """put the release notes in the proper order

  FIXME: Tag numbers could be obtained by getting the last tag with a name similar to
  the branch, will print out just the base branch for now.

  """
  releaseNotes = ""
  for baseBranch, pr in prs.iteritems():
    releaseNotes += "[%s]\n\n" % baseBranch
    systemChangesDict = defaultdict( list )
    for prid, content in pr.iteritems():
      notes = content['comment']
      system = ''
      for line in notes.splitlines():
        line = line.strip()
        if line.startswith("*"):
          system = getFullSystemName( line.strip("*:").strip() )
        elif line:
          splitline = line.split(":", 1)
          if splitline[0] == splitline[0].upper() and len(splitline) > 1:
            line = "%s: (!%s) %s" % (splitline[0], prid, splitline[1].strip() )
          systemChangesDict[system].append( line )

    for system, changes in sorted(systemChangesDict.iteritems()):
      if not system:
        continue
      releaseNotes += "*%s\n\n" % system
      releaseNotes += "\n".join( changes )
      releaseNotes += "\n\n"
    releaseNotes += "\n"

  return releaseNotes

class GitlabInterface( object ):
  """ object to make calls to github API
  """

  def __init__( self, owner='DiracGrid', repo='Dirac'):
    self.owner = owner
    self.repo = repo
    self._options = dict( owner=self.owner, repo=self.repo  )
    self.printLevel = logging.INFO

    self.branches = ['master', 'Rel-v29r0', 'Rel-v30r0']
    self.openPRs = False
    self.startDate = str(datetime.now() - timedelta(days=14))[:10]

  def parseOptions(self):
    """parse the command line options"""
    parser = argparse.ArgumentParser("iLCDirac Release Notes",
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--branches", action="store", default=self.branches,
                        dest="branches", nargs='+',
                        help="branches to get release notes for")

    parser.add_argument("--date", action="store", default=self.startDate, dest="startDate",
                        help="date after which PRs are checked, default (two weeks ago): %s" % self.startDate)

    parser.add_argument("--openPRs", action="store_true", dest="openPRs", default=self.openPRs,
                        help="get release notes for open (unmerged) PRs, for testing purposes")
    parser.add_argument("-d", "--debug", action="count", dest="debug", help="d, dd, ddd", default=0)

    parsed = parser.parse_args()

    self.branches = parsed.branches
    self.startDate = parsed.startDate
    self.openPRs = parsed.openPRs
    self.printLevel = _parsePrintLevel(parsed.debug)
    LOGGER.setLevel(self.printLevel)

  def getNotesFromPRs( self, prs ):
    """ Loop over prs, get base branch, get PR comment and collate into dict of branch:dict( #PRID, dict(comment, mergeDate) ) """

    rawReleaseNotes = defaultdict( dict )

    for pr in prs:
      baseBranch = pr['target_branch']
      if baseBranch not in self.branches:
        continue
      comment = parseForReleaseNotes( pr['description'] )
      prID = pr['iid']
      mergeDate = pr.get('updated_at', None)
      LOGGER.info("PR: %s, %s, %s", baseBranch, prID, mergeDate[:10])
      mergeDate = mergeDate if mergeDate is not None else '9999-99-99'
      if mergeDate[:10] < self.startDate:
        continue

      rawReleaseNotes[baseBranch].update( {prID: dict(comment=comment, mergeDate=mergeDate)} )

    return rawReleaseNotes


  def getReleaseNotes( self ):

    if self.openPRs:
      prs = getGitlabPRs(state='all')
    else:
      prs = getGitlabPRs(state='merged')
    LOGGER.debug(pformat(prs))
    prs = self.getNotesFromPRs( prs )
    releaseNotes = collateReleaseNotes( prs )
    LOGGER.info("\n%s", releaseNotes)


if __name__ == "__main__":
  RUNNER = GitlabInterface()
  try:
    RUNNER.parseOptions()
  except RuntimeError as e:
    LOGGER.error("Error during runtime: %r", e)
    exit(1)

  try:
    RUNNER.getReleaseNotes()
  except RuntimeError as e:
    exit(1)
