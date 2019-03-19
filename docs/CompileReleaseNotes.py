#!/bin/env python
"""
compile the release notes

"""
from __future__ import print_function
import os
import re
import sys

from DIRAC.Core.Utilities import Distribution
from DIRAC import gLogger, S_OK, S_ERROR

def doit( tag=None ):
  """compile release notes rst file"""
  res = __generateReleaseNotes( tag )
  if not res['OK']:
    print(res['Message'])
    return 1
  return 0


def __loadReleaseNotesFile( ):
  """load release notes file """
  relNotes = os.path.join(os.environ.get("DIRAC"), "ILCDIRAC" , "release.notes" )
  if not os.path.isfile( relNotes ):
    return S_OK( "" )
  try:
    fd = open( relNotes, "r" )
    relaseContents = fd.readlines()
    fd.close()
  except (OSError, IOError) as excp:
    return S_ERROR( "Could not open %s: %s" % ( relNotes, excp ) )
  gLogger.info( "Loaded %s" % relNotes )
  relData = []
  version = False
  feature = False
  lastKey = False
  for rawLine in relaseContents:
    rawLine = makeJiraLink( rawLine )
    rawLine = makeDIRACLink( rawLine )
    rawLine = makeILCDIRACMRLink( rawLine )
    line = rawLine.strip()
    if not line:
      continue
    if line[0] == "[" and line[-1] == "]":
      version = line[1:-1].strip()
      relData.append( ( version, { 'comment' : [], 'features' : [] } ) )
      feature = False
      lastKey = False
      continue
    if line[0] == "*":
      feature = line[1:].strip()
      relData[-1][1][ 'features' ].append( [ feature, {} ] )
      lastKey = False
      continue
    if not feature:
      relData[ -1 ][1][ 'comment' ].append( rawLine )
      continue
    keyDict = relData[-1][1][ 'features' ][-1][1]
    foundKey = False
    for key in ( 'BUGFIX', 'BUG', 'FIX', "CHANGE", "NEW", "FEATURE" ):
      if line.find( "%s:" % key ) == 0:
        line = line[ len( key ) + 2: ].strip()
      elif line.find( "%s " % key ) == 0:
        line = line[ len( key ) + 1: ].strip()
      else:
        continue
      foundKey = key
      break

    if foundKey in ( 'BUGFIX', 'BUG', 'FIX' ):
      foundKey = 'BUGFIX'
    elif foundKey in ( 'NEW', 'FEATURE' ):
      foundKey = 'FEATURE'

    if foundKey:
      if foundKey not in keyDict:
        keyDict[ foundKey ] = []
      keyDict[ foundKey ].append( line )
      lastKey = foundKey
    elif lastKey:
      keyDict[ lastKey ][-1] += " %s" % line

  return S_OK( relData )

def makeJiraLink( text ):
  """ turn ILCDIRAC-XYZ into a link to jira """
  jiraBaseLink = "https://its.cern.ch/jira/browse/"
  text = re.sub( "(ILCDIRAC-[0-9]+)" , r"`\g<1> <%s\g<1>>`_" % jiraBaseLink, text )
  return text

def makeDIRACLink( text ):
  """ turn DIRAC Version string into link to dirac release notes"""
  diracLinkBase="http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/installSource/releasenotes.DIRAC."
  text = re.sub( "(v6r[0-9]+p[0-9]+)", r"`\g<1> <%s\g<1>%s>`_" %(diracLinkBase, ".html" ) , text )
  return text

def makeILCDIRACMRLink( text ):
  """ turn (!123) into link to merge request """
  ilcdiracLinkBase="https://gitlab.cern.ch/CLICdp/iLCDirac/ILCDIRAC/merge_requests/"
  text = re.sub( r"\(!([0-9]+)\)", r"`(!\g<1>) <%s\g<1>>`_" %(ilcdiracLinkBase, ) , text )
  return text

  
def __generateReleaseNotes( version ):
  """create rst file from release notes """
  result = __loadReleaseNotesFile()
  if not result[ 'OK' ]:
    return result
  releaseData = result[ 'Value' ]
  gLogger.info( "Loaded release.notes" )
  for rstFileName, singleVersion in ( ( "releasenotes.rst", True ),
                                      ( "releasehistory.rst", False ) ):
    result = __generateRSTFile( releaseData, rstFileName, version, singleVersion)
    if not result[ 'OK' ]:
      gLogger.error( "Could not generate %s: %s" % ( rstFileName, result[ 'Message' ] ) )
      continue
  return S_OK()

def __generateRSTFile( releaseData, rstFileName, pkgVersion, singleVersion ):
  """create the rst file from the release.notes """
  rstData = []
  parsedPkgVersion = Distribution.parseVersionString( pkgVersion ) if pkgVersion is not None else (99, 99, 99, 99)
  for version, verData in releaseData:
    if singleVersion and version != pkgVersion and pkgVersion is not None:
      continue
    if Distribution.parseVersionString( version ) > parsedPkgVersion:
      continue
    versionLine = "Version %s" % version
    rstData.append( "" )
    rstData.append( "=" * len( versionLine ) )
    rstData.append( versionLine )
    rstData.append( "=" * len( versionLine ) )
    rstData.append( "" )
    if verData[ 'comment' ]:
      rstData.append( "\n".join( verData[ 'comment' ] ) )
      rstData.append( "" )
    for feature, featureData in verData[ 'features' ]:
      if not featureData:
        continue
      rstData.append( feature )
      rstData.append( "=" * len( feature ) )
      rstData.append( "" )
      for key in sorted( featureData ):
        rstData.append( key.capitalize() )
        rstData.append( ":" * ( len( key ) + 5 ) )
        rstData.append( "" )
        for entry in featureData[ key ]:
          rstData.append( " - %s" % entry )
        rstData.append( "" )
    if pkgVersion is None and singleVersion:
      print("Just taking the first entry in releaseNotes")
      break
  #Write releasenotes.rst
  try:
    rstFilePath = os.path.join( os.getcwd(), "source", rstFileName )
    fd = open( rstFilePath, "w" )
    fd.write( "\n".join( rstData ) )
    fd.close()
  except (OSError,IOError) as excp:
    return S_ERROR( "Could not write %s: %s" % ( rstFileName, excp ) )
  return S_OK()

  
  
if __name__=="__main__":
  print("args", sys.argv)
  TAG = sys.argv[1] if len(sys.argv) > 1 else None
  TAG = os.environ.get( "CI_COMMIT_TAG", TAG )
  print("Tag found?:", TAG)
  exit( doit( TAG ))
