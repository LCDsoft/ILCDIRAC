"""
Utilities to deal with the normalisation of LFNs for various occasions

:author: sailer
"""

__RCSID__= "$Id$"

# use posixpath because it always uses "/"
import posixpath
from DIRAC import gLogger


def joinPathForMetaData( *args ):
  """
  Returns path expected by MetaDataDictionaries, always ends with a slash
  When paths are used for metadata, the ending "/" will be rstripped.
  """
  # if there is a lone slash in this list then the end result is only "/" so we remove them
  cleanedEntries = tuple( ent for ent in args if ent != "/")
  gLogger.debug ( "After cleaning", cleanedEntries )
  finalPath = ""
  for entry in cleanedEntries:
    gLogger.debug("This entry", entry)
    finalPath = posixpath.join(finalPath, entry)
  gLogger.debug( "After Joining", finalPath )
  finalPath = posixpath.normpath(finalPath)
  gLogger.debug( "After norming", finalPath )
  finalPath = finalPath + "/"
  gLogger.verbose ( "Final Path ", finalPath )
  return finalPath



def cleanUpLFNPath( lfn ):
  """ Normalise LFNs
  """
  gLogger.debug("LFN before Cleanup", lfn)
  lfn = posixpath.normpath(lfn)
  gLogger.verbose("LFN after Cleanup", lfn)
  return lfn
