"""
Utilities to deal with the normalisation of LFNs for various occasions

:author: sailer
"""

__RCSID__= "$Id$"

# use posixpath because it always uses "/"
import posixpath
from DIRAC import gLogger

LOG = gLogger.getSubLogger(__name__)

def joinPathForMetaData( *args ):
  """
  Returns path expected by MetaDataDictionaries, always ends with a slash
  When paths are used for metadata, the ending "/" will be rstripped.
  """
  # if there is a lone slash in this list then the end result is only "/" so we remove them
  cleanedEntries = tuple( ent for ent in args if ent != "/")
  LOG.debug("After cleaning", cleanedEntries)
  finalPath = ""
  for entry in cleanedEntries:
    LOG.debug("This entry", entry)
    finalPath = posixpath.join(finalPath, entry)
  LOG.debug("After Joining", finalPath)
  finalPath = posixpath.normpath(finalPath)
  LOG.debug("After norming", finalPath)
  finalPath = finalPath + "/"
  LOG.verbose("Final Path ", finalPath)
  return finalPath


def cleanUpLFNPath(lfn):
  """Normalise LFNs and remove 'LFN:' prefix."""
  LOG.debug("LFN before Cleanup", lfn)
  lfn = posixpath.normpath(lfn)
  if lfn.lower().startswith('lfn'):
    LOG.debug("LFN starts with lfn:'")
    lfn = lfn[4:]
  LOG.verbose("LFN after Cleanup", lfn)
  return lfn
