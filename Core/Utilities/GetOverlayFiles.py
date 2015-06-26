'''
List the content of the overlay directory given a background type. Called from L{PrepareXMLFile}.

@author: S. Poss
@since: Jan 28, 2011
'''
__RCSID__ = "$Id$"

from DIRAC import gLogger
import os

def getOverlayFiles(evttype = 'gghad'):
  """ Return the list of files contained in the overlay_BKG folder, where BKG can be anything
  """
  localfiles = []
  ovdir = os.path.join(os.getcwd() , "overlayinput_"+evttype )

  if not os.path.exists( ovdir ):
    gLogger.error( 'overlay directory does not exists', "overlayinput_"+evttype )
    return localfiles
  listdir = os.listdir( ovdir )
  for item in listdir:
    if item.count( '.slcio' ):
      localfiles.append( os.path.join( ovdir, item ) )
  return localfiles
