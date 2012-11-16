'''
List the content of the overlay directory given a bakground type. Called from L{PrepareXMLFile}.

Created on Jan 28, 2011

@author: sposs
'''
from DIRAC import gLogger
import os

def getOverlayFiles(evttype = 'gghad'):
  """ Return the list of files contained in the overlay_BKG folder, where BKG can be anything
  """
  localfiles = []
  if not os.path.exists( "./overlayinput_"+evttype ):
    gLogger.error( 'overlay directory does not exists' )
    return localfiles
  curdir = os.getcwd()
  os.chdir( "./overlayinput_"+evttype )
  listdir = os.listdir( os.getcwd() )
  for item in listdir:
    if item.count( '.slcio' ):
      localfiles.append( os.getcwd()+os.sep+item )
  os.chdir(curdir)
  return localfiles