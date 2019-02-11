'''
Checks the validity of the provided XML files at job submission

Created on Feb 24, 2011

:author: S. Poss
:since: Feb 24, 2011
'''

__RCSID__ = "$Id$"

from DIRAC                                                import S_OK, S_ERROR
from xml.etree.ElementTree                                import ElementTree

def checkXMLValidity(xmlfile):
  """ Check that the xml parsing of the specified xml will not fail when running on the GRID

  :param str xmlfile: path to xml file
  :returns: :func:`S_OK() <DIRAC:DIRAC.Core.Utilities.ReturnValues.S_OK>`, :func:`~DIRAC:DIRAC.Core.Utilities.ReturnValues.S_ERROR`
  """
  tree = ElementTree()
  try:
    tree.parse(xmlfile)
  except Exception as x:
    return S_ERROR("Found problem in file %s: %s %s"%(xmlfile, Exception, x))
  
  return S_OK()
