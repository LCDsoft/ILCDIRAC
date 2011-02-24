'''
Created on Feb 24, 2011

@author: sposs
'''
from DIRAC                                                import S_OK, S_ERROR
from xml.etree.ElementTree                                import ElementTree

def CheckXMLValidity(xmlfile):
  tree = ElementTree()
  try:
    tree.parse(xmlfile)
  except Exception,x:
    return S_ERROR("Found problem in file %s: %s %s"%(xmlfile,Exception,x))
  
  return S_OK()