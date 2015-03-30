#!/bin/env python
'''
Created on Mar 20, 2013

@author: stephane
'''
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK
from types import ListType, DictType
from DIRAC.Core.Utilities import uniqueElements
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class Params(object):
  """Parameter Object"""
  def __init__(self):
    self.printOnlyDirectories = False


  def setPrintOnlyDs(self,dummy_opt):
    self.printOnlyDirectories = True
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch("D", "OnlyDirectories", "Print only directories", self.setPrintOnlyDs)
    Script.setUsageMessage("""%s [-D] path meta1=A meta2=B etc.""" % Script.scriptName)


def createQueryDict(argss):
  """
  Create a proper dictionary, stolen from FC CLI
  """  
  
  fileCatClient = FileCatalogClient()
  result = fileCatClient.getMetadataFields()

  if not result['OK']:
    gLogger.error("Failed checking for metadata fields")
    return None
  if not result['Value']:
    gLogger.error('No meta data fields available')
    return None
  typeDict = result['Value']['FileMetaFields']
  typeDict.update(result['Value']['DirectoryMetaFields'])
  metaDict = {}
  contMode = False
  for arg in argss:
    if not contMode:
      operation = ''
      for op in ['>=','<=','>','<','!=','=']:
        if arg.find(op) != -1:
          operation = op
          break
      if not operation:
        gLogger.error("Error: operation is not found in the query")
        return None
        
      name,value = arg.split(operation)
      if not name in typeDict:
        gLogger.error("Error: metadata field %s not defined" % name)
        return None
      mtype = typeDict[name]
    else:
      value += ' ' + arg
      value = value.replace(contMode,'')
      contMode = False  
    
    if value[0] in ['"', "'"]:
      if value[-1] not in ['"', "'"]:
        contMode = value[0]
        continue 
    
    if value.find(',') != -1:
      valueList = [ x.replace("'","").replace('"','') for x in value.split(',') ]
      mvalue = valueList
      if mtype[0:3].lower() == 'int':
        mvalue = [ int(x) for x in valueList if not x in ['Missing','Any'] ]
        mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
      if mtype[0:5].lower() == 'float':
        mvalue = [ float(x) for x in valueList if not x in ['Missing','Any'] ]
        mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
      if operation == "=":
        operation = 'in'
      if operation == "!=":
        operation = 'nin'    
      mvalue = {operation:mvalue}  
    else:            
      mvalue = value.replace("'","").replace('"','')
      if not value in ['Missing','Any']:
        if mtype[0:3].lower() == 'int':
          mvalue = int(value)
        if mtype[0:5].lower() == 'float':
          mvalue = float(value)               
      if operation != '=':     
        mvalue = {operation:mvalue}      
                              
    if name in metaDict:
      if type(metaDict[name]) == DictType:
        if type(mvalue) == DictType:
          op,value = mvalue.items()[0]
          if op in metaDict[name]:
            if type(metaDict[name][op]) == ListType:
              if type(value) == ListType:
                metaDict[name][op] = uniqueElements(metaDict[name][op] + value)
              else:
                metaDict[name][op] = uniqueElements(metaDict[name][op].append(value))     
            else:
              if type(value) == ListType:
                metaDict[name][op] = uniqueElements([metaDict[name][op]] + value)
              else:
                metaDict[name][op] = uniqueElements([metaDict[name][op],value])       
          else:
            metaDict[name].update(mvalue)
        else:
          if type(mvalue) == ListType:
            metaDict[name].update({'in':mvalue})
          else:  
            metaDict[name].update({'=':mvalue})
      elif type(metaDict[name]) == ListType:   
        if type(mvalue) == DictType:
          metaDict[name] = {'in':metaDict[name]}
          metaDict[name].update(mvalue)
        elif type(mvalue) == ListType:
          metaDict[name] = uniqueElements(metaDict[name] + mvalue)
        else:
          metaDict[name] = uniqueElements(metaDict[name].append(mvalue))      
      else:
        if type(mvalue) == DictType:
          metaDict[name] = {'=':metaDict[name]}
          metaDict[name].update(mvalue)
        elif type(mvalue) == ListType:
          metaDict[name] = uniqueElements([metaDict[name]] + mvalue)
        else:
          metaDict[name] = uniqueElements([metaDict[name],mvalue])          
    else:            
      metaDict[name] = mvalue         

  
  return metaDict

def findInFC():
  """Find something in the FileCatalog"""
  from DIRAC import exit as dexit
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  if len(args)<2:
    gLogger.error('Not enough arguments')
    Script.showHelp()
    dexit(1)
    
  path = args[0]
  if path == '.':
    path = '/'
  gLogger.verbose("Path:", path)
  metaQuery = args[1:]
  metaDataDict = createQueryDict(metaQuery)
  gLogger.verbose("Query:",str(metaDataDict))
  if not metaDataDict:
    gLogger.info("No query")
    dexit(1)
  
  fc = FileCatalogClient()
  res = fc.findFilesByMetadata(metaDataDict, path)
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  if not res['Value']:
    gLogger.notice("No files found")

  listToPrint = None

  if clip.printOnlyDirectories:
    listToPrint = set( "/".join(fullpath.split("/")[:-1]) for fullpath in res['Value'] )
  else:
    listToPrint = res['Value']

  for entry in listToPrint:
    print entry

  dexit(0)

if __name__ == '__main__':
  findInFC()
