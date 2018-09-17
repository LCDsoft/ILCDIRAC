"""Find files in the dirac file catalog based on meta data.

Usage::

   dirac-ilc-find-in-FC [-D] PATH Constraint1 [Constraint2 [...]]

It is also possible to use any of these operators >=, <=, >, <, !=, = when using metadata constraints.  The list of
metadata options can be obtained from the :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-filecatalog-cli`
by typing: ``meta show``

For example::

   dirac-ilc-find-in-FC /ilc ProdID>1234 Datatype=DST

to list only the directories containing the files use the "-D" flag::

   dirac-ilc-find-in-FC -D /ilc ProdID>1234 Datatype=DST


.. seealso::

  :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-find-lfns`


:since: Mar 20, 2013
:author: stephane

"""

from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

LOG = gLogger.getSubLogger('')

__RCSID__ = "$Id$"

OPLIST = ['>=','<=','>','<','!=','=']
SCRIPTNAME = "dirac-ilc-find-in-FC"

class _Params(object):
  """Parameter Object"""
  def __init__(self):
    self.printOnlyDirectories = False


  def setPrintOnlyDs(self,dummy_opt):
    self.printOnlyDirectories = True
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch("D", "OnlyDirectories", "Print only directories", self.setPrintOnlyDs)
    Script.setUsageMessage("""%s [-D] path meta1=A meta2=B etc.\nPossible operators for metadata: %s""" % (SCRIPTNAME, OPLIST ) )


def _createQueryDict(argss):
  """
  Create a proper dictionary, stolen from FC CLI
  """  
  
  fileCatClient = FileCatalogClient()
  result = fileCatClient.getMetadataFields()

  if not result['OK']:
    LOG.error("Failed checking for metadata fields")
    return None
  if not result['Value']:
    LOG.error('No meta data fields available')
    return None
  typeDict = result['Value']['FileMetaFields']
  typeDict.update(result['Value']['DirectoryMetaFields'])
  metaDict = {}
  contMode = False
  for arg in argss:
    if not contMode:
      operation = ''
      for op in OPLIST:
        if arg.find(op) != -1:
          operation = op
          break
      if not operation:
        LOG.error("Error: operation is not found in the query")
        return None
        
      name,value = arg.split(operation)
      if name not in typeDict:
        LOG.error("Error: metadata field %s not defined" % name)
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
        mvalue = [ int(x) for x in valueList if x not in ['Missing','Any'] ]
        mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
      if mtype[0:5].lower() == 'float':
        mvalue = [ float(x) for x in valueList if x not in ['Missing','Any'] ]
        mvalue += [ x for x in valueList if x in ['Missing','Any'] ]
      if operation == "=":
        operation = 'in'
      if operation == "!=":
        operation = 'nin'    
      mvalue = {operation:mvalue}  
    else:            
      mvalue = value.replace("'","").replace('"','')
      if value not in ['Missing','Any']:
        if mtype[0:3].lower() == 'int':
          mvalue = int(value)
        if mtype[0:5].lower() == 'float':
          mvalue = float(value)               
      if operation != '=':     
        mvalue = {operation:mvalue}      
                              
    if name in metaDict:
      if isinstance( metaDict[name], dict ):
        if isinstance( mvalue, dict ):
          op,value = mvalue.items()[0]
          if op in metaDict[name]:
            if isinstance( metaDict[name][op], list ):
              if isinstance( value, list ):
                metaDict[name][op] = uniqueElements(metaDict[name][op] + value)
              else:
                metaDict[name][op] = uniqueElements(metaDict[name][op].append(value))     
            else:
              if isinstance( value, list ):
                metaDict[name][op] = uniqueElements([metaDict[name][op]] + value)
              else:
                metaDict[name][op] = uniqueElements([metaDict[name][op],value])       
          else:
            metaDict[name].update(mvalue)
        else:
          if isinstance( mvalue, list ):
            metaDict[name].update({'in':mvalue})
          else:  
            metaDict[name].update({'=':mvalue})
      elif isinstance( metaDict[name], list ):
        if isinstance( mvalue, dict ):
          metaDict[name] = {'in':metaDict[name]}
          metaDict[name].update(mvalue)
        elif isinstance( mvalue, list ):
          metaDict[name] = uniqueElements(metaDict[name] + mvalue)
        else:
          metaDict[name] = uniqueElements(metaDict[name].append(mvalue))      
      else:
        if isinstance( mvalue, dict ):
          metaDict[name] = {'=':metaDict[name]}
          metaDict[name].update(mvalue)
        elif isinstance( mvalue, list ):
          metaDict[name] = uniqueElements([metaDict[name]] + mvalue)
        else:
          metaDict[name] = uniqueElements([metaDict[name],mvalue])          
    else:            
      metaDict[name] = mvalue         

  
  return metaDict

def _findInFC():
  """Find something in the FileCatalog"""
  from DIRAC import exit as dexit
  clip = _Params()
  clip.registerSwitches()
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  if len(args)<2:
    Script.showHelp('ERROR: Not enough arguments')
    LOG.error("Run %s --help" % SCRIPTNAME)
    dexit(1)
    
  path = args[0]
  if path == '.':
    path = '/'

  ## Check that the first argument is not a MetaQuery
  if any( op in path for op in OPLIST ):
    LOG.error("ERROR: Path '%s' is not a valid path! The first argument must be a path" % path)
    LOG.error("Run %s --help" % SCRIPTNAME)
    dexit(1)

  LOG.verbose("Path:", path)
  metaQuery = args[1:]
  metaDataDict = _createQueryDict(metaQuery)
  LOG.verbose("Query:", str(metaDataDict))
  if not metaDataDict:
    LOG.info("No query")
    dexit(1)
  
  fc = FileCatalogClient()
  res = fc.findFilesByMetadata(metaDataDict, path)
  if not res['OK']:
    LOG.error(res['Message'])
    dexit(1)
  if not res['Value']:
    LOG.notice("No files found")

  listToPrint = None

  if clip.printOnlyDirectories:
    listToPrint = set( "/".join(fullpath.split("/")[:-1]) for fullpath in res['Value'] )
  else:
    listToPrint = res['Value']

  for entry in listToPrint:
    print entry

  dexit(0)

if __name__ == '__main__':
  _findInFC()
