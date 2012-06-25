'''
Created on Apr 18, 2012

@author: Stephane Poss
'''
from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from types import StringTypes
import os

ModelsDict = {}

def initializeLesHouchesFileManagerHandler( serviceInfo ):
  res = gConfig.getOptionsDict("/Operations/Models")
  if not res['OK']:
    return res
  templates = res['Value']
  cfgPath = serviceInfo['serviceSectionPath']
  location = ''
  location = gConfig.getValue( "%s/BasePath" % cfgPath, location  )
  if not location:
    gLogger.error( 'Path to LesHouches files not defined' )
    return S_ERROR("Path to LesHouches files not defined in CS")
  missing = False
  global ModelsDict
  for template,tfile in templates.items():
    ModelsDict[template] = {}
    ModelsDict[template]['file'] = tfile
    if not tfile:
      ModelsDict[template]['content'] = ['']
      continue
    file_path = os.path.join([location,tfile])
    if not os.path.exists(file_path):
      gLogger.error("Missing %s" % file_path)
      missing = True
      break
    LesHouchesFile = open(file_path,"r")
    ModelsDict[template]['content'] = LesHouchesFile.readlines()
    LesHouchesFile.close()
    
  if missing:
    return S_ERROR("File missing")  
    
  return S_OK()

class LesHouchesFileManagerHandler(RequestHandler):
  '''
  classdocs
  '''
  types_getLesHouchesFile = [StringTypes]
  def export_getLesHouchesFile(self,ModelName):
    if not ModelName in ModelsDict:
      return S_ERROR("Unavailable template")
    
    return S_OK(ModelsDict[ModelName]['content'])
  