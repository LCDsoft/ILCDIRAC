'''
Created on Apr 18, 2012

@author: Stephane Poss
'''
from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from types import *
import os

templatesDict = {}

def initializeLesHouchesFileHandler( serviceInfo ):
  res = gConfig.getOptionsDict("/Operations/Models")
  if not res['OK']:
    return res
  templates = res['Value']
  
  location = gConfig.getValue('/Operations/Models/Path','')
  if not location:
    return S_ERROR("Path to LesHouches files not defined in CS")
  missing = False
  global templatesDict
  for template,file in templates.items():
    templatesDict[template] = {}
    templatesDict[template]['file']= file
    file_path = os.path.join([location,file])
    if not os.path.exists(file_path):
      gLogger.error("Missing %s"%file_path)
      missing = True
      break
    LesHouchesFile = open(file_path,"r")
    templatesDict[template]['content'] = LesHouchesFile.readlines()
    LesHouchesFile.close()
    
  if missing:
    return S_ERROR("File missing")  
    
  return S_OK()

class LesHouchesFileHandler(RequestHandler):
  '''
  classdocs
  '''
  types_getTemplate = [StringTypes]
  def export_getTemplate(self,templateName):
    if not templateName in templatesDict:
      return S_ERROR("Unavailable template")
    
    return S_OK(templatesDict[templateName]['content'])
  