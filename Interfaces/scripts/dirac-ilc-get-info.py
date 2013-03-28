#!/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, exit as dexit

class Params(object):
  def __init__(self):
    self.file = ""
    self.prodid = 0
    
  def setFile(self, opt):
    self.file = opt
    return S_OK()

  def setProdID(self, opt):
    try:
      self.prodid = int(opt)
    except ValueError:
      return S_ERROR('Prod ID must be integer')
    return S_OK()
  
  def registerSwitch(self):
    Script.registerSwitch('p:', "ProductionID=", "Production ID", self.setProdID)
    Script.registerSwitch('f:', "File=", "File name", self.setFile)    
    Script.setUsageMessage("%s -p 12345" % Script.scriptName)
  
if __name__ == "__main__":
  clip = Params()
  clip.registerSwitch()
  Script.parseCommandLine(script = True)
  
  if not clip.prodid and not clip.file:
    Script.showHelp()
    dexit(1)
  
  from DIRAC import gLogger
  import os
  
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  tc = TransformationClient()  

  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  fc = FileCatalogClient()
  fmeta = {}

  if clip.prodid:
    res = tc.getTransformation(clip.prodid)
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)
    trans = res['Value']
    #do something with transf
    res = fc.findDirectoriesByMetadata({'ProdID':clip.prodid})
    if res['OK']:
      if len(res['Value'].values()):
        for dirs in res['Value'].values():
          res = fc.getDirectoryMetadata(dirs)
          if res['OK']:
            fmeta.update(res['Value'])
          else:
            gLogger.warn("Failed to get dir metadata")
          res = fc.listDirectory(dirs)
          if not res['OK']:
            continue
          content = res['Value']['Successful'][dirs]
          if content["Files"]:
            for f_ex in content["Files"].keys():
              res = fc.getFileUserMetadata(f_ex)
              if res['OK']:
                fmeta.update(res['Value'])
                break
          
    #here we have trans and fmeta
    gLogger.notice("got trans and fmeta")
            
      

  
  if clip.file:
    f = clip.file
    if f.count("/"):
      fpath = os.path.dirname(f)
      res = fc.getDirectoryMetadata(fpath)
      if not res['OK']:
        gLogger.error(res['Message'])
        dexit(0)
      fmeta.update(res['Value'])
      res = fc.getFileUserMetadata(f)
      if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
      fmeta.update(res['Value'])
    else:
      ext = f.split(".")[-1]
      fitems = []
      [fitems.extend(i.split('_')) for i in a.split('.')[:-1]]
      pid = ''
      if ext == 'stdhep':
        pid = fitems[fitems.index('gen')+1]
        print pid
      if ext == 'slcio':
        if 'rec' in fitems:
          pid = fitems[fitems.index('rec')+1]
        elif 'dst' in fitems:
          pid = fitems[fitems.index('dst')+1]
        elif 'sim' in fitems:
          pid = fitems[fitems.index('sim')+1]
        else:
          gLogger.error("This file does not follow the ILCDIRAC production conventions!")
          gLogger.error("Please specify a prod ID directly or check the file.")
          dexit(0)
      #as task follows the prod id, to get it we need
      tid = fitems[fitems.index(pid)+1]
      last_folder = str(int(tid)/1000).zfill(3)
      
      res = fc.findDirectoriesbyMetadata({'ProdID':int(pid)})
      if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
      dir_ex = res['Value'].values()[0]
      if dir_ex.split("/")[-1] == pid:
        fpath = dir_ex+last_folder+"/"
      elif dir_ex.split("/")[-2] == pid:
        fpath = "/".join(dir_ex.split('/')[:-2])+"/"+last_folder+"/"
      else:
        gLogger.error('Path does not follow conventions, will not get file family')
      
      if fpath:
        fpath += f
        res = fc.getFileAncestors() 
        res = fc.getFileDescendents()
          
      res = fc.getDirectoryMetadata(dir_ex)
      if not res['OK']:
        gLogger.error(res['Message'])
      else:
        fmeta.update(res['Value'])
      
      res = tc.getTransformation(pid)
      if not res['OK']:
        gLogger.error(res['Message'])
        gLogger.error('Will proceed anyway')
      else:
        trans = res['Value']
        
  gLogger.notice(str(fmeta))
      
  dexit(0)