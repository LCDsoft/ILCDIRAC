#!/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, exit as dexit
import pprint

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

def createTransfoInfo(trans):
  info = []
  info.append(" - It's a %s production" % trans['Type'])
  info.append(" - It's described as %s" % trans["LongDescription"])
  info.append(" - It's part of the %s group" % trans['TransformationGroup'])
  info.append(" - Its status is currently %s" % trans["Status"])
  info.append(" - It uses the %s plugin"%trans['Plugin'])
  info.append(" - Its name is %s" % trans['TransformationName'])
  info.append(" - It was created by %s" % trans['AuthorDN'])
  if 'InputDataQuery' in trans:
    info.append(' - Was input with %s ' % str(trans['InputDataQuery']))
  if 'AddParams' in trans:
    for key, val in trans['AddParams'].items():
      if key == 'SWPackages':
        info.append(" - Uses the software %s" % trans['AddParams']['SWPackages'].replace(";", ", "))
      if key.lower().count("steeringfile"):
        info.append(" - The steering file used for %s is %s" % (key.split("_")[0], trans['AddParams'][key]))
      if key.lower().count("detectormodel"):
        info.append(" - Detector model %s" % trans['AddParams'][key])
      if key.lower().count('trackingstra'):
        info.append(" - Tracking strategy %s" % trans['AddParams'][key])
      if key.count('whizardparams'):
        pp = pprint.PrettyPrinter(indent=4)
        whizp = pp.pformat(eval(trans['AddParams'][key]))
        info.append(" - Uses the following whizard parameters:")
        info.append("      %s" % whizp)
  info.append('')
  
  return info

def createFileInfo(fmeta):
  from DIRAC.Core.Utilities import DEncode
  if 'ProdID' in fmeta:
    del fmeta['ProdID']
  
  info = []
  
  info.append(" - Machine %s" % fmeta['Machine'])
  del fmeta['Machine']
  info.append(" - Energy %sGeV"% fmeta['Energy'])
  del fmeta['Energy']
  if 'MachineParams' in fmeta:
    info.append(' - The machine parameters are %s' % fmeta['MachineParams'])
    del fmeta['MachineParams']

  if 'EvtClass' in fmeta:
    info.append(' - Is among the %s event class' % fmeta['EvtClass'])  
    del fmeta['EvtClass']
  if 'ProcessID' in fmeta:
    info.append(' - Is the ProcessID %s' % str(fmeta['ProcessID']))
    del fmeta['ProcessID']
  elif 'GenProcessID' in fmeta:
    info.append(' - Is the GenProcessID %s' % str(fmeta['GenProcessID']))
    del fmeta['GenProcessID']
  info.append(" - Is the %s event type" % fmeta["EvtType"])
  del fmeta["EvtType"]
 
  if 'Polarisation' in fmeta:
    info.append(" - Has %s polarisation" % fmeta['Polarisation'])
    del fmeta["Polarisation"]
 
  if 'BeamParticle1' in fmeta:
    info.append(" - Beam 1 particle is %s" % fmeta['BeamParticle1'])
    info.append(" - Beam 2 particle is %s" % fmeta['BeamParticle2'])
    del fmeta['BeamParticle1']
    del fmeta['BeamParticle2']
 
  if 'PolarizationB1' in fmeta:
    info.append(' - Has %s polarization for Beam 1 and %s for beam 2' % 
                (fmeta['PolarizationB1'], fmeta['PolarizationB2']))
    del fmeta['PolarizationB1']
    del fmeta["PolarizationB2"]
 
  if 'Datatype' in fmeta:
    if fmeta['Datatype'] == 'gen':
      info.append(' - This is a generator level sample')
    elif fmeta["Datatype"] == 'SIM':
      info.append(" - This is a simulated sample")
    elif fmeta['Datatype'] in ['REC', 'DST']:
      info.append(' - This is a reconstructed sample')
    else:
      info.append(' - The datatype is unknown: %s' % fmeta['Datatype'])
    del fmeta['Datatype']
  
  if "SWPackages" in fmeta:
    info.append(" - Was produced with %s" % ", ".join(fmeta["SWPackages"].split(';')))
    del fmeta["SWPackages"]
  if "SoftwareTag" in fmeta:
    info.append(' - Was produced with %s' % fmeta['SoftwareTag'])
    del fmeta['SoftwareTag']
  if 'ILDConfig' in fmeta:
    info.append(' - Used the %s ILDConfig package' % fmeta["ILDConfig"])
    del fmeta["ILDConfig"]
  if 'DetectorModel' in fmeta:
    info.append(" - Using the %s detector model" % fmeta['DetectorModel'])
    del fmeta['DetectorModel']
  if 'NumberOfEvents' in fmeta:
    info.append(' - Has %s events or less per file' % fmeta['NumberOfEvents'])
    del fmeta['NumberOfEvents']
  if "CrossSection" in fmeta:
    xsec = str(fmeta["CrossSection"])
    del fmeta["CrossSection"]
    if 'CrossSectionError' in fmeta:
      xsec += " +/- "+str(fmeta["CrossSectionError"])
      del fmeta["CrossSectionError"]
    xsec += " fb" 
    info.append(" - Cross section %s" % xsec)
  if "AdditionalInfo" in fmeta:
    try:
      dinfo = DEncode.decode(fmeta["AdditionalInfo"])   
    except:
      dinfo = eval(fmeta["AdditionalInfo"])  
    info.append(" - There is some additional info:")
    
    if type(dinfo) == type({}):
        dictinfo = dinfo
        if 'xsection' in dictinfo:
          if 'sum' in dictinfo['xsection']:
            if 'xsection' in dictinfo['xsection']['sum']:
              xsec= str(dictinfo['xsection']['sum']['xsection'])
              if 'err_xsection' in dictinfo['xsection']['sum']:
                xsec += ' +/- %s' % dictinfo['xsection']['sum']['err_xsection']
              xsec += "fb"  
              info.append('    Cross section %s' % xsec)
                
    else:
      info.append('    %s' % dinfo)
          
    del fmeta["AdditionalInfo"]
  if 'Luminosity' in fmeta:
    info.append(' - Sample corresponds to a luminosity of %sfb'%fmeta["Luminosity"])
    del fmeta['Luminosity']
    
  if 'Ancestors' in fmeta:
    if len(fmeta["Ancestors"]):
      info.append(" - Was produced from:")
      for anc in fmeta["Ancestors"]:
        info.append('    %s' % anc)
    del fmeta["Ancestors"]
    
  if 'Descendants' in fmeta:
    if len(fmeta["Descendants"]):
      info.append(" - Gave the following files:")
      for des in fmeta["Descendants"]:
        info.append('    %s' % des)
    del fmeta["Descendants"]

  if 'DetectorType' in fmeta:
    #We don't need this here
    del fmeta['DetectorType']

  if fmeta:
    info.append('Remaining metadata: %s' % str(fmeta))  
    
        
  return info
  
if __name__ == "__main__":
  clip = Params()
  clip.registerSwitch()
  Script.parseCommandLine()
  
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
  trans = None
  info = []
  
  if clip.prodid:
    res = tc.getTransformation(clip.prodid)
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)
    trans = res['Value']
    res = tc.getTransformationInputDataQuery( clip.prodid )
    if res['OK']:
      trans['InputDataQuery'] = res['Value']
    res = tc.getAdditionalParameters ( clip.prodid )
    if res['OK']:
      trans['AddParams'] = res['Value']
    #do something with transf
    res = fc.findDirectoriesByMetadata({'ProdID':clip.prodid})
    if res['OK']:
      if len(res['Value'].values()):
        gLogger.verbose("Found some directory matching the metadata")
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
    info.append("")
    info.append("Production %s has the following parameters:" % trans['TransformationID'])
    info.extend(createTransfoInfo(trans))
    if fmeta:
      info.append('The files created by this production have the following metadata:')
      info.extend(createFileInfo(fmeta))
      info.append("It's possible that some meta data was not brought back,")
      info.append("in particular file level metadata, so check some individual files")  
  
  if clip.file:
    f = clip.file
    pid = ""
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
      if 'ProdID' in fmeta:
        pid = str(fmeta['ProdID'])
      res = fc.getFileAncestors([f], 1) 
      if res["OK"]:
        for lfn,ancestorsDict in res['Value']['Successful'].items():
          if ancestorsDict.keys():
            fmeta["Ancestors"] = ancestorsDict.keys()
      res = fc.getFileDescendents([f], 1)
      if res["OK"]:
        for lfn,descendDict in res['Value']['Successful'].items():
          if descendDict.keys():
            fmeta['Descendants'] = descendDict.keys()  
    else:
      ext = f.split(".")[-1]
      fitems = []
      [fitems.extend(i.split('_')) for i in f.split('.')[:-1]]
      pid = ''
      if ext == 'stdhep':
        pid = fitems[fitems.index('gen')+1]
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
      res = fc.findDirectoriesByMetadata({'ProdID':int(pid)})
      if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
      dir_ex = res['Value'].values()[0]
      fpath = ""
      if int(dir_ex.split("/")[-1]) == int(pid):
        fpath = dir_ex+last_folder+"/"
      elif int(dir_ex.split("/")[-2]) == int(pid):
        fpath = "/".join(dir_ex.split('/')[:-2])+"/"+pid.zfill(8)+"/"+last_folder+"/"
      else:
        gLogger.error('Path does not follow conventions, will not get file family')
      
      if fpath:
        fpath += f
        res = fc.getFileAncestors([fpath], 1) 
        if res["OK"]:
          for lfn,ancestorsDict in res['Value']['Successful'].items():
            fmeta["Ancestors"] = ancestorsDict.keys()
        res = fc.getFileDescendents([fpath], 1)
        if res["OK"]:
          for lfn,descendDict in res['Value']['Successful'].items():
            fmeta['Descendants'] = descendDict.keys()
              
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
      res = tc.getTransformationInputDataQuery( pid )
      if res['OK']:
        trans['InputDataQuery'] = res['Value']
      res = tc.getAdditionalParameters ( pid )
      if res['OK']:
        trans['AddParams'] = res['Value']
    info.append("")
    info.append("Input file has the following properties:")
    info.extend(createFileInfo(fmeta))  
    info.append("")
    info.append('It was created with the production %s:' % pid)
    if trans:
      info.extend(createTransfoInfo(trans))
        
  gLogger.notice("\n".join(info))
      
  dexit(0)