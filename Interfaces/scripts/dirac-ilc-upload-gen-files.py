#!/bin/env python
'''
Created on Mar 21, 2013

@author: stephane
'''
__RCSID__ = "$Id"
from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR
import os

mandatory_keys = ['GenProcessID','GenProcessName','NumberOfEvents','BeamParticle1','BeamParticle2',
                  'PolarizationB1','PolarizationB2', 'ProgramNameVersion','CrossSection','CrossSectionError']

class Params(object):
  def __init__(self):
    self.dir = ''
    self.se = ''
    self.machineParams = 'B1b_ws'
    self.evtclass = ''
    self.evttype = ''
    self.lumi = 0.0
    self.p1 = 'e'
    self.p2 = 'p'
    self.pol1 = ''
    self.pol2 = ''
    self.software = 'whizard-1.95'
    self.fmeta = {}
    self.force = False
    self.energy = -1
  def setDir(self, opt):
    self.dir = opt
    return S_OK()
  
  def setSE(self,opt):
    if opt.count(","):
      return S_ERROR('Cannot use a list of storage elements (yet)')
    else:  
      self.se = opt
    return S_OK()
  
  def setMachineParams(self, opt):
    self.machineParams = opt
    return S_OK()

  def setEnergy(self, opt):
    try:
      self.energy = int(opt)
    except ValueError:
      return S_ERROR("Energy should be unit less, only integers")
    return S_OK()
  
  def setProcessID(self,opt):
    try:
      self.fmeta['GenProcessID'] = int(opt)
    except ValueError:
      return S_ERROR("EvtID MUST be integer")
    return S_OK()
  
  def setEvtClass(self,opt):
    self.evtclass = opt
    return S_OK()
  
  def setEvtType(self,opt):
    self.evttype = opt
    self.fmeta['GenProcessName'] = opt
    return S_OK()
  
  def setLumi(self,opt):
    self.lumi = float(opt)
    return S_OK()
  
  def setNumberOfEvents(self,opt):
    self.fmeta['NumberOfEvents'] = int(opt)
    return S_OK()
  
  def setBeamP1(self, opt):
    self.fmeta['BeamParticle1'] = opt
    if opt=='e1':
      self.p1 = 'e'
    elif opt == 'E1':
      self.p1 = 'p'
    else:
      self.p1 = opt
    return S_OK()
  def setBeamP2(self,opt):
    self.fmeta['BeamParticle2'] = opt
    if opt=='e1':
      self.p2 = 'e'
    elif opt == 'E1':
      self.p2 = 'p'
    else:
      self.p2 = opt
    return S_OK()
  def setPol1(self,opt):
    self.fmeta['PolarizationB1'] = opt
    self.pol1 = opt
    return S_OK()
  def setPol2(self,opt):
    self.fmeta['PolarizationB2'] = opt
    self.pol2 = opt
    return S_OK()
  
  def setSoftware(self,opt):
    self.fmeta['ProgramNameVersion'] = opt
    self.software = opt
    return S_OK()
  
  def setXSec(self,opt):
    try:
      self.fmeta['CrossSection'] = float(opt)
    except ValueError:
      return S_ERROR("XSection must be float, unit less")
    return S_OK()
  def setXSecE(self,opt):
    try:
      self.fmeta['CrossSectionError'] = float(opt)
    except ValueError:
      return S_ERROR("XSectionError must be float, unit less")
    return S_OK()
  
  def setForce(self,dummy_opt):
    self.force = True
    return S_OK()
  
  def registerSwitches(self):
    Script.registerSwitch('P:', 'Path=', 'Path where the file(s) are (directory or single file)', self.setDir)
    Script.registerSwitch('S:', "SE=", 'Storage element(s) to use ex: DESY-SRM', self.setSE)
    Script.registerSwitch('M:', "MachineParams=", 'Machine Parameters, default: %s' % self.machineParams, 
                          self.setMachineParams)
    Script.registerSwitch("E:", "Energy=", "Energy in gev, e.g. 1000", self.setEnergy)
    Script.registerSwitch("I:", "EvtID=","Process ID, like 35945",self.setProcessID)
    Script.registerSwitch("C:", "EvtClass=","Process class, like 6f_ttbar",self.setEvtClass)
    Script.registerSwitch("T:", "EvtType=",'Process type, like 6f_yyyyee',self.setEvtType)
    Script.registerSwitch("L:", "Luminosity=",'Luminosity of the sample',self.setLumi)
    Script.registerSwitch("N:", "NumberOfEvents=",'Number of events per file',self.setNumberOfEvents)
    Script.registerSwitch('', 'BeamParticle1=', 'Particle of beam 1, e.g. e1', self.setBeamP1 )
    Script.registerSwitch('', 'BeamParticle2=', 'Particle of beam 2, e.g. E1', self.setBeamP2 )
    Script.registerSwitch('', 'PolarisationBeam1=', 'Polarisation for particle of beam 1: L, R, W', self.setPol1 )
    Script.registerSwitch('', 'PolarisationBeam2=', 'Polarisation for particle of beam 2: L, R, W', self.setPol2 )
    Script.registerSwitch('', 'XSection=', 'Cross section in fb' , self.setXSec)
    Script.registerSwitch('', 'XSectionError=', 'Cross section error in fb', self.setXSecE )
    Script.registerSwitch('', 'Software=', "Software and version, e.g. %s"%self.software, self.setSoftware)
    Script.registerSwitch('f', 'force', "Do not stop for confirmation", self.setForce)
    Script.setUsageMessage('\n%s -P /some/path/ -E 1000 -M B1b_ws -I 35945 etc.\n' % Script.scriptName)  
  
def uploadGenFiles():
  """uploads the generator files"""
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine()

  
  from DIRAC import gLogger, exit as dexit

  if not clip.dir:
    gLogger.error('You need to set the path')
    Script.showHelp()
    dexit(1)
  if not clip.se:
    gLogger.error('You need a storage element')
    Script.showHelp()
    dexit(1)
  
  for key in mandatory_keys:
    if not key in clip.fmeta:
      gLogger.error("All meta data not defined, please check")
      Script.showHelp()
      dexit(1)
    
  #resolve the inout files
  flist = []
  if os.path.isdir(clip.dir):
    flistd = os.listdir(clip.dir)
    for f in flistd:
      if f.count(".stdhep"):
        flist.append( os.path.join(clip.dir, f) )
  elif os.path.isfile(clip.dir):
    flist.append(clip.dir)
  else:
    gLogger.error("%s is not a file nor a directory" % clip.dir)
    dexit(1)  
  
  gLogger.notice("Will eventually upload %s file(s)" % len(flist))
    
  from DIRAC.Core.Utilities.PromptUser import promptUser
    
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  basepath = Operations().getValue('Production/ILC_ILD/BasePath','')
  if not basepath:
    gLogger.error('Failed to contact CS, please try again')
    dexit(1)
  
  basepath = "/".join(basepath.split("/")[:-2])+"/" #need to get rid of the ild/ part at the end
    
  finalpath = os.path.join(basepath, 'generated', clip.energy+"-"+clip.machineParams, clip.evtclass, str(clip.fmeta['GenProcessID']))
  gLogger.notice("Will upload the file(s) under %s" % finalpath)
  if not clip.force:
    res = promptUser('Continue?', ['y','n'], 'n')
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)
    if not res['Value'].lower()=='y':
      dexit(0)
  
  dirmeta = []
  dirmeta.append({'path':os.path.join(basepath, 'generated'), 'meta':{'Datatype':'gen'}})
  dirmeta.append({'path':os.path.join(basepath, 'generated', clip.energy+"-"+clip.machineParams), 'meta':{'Energy':clip.energy, 'MachineParams':clip.machineParams}})
  dirmeta.append({'path':os.path.join(basepath, 'generated', clip.energy+"-"+clip.machineParams, clip.evtclass), 'meta':{'EvtClass':clip.evtclass }})
  dirmeta.append({'path':finalpath, 'meta': {'EvtType':clip.evttype ,'Luminosity':clip.lumi, 'ProcessID': clip.fmeta['GenProcessID']} })
  
  final_fname_base = 'E'+clip.energy+"-"+clip.machineParams+".P"+clip.fmeta['GenProcessName']+".G"+clip.fmeta['ProgramNameVersion'] + "."+clip.p1+clip.pol1+"."+clip.p2+clip.pol2+".I"+str(clip.fmeta['GenProcessID'])
  gLogger.notice("Final file name(s) will be %s where XXX will be replaced by file number, and ext by the input file extension" % (final_fname_base+".XXX.ext") )
  if not clip.force:
    res = promptUser('Continue?', ['y','n'], 'n')
    if not res['OK']:
      gLogger.error(res['Message'])
      dexit(1)
    if not res['Value'].lower()=='y':
      dexit(0)    

  
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  fc = FileCatalogClient()
  
  for pathdict in dirmeta:
    res = fc.createDirectory(pathdict['path'])
    if not res['OK']:
      gLogger.error("Could not create this directory in FileCatalog, abort:", pathdict['path'] )
      dexit(0)

    res = fc.setMetadata(pathdict['path'], pathdict['meta'])
    if not res['OK']:
      gLogger.error("Failed to set meta data %s to %s" %(pathdict['meta'], pathdict['path']))

  rm = ReplicaManager()
  for f in flist:
    fnum = f.split(".")[-2]
    fext = f.split(".")[-1]
    final_fname = final_fname_base + '.' + fnum + "." + fext
    gLogger.notice("Uploading %s to" % f, finalpath+"/"+final_fname)
    if not clip.force:
      res = promptUser('Continue?', ['y','n'], 'n')
      if not res['OK']:
        gLogger.error(res['Message'])
        break
      if not res['Value'].lower()=='y':
        break    

    res = rm.putAndRegister(finalpath+"/"+final_fname, f, clip.se)
    if not res['OK']:
      gLogger.error("Failed to upload %s:" % f, res['Message'])
      continue
    res = fc.setMetadata(finalpath+"/"+final_fname, clip.fmeta)
    if not res['OK']:
      gLogger.error("Failed setting the metadata to %s:" % f, res['Message'])
      
  dexit(0)

if __name__ == '__main__':
  uploadGenFiles()
