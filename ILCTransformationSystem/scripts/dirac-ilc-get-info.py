#!/bin/env python
"""Print production properties and more information for given production

Example

  dirac-ilc-get-info -p 5678

Options:
  -p, --ProductionID prodID      ProductionID
  -f, --File lfn                 LFN from the Production

"""
import pprint

from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR, exit as dexit

__RCSID__ = "$Id$"


class _Params(object):
  """ CLI parameters """
  def __init__(self):
    self.filename = ""
    self.prodid = 0

  def setFilename(self, opt):
    self.filename = opt
    return S_OK()

  def setProdID(self, opt):
    try:
      self.prodid = int(opt)
    except ValueError:
      return S_ERROR('Prod ID must be integer')
    return S_OK()

  def registerSwitches(self):
    Script.registerSwitch('p:', "ProductionID=", "Production ID", self.setProdID)
    Script.registerSwitch('f:', "File=", "File name", self.setFilename)
    Script.setUsageMessage("%s -p 12345" % Script.scriptName)


def _createTransfoInfo(trans):
  """creates information for Transformation"""
  info = []
  parametersToShow = ['Type', 'LongDescription', 'TransformationGroup', 'Status',
                      'Plugin', 'TransformationName', 'AuthorDN', 'InputDataQuery']
  maxLength = len(max(parametersToShow + trans['AddParams'].keys(), key=len))
  for key in parametersToShow:
    if key in trans:
      info.append("    %s: %s" % (key.ljust(maxLength), trans[key]))
  for key, val in trans.get('AddParams', {}).iteritems():
    if key in ['DetailedInfo', 'JobType', 'FCInputQuery', 'detectorType', 'NbInputFiles', 'nbevts', 'Energy'] \
       or any(key.endswith(ext) for ext in('slcio', 'stdhep')):
      continue
    if 'whizardparams' in key:
      pp = pprint.PrettyPrinter(indent=4)
      whizp = pp.pformat(eval(val))
      info.append(" - Uses the following whizard parameters:")
      info.append("      %s" % whizp)
    elif 'SinFile' in key:
      # strip empty lines from the file
      sinFile = [line for line in val.splitlines() if line]
      info.append("    %s: %s" % (key.ljust(maxLength),
                                  ("\n" + " " * (maxLength + 6)).join(sinFile)))
    else:
      info.append("    %s: %s" % (key.ljust(maxLength), val))

  info.append("")
  return info

def _createFileInfo(fmeta):
  """creates information for file"""
  from DIRAC.Core.Utilities import DEncode
  fmeta.pop('ProdID', None)

  info = []

  parametersToShow = ['Machine', 'Energy', 'MachineParams', 'EvtClass', 'ProcessID', 'GenProcessID',
                      'EvtType', 'Polarisation', 'BeamParticle1', 'BeamParticle2',
                      'PolarisationB1', 'PolarisationB2','Datatype',
                      'SWPackages', 'SoftwareTag', 'ILDConfig', 'DetectorModel', 'NumberOfEvents',
                      'CrossSection'
                     ]
  datatypes = dict(GEN="Generator Sample", SIM="Simulated Sample",
                   REC="Reconstructed Sample", DST="Reconstructed Sample")
  maxLength = len(max(parametersToShow, key=len))

  for parameter in parametersToShow:
    if parameter not in fmeta:
      continue

    value = fmeta[parameter]
    if parameter == 'Datatype':
      value = datatypes.get(value.upper(), ("Unknown Datatype"))

    if parameter == 'Energy':
      value += " GeV"

    if parameter == 'CrossSection':
      value = str(value) + " fb"
      if 'CrossSectionError' in fmeta:
        value += "+/-" + str(fmeta["CrossSectionError"]) + "fb"
        del fmeta['CrossSectionError']

    info.append("    %s: %s" % (parameter.ljust(maxLength), value))
    del fmeta[parameter]

  if "AdditionalInfo" in fmeta:
    try:
      dinfo = DEncode.decode(fmeta["AdditionalInfo"])
    except Exception: ##cannot do anything else because decode raises base Exception #pylint: disable=W0703
      dinfo = eval(fmeta["AdditionalInfo"])
    info.append(" - There is some additional info:")
    if isinstance( dinfo, tuple ):
      dinfo = dinfo[0]
    if isinstance( dinfo, dict ):
      dictinfo = dinfo
      if 'xsection' in dictinfo and 'sum' in dictinfo['xsection'] and 'xsection' in dictinfo['xsection']['sum']:
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
    info.append('Remaining file metadata: %s' % str(fmeta))

  return info

def _getInfo():
  """gets info about transformation"""
  clip = _Params()
  clip.registerSwitches()
  Script.parseCommandLine()

  if not clip.prodid and not clip.filename:
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
    res1 = fc.findDirectoriesByMetadata({'ProdID':clip.prodid})
    if res1['OK'] and len(res1['Value'].values()):
      gLogger.verbose("Found %i directory matching the metadata" % len(res1['Value'].values()) )
      for dirs in res1['Value'].values():
        res = fc.getDirectoryUserMetadata(dirs)
        if res['OK']:
          fmeta.update(res['Value'])
        else:
          gLogger.error("Failed to get metadata for %s, SKIPPING" % dirs)
          continue
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
    info.extend(_createTransfoInfo(trans))

    if fmeta:
      info.append('The files created by this production have the following metadata:')
      info.extend(_createFileInfo(fmeta))
      info.append("It's possible that some meta data was not brought back,")
      info.append("in particular file level metadata, so check some individual files")

  if clip.filename:
    pid = ""
    if clip.filename.count("/"):
      fpath = os.path.dirname(clip.filename)
      res = fc.getDirectoryUserMetadata(fpath)
      if not res['OK']:
        gLogger.error(res['Message'])
        dexit(0)
      fmeta.update(res['Value'])
      res = fc.getFileUserMetadata(clip.filename)
      if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
      fmeta.update(res['Value'])
      if 'ProdID' in fmeta:
        pid = str(fmeta['ProdID'])
      res = fc.getFileAncestors([clip.filename], 1)
      if res["OK"]:
        for dummy_lfn,ancestorsDict in res['Value']['Successful'].items():
          if ancestorsDict.keys():
            fmeta["Ancestors"] = ancestorsDict.keys()
      res = fc.getFileDescendents([clip.filename], 1)
      if res["OK"]:
        for dummy_lfn,descendDict in res['Value']['Successful'].items():
          if descendDict.keys():
            fmeta['Descendants'] = descendDict.keys()
    else:
      ext = clip.filename.split(".")[-1]
      fitems = []
      for i in clip.filename.split('.')[:-1]:
        fitems.extend(i.split('_'))
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
      if not pid:
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
        fpath += clip.filename
        res = fc.getFileAncestors([fpath], 1)
        if res["OK"]:
          for dummy_lfn,ancestorsDict in res['Value']['Successful'].items():
            fmeta["Ancestors"] = ancestorsDict.keys()
        res = fc.getFileDescendents([fpath], 1)
        if res["OK"]:
          for dummy_lfn,descendDict in res['Value']['Successful'].items():
            fmeta['Descendants'] = descendDict.keys()

      res = fc.getDirectoryUserMetadata(dir_ex)
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
    info.extend(_createFileInfo(fmeta))
    info.append("")
    info.append('It was created with the production %s:' % pid)
    if trans:
      info.extend(_createTransfoInfo(trans))

  gLogger.notice("\n".join(info))

  dexit(0)

if __name__ == "__main__":
  _getInfo()
