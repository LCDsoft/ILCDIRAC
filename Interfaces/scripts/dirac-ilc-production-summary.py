"""
Prepare the production summary tables
"""
from DIRAC.Core.Base import Script

import os,string
from DIRAC.TransformationSystem.Client.TransformationClient         import TransformationClient

from ILCDIRAC.Core.Utilities.ProcessList import ProcessList

from ILCDIRAC.Core.Utilities.HTML import *
from DIRAC import gConfig, S_OK, S_ERROR, exit as dexit

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC.Core.Utilities import DEncode


def getFileInfo(lfn):
  """ Retrieve the file info
  """
  fc = FileCatalogClient()
  lumi = 0
  nbevts = 0
  res  = fc.getFileUserMetadata(lfn)
  if not res['OK']:
    print "Failed to get metadata of %s"%lfn
  if res['Value'].has_key('Luminosity'):   
    lumi += float(res['Value']['Luminosity'])
  addinfo = None
  if 'AdditionalInfo' in res['Value']:
    addinfo = res['Value']['AdditionalInfo']
    if addinfo.count("{"):
      addinfo = eval(addinfo)
    else:
      addinfo = DEncode.decode(addinfo)[0]
  if "NumberOfEvents" in res['Value'].keys():
    nbevts += int(res['Value']['NumberOfEvents'])
  return (float(lumi),int(nbevts),addinfo)

def translate(detail):
  """ Replace whizard naming convention by human conventions
  """
  detail = detail.replace('v','n1:n2:n3:N1:N2:N3')
  detail = detail.replace('e1','e-')
  detail = detail.replace('E1','e+')
  detail = detail.replace('e2','mu-')
  detail = detail.replace('E2','mu+')
  detail = detail.replace('e3','tau-')
  detail = detail.replace('E3','tau+')
  detail = detail.replace('n1','nue')
  detail = detail.replace('N1','nueb')
  detail = detail.replace('n2','numu')
  detail = detail.replace('N2','numub')
  detail = detail.replace('n3','nutau')
  detail = detail.replace('N3','nutaub')
  detail = detail.replace('U','ubar')
  detail = detail.replace('C','cbar')
  detail = detail.replace('T','tbar')
  detail = detail.replace('tbareV','TeV')
  detail = detail.replace('D','dbar')
  detail = detail.replace('S','sbar')
  detail = detail.replace('B','bbar')
  detail = detail.replace('Z0','Z')
  detail = detail.replace('Z','Z0')
  #detail = detail.replace('ql','u:d:s:c:b:U:D:S:C:B')
  detail = detail.replace('gghad','gamma gamma -> hadrons')
  detail = detail.replace(',','')
  detail = detail.replace('n N','nu nub')
  detail = detail.replace('se--','seL-')
  detail = detail.replace('se-+','seL+')
  detail = detail.replace(' -> ','->')
  detail = detail.replace('->',' -> ')
  detail = detail.replace(' H ->',', H ->')
  detail = detail.replace(' Z0 ->',', Z0 ->')
  detail = detail.replace(' W ->',', W ->')
  
  return detail

class Params(object):
  """ CLI Parameters class
  """
  def __init__(self):
    """ Initialize
    """
    self.prod = []
    self.minprod = 0
    self.full_det = False
    self.verbose = False
    self.ptypes = ['MCGeneration','MCSimulation','MCReconstruction',"MCReconstruction_Overlay"]
    self.statuses = ['Active','Stopped','Completed','Archived']
    
  def setProdID(self, opt):
    """ Set the prodID to use. can be a range, a list, a unique value
    and a 'greater than' value
    """
    if opt.count("gt"):
      self.minprod = int(opt.replace("gt",""))
    elif opt.count("-"):
      self.prod = range(int(opt.split("-")[0]), int(opt.split("-")[1])+1)
    elif opt.count(","):
      self.prod = [int(p) for p in opt.split(",")]
    else:
      self.prod = int(opt)
    return S_OK()

  def setFullDetail(self,opt):
    """ Get every individual file's properties, makes this 
    very very slow
    """
    self.full_det = True
    return S_OK()

  def setVerbose(self, opt):
    """ Extra printouts
    """
    self.verbose = True
    return S_OK()

  def setProdTypes(self, opt):
    """ The prod types to consider
    """
    self.ptypes = opt.split(",")
    return S_OK()

  def setStatuses(self, opt):
    ''' The prod statuses
    '''
    self.statuses = opt.split(",")
    return S_OK()

  def registerSwitch(self):
    """ Register all CLI switches
    """
    Script.registerSwitch("P:", "prods=", "Productions: greater than with gt1234, range with 32-56, list with 34,56", self.setProdID)
    Script.registerSwitch("p", "precise_detail", "Precise detail, slow", self.setFullDetail)
    Script.registerSwitch("v", "verbose", "Verbose output", self.setVerbose)
    Script.registerSwitch("t:", "types=", "Production Types, comma separated, default all", self.setProdTypes)
    Script.registerSwitch("S:", "Statuses=", "Statuses, comma separated, default all", self.setStatuses)
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

if __name__=="__main__":
  clip = Params()
  clip.registerSwitch()
  Script.parseCommandLine()
  prod = clip.prod
  full_detail = clip.full_det
  fc = FileCatalogClient()
  
  processlist = gConfig.getValue('/LocalSite/ProcessListPath')
  prl = ProcessList(processlist)
  processesdict = prl.getProcessesDict()
  
  trc = TransformationClient()
  prodids = []
  if not prod:
   conddict = {}
   conddict['Status'] = clip.statuses
   if clip.ptypes:
     conddict['Type'] = clip.ptypes
   res = trc.getTransformations( conddict )
   if res['OK']:
     for transfs in res['Value']:
       prodids.append(transfs['TransformationID'])
  else:
    prodids.extend(prod)

  metadata = []
  
  for prodID in prodids:
    if prodID<clip.minprod:
      continue
    meta = {}
    meta['ProdID']=prodID
    res = trc.getTransformation(str(prodID))
    if not res['OK']:
      print "Error getting transformation %s" % prodID 
      continue
    prodtype = res['Value']['Type']
    proddetail = res['Value']['Description']
    if prodtype == 'MCReconstruction' or prodtype == 'MCReconstruction_Overlay' :
      meta['Datatype']='DST'
    if prodtype == 'MCGeneration':
      meta['Datatype']='gen'
    if prodtype == 'MCSimulation':
      meta['Datatype']='SIM'
    res = fc.findFilesByMetadata(meta)  
    if not res['OK']:
      print res['Message']
      continue
    lfns = res['Value']
    nb_files = len(lfns)
    path = ""
    if not len(lfns):
     if clip.verbose:
       print "No files found for prod %s" % prodID
     continue
    path = os.path.dirname(lfns[0])
    res = fc.getDirectoryMetadata(path)
    if not res['OK']:
      if clip.verbose:
        print 'No meta data found for %s' % path
      continue
    dirmeta = {}
    dirmeta['proddetail'] = proddetail    
    dirmeta['nb_files']=nb_files
    dirmeta.update(res['Value'])
    lumi  = 0.
    nbevts = 0
    addinfo = None
    if not full_detail:
      lfn  = lfns[0]
      res = getFileInfo(lfn)
      nbevts = res[1]*len(lfns)
      lumi = res[0]*len(lfns)
      addinfo = res[2]
    else:
      for lfn in lfns:
        res = getFileInfo(lfn)
        lumi += res[0]
        nbevts += res[1]
        addinfo = res[2]
        
    if nbevts:
      dirmeta['NumberOfEvents']=nbevts
    if not lumi:
      dirmeta['Luminosity']=0
      dirmeta['CrossSection']=0
    else:
      if nbevts:
        dirmeta['CrossSection']=nbevts/lumi
      else:
        dirmeta['CrossSection']=0
    if addinfo:
      if 'xsection' in addinfo:
        if 'xsection' in addinfo['xsection']:
          dirmeta['CrossSection']=addinfo['xsection']['xsection']
                
    if not dirmeta.has_key('NumberOfEvents'):
      dirmeta['NumberOfEvents']=0
    #print processesdict[dirmeta['EvtType']]
    dirmeta['detail']=''
    if processesdict.has_key(dirmeta['EvtType']):
      if processesdict[dirmeta['EvtType']].has_key('Detail'):
        detail = processesdict[dirmeta['EvtType']]['Detail']
        
    else:
      detail=dirmeta['EvtType']
  
  
    if not prodtype == 'MCGeneration':
      res = trc.getTransformationInputDataQuery(str(prodID))
      if res['OK']:
        if res['Value'].has_key('ProdID'):
          dirmeta['MomProdID']=res['Value']['ProdID']
    if not dirmeta.has_key('MomProdID'):
      dirmeta['MomProdID']=0
    dirmeta['detail']= translate(detail)
    metadata.append(dirmeta)
  
  detectors = {}
  detectors['ILD'] = {}
  corres = {"MCGeneration":'gen',"MCSimulation":'SIM',"MCReconstruction":"REC","MCReconstruction_Overlay":"REC"}
  detectors['ILD']['SIM'] = []
  detectors['ILD']['REC'] = []
  detectors['SID'] = {}
  detectors['SID']['SIM'] = []
  detectors['SID']['REC'] = []
  detectors['sid'] = {}
  detectors['sid']['SIM'] = []
  detectors['sid']['REC'] = []
  detectors['gen']=[]
  for channel in metadata:
    if not channel.has_key('DetectorType'):
      detectors['gen'].append((channel['detail'],
                               channel['Energy'],
                               channel['ProdID'],
                               channel['nb_files'],
                               channel['NumberOfEvents']/channel['nb_files'],
                               channel['NumberOfEvents'],
                               channel['CrossSection'],str(channel['proddetail'])))
    else:
      if not channel['DetectorType'] in detectors:
        print "This is unknown detector", channel['DetectorType']
        continue
      detectors[channel['DetectorType']][corres[prodtype]].append((channel['detail'],
                                                                   channel['Energy'],
                                                                   channel['DetectorType'],
                                                                   channel['ProdID'],
                                                                   channel['nb_files'],
                                                                   channel['NumberOfEvents']/channel['nb_files'],
                                                                   channel['NumberOfEvents'],
                                                                   channel['CrossSection'],
                                                                   channel['MomProdID'],
                                                                   str(channel['proddetail'])))
  
  of = file("tables.html","w")
  of.write("""<!DOCTYPE html>
<html>
 <head>
<title> Production summary </title>
</head>
<body>
""")
  if len(detectors['gen']):           
    of.write("<h1>gen prods</h1>\n")
    t = Table(header_row = ('Channel', 'Energy','ProdID','Tasks','Average Evts/task','Statistics','Cross Section (fb)','Comment'))
    for item in detectors['gen']:
      t.rows.append( item )
    of.write(str(t))
    if clip.verbose:
      print "Gen prods"
      print str(t)

  if len(detectors['ILD']):           
    of.write("<h1>ILD prods</h1>\n")
    for ptype in detectors['ILD'].keys():
      if len(detectors['ILD'][ptype]):
        of.write("<h2>%s</h2>\n"%ptype)
        t = Table(header_row = ('Channel', 'Energy','Detector','ProdID','Number of Files','Events/File','Statistics','Cross Section (fb)','Origin ProdID','Comment'))
        for item in detectors['ILD'][ptype]:
          t.rows.append( item )
        of.write(str(t))
        if clip.verbose:
          print "ILC CDR prods %s"%ptype
          print str(t)
  
  if len(detectors['SID']):           
    of.write("<h1>SID prods</h1>\n")
    for ptype in detectors['SID'].keys():
      if len(detectors['SID'][ptype]):
        of.write("<h2>%s</h2>\n"%ptype)
        t = Table(header_row = ('Channel', 'Energy','Detector','ProdID','Number of Files','Events/File','Statistics','Cross Section (fb)','Origin ProdID','Comment'))
        for item in detectors['SID'][ptype]:
          t.rows.append( item )
        of.write(str(t))
        if clip.verbose:
          print "SID CDR prods %s"%ptype
          print str(t)

  if len(detectors['sid']):           
    of.write("<h1>sid dbd prods</h1>\n")
    for ptype in detectors['SID'].keys():
      if len(detectors['sid'][ptype]):
        of.write("<h2>%s</h2>\n"%ptype)
        t = Table(header_row = ('Channel', 'Energy','Detector','ProdID','Number of Files','Events/File','Statistics','Cross Section (fb)','Origin ProdID','Comment'))
        for item in detectors['sid'][ptype]:
          t.rows.append( item )
        of.write(str(t))
        if clip.verbose:
          print "sid DBD prods %s"%ptype
          print str(t)
  
  of.write("""
</body>
</html>
""")
  of.close()
  print "Check ./tables.html in any browser for the results"
  dexit(0)
