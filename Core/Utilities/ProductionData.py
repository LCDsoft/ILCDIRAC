""" 
Utility to construct production LFNs from workflow parameters
according to LHCb conventions.

@author: S. Poss
@since: Jun 16, 2010
"""

__RCSID__ = ""

import string, re, os, types, datetime

from ILCDIRAC.Core.Utilities.resolvePathsAndNames import getProdFilename
from ILCDIRAC.Core.Utilities.LFNPathUtilities import cleanUpLFNPath
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import S_OK, S_ERROR, gLogger

gLogger = gLogger.getSubLogger('ProductionData')
#############################################################################
def constructProductionLFNs(paramDict):
  """ Used for local testing of a workflow, a temporary measure until
      LFN construction is tidied.  This works using the workflow commons for
      on the fly construction.
  """
  result = checkForMandatoryKeys(paramDict, ['PRODUCTION_ID', 'JOB_ID', 'outputList'])
  if not result['OK']:
    return result
  
  productionID = paramDict['PRODUCTION_ID']
  jobID = paramDict['JOB_ID']
#  wfMode = paramDict['dataType']
  #wfLfnprefix=paramDict['lfnprefix']
  #wfLfnpostfix=paramDict['lfnpostfix']
  wfMask = ""
  # wfMask = paramDict['outputDataFileMask']
  if not type(wfMask) == type([]):
    wfMask = [i.lower().strip() for i in wfMask.split(';')]
  outputList = paramDict['outputList']

  res = getVOfromProxyGroup()
  #res = gConfig.getOption("/DIRAC/VirtualOrganization", "ilc")
  if not res['OK']:
    gLogger.error('Could not get VO from CS, assuming ilc')
    vo = 'ilc'
  else:
    vo = res['Value']
  fileTupleList = []
  #gLogger.verbose('wfLfnprefix = %s, wfLfnpostfix = %s, wfMask = %s, wfType=%s' %(wfLfnprefix,wfLfnpostfix,wfMask,wfType))
  gLogger.verbose('outputList %s' % (outputList))
  for info in outputList:
    #Nasty check on whether the created code parameters were not updated e.g. when changing defaults in a workflow
    fileName = info['outputFile']
    #rename to take care of correct path
    fileName = getProdFilename(fileName, int(productionID), int(jobID))
    #index=0
    #if not re.search('^\d',fileName[index]):
    #  index+=1
    #if not fileName[index]==str(productionID).zfill(8):
    #  fileName[index]=str(productionID).zfill(8)
    #if not fileName[index+1]==str(jobID).zfill(8):
    #  fileName[index+1]=str(jobID).zfill(8)
    fileTupleList.append((info['outputPath'], fileName))

  debugRoot = ''
  #if inputData:
  #  gLogger.verbose('Making lfnRootPath for job with inputdata: %s' %(inputData))
  #  lfnRoot = _getLFNRoot(inputData,wfLfnpostfix)
  #  debugRoot= _getLFNRoot('','debug',wfLfnpostfix)   
  #else:
  #  lfnRoot = _getLFNRoot('',wfLfnprefix,wfLfnpostfix)
  #  gLogger.verbose('lfnRootPath is: %s' %(lfnRoot))
  #  debugRoot= _getLFNRoot('','debug',wfLfnpostfix)
  #lfnRoot = 
  #gLogger.verbose('lfnRootPath is: %s' %(lfnRoot))
  #if not lfnRoot:
  #  return S_ERROR('LFN root could not be constructed')

  #Get all LFN(s) to both output data and BK lists at this point (fine for BK)
  outputData = []
  #bkLFNs = []
  debugLFNs = []
  for fileTuple in fileTupleList:
    #lfn = _makeProductionLfn(str(jobID).zfill(8),lfnRoot,fileTuple,wfLfnprefix,str(productionID).zfill(8))
    lfn = fileTuple[0] + "/" + str(productionID).zfill(8) + "/" + str(int(jobID)/1000).zfill(3) + "/" + fileTuple[1]
    lfn = cleanUpLFNPath(lfn)
    outputData.append(lfn)
    #bkLFNs.append(lfn)
    if debugRoot:
      #debugLFNs.append(_makeProductionLfn(str(jobID).zfill(8),debugRoot,fileTuple,wfLfnprefix,str(productionID).zfill(8)))
      debugLFNs.append("/" + vo + "/prod/debug/" + str(productionID).zfill(8))
  #if debugRoot:
  # debugLFNs.append(_makeProductionLfn(str(jobID).zfill(8),debugRoot,('%s_core' % str(jobID).zfill(8) ,'core'),wfLfnprefix,str(productionID).zfill(8)))

  #Get log file path - unique for all modules
  #logPath = _makeProductionPath(str(jobID).zfill(8),lfnRoot,'LOG',wfLfnprefix,str(productionID).zfill(8),log=True)
  logPathtemp = fileTupleList[0][0]
  #logPathroot = string.join(logPathtemp[0:len(logPathtemp)-1], "/")
  #TODO adjust for ILD
  logPath = logPathtemp + "/" + str(productionID).zfill(8) + "/LOG"
  logFilePath = [cleanUpLFNPath('%s/%s' % (logPath, str(int(jobID)/1000).zfill(3)))]
  logTargetPath = [cleanUpLFNPath('%s/%s_%s.tar' % (logPath, str(productionID).zfill(8), str(int(jobID)).zfill(3)))]
  #[ aside, why does makeProductionPath not append the jobID itself ????
  #  this is really only used in one place since the logTargetPath is just written to a text file (should be reviewed)... ]

  #Strip output data according to file mask
  if wfMask:
    newOutputData = []
    #newBKLFNs = []
    for od in outputData:
      for i in wfMask:
        if re.search('.%s$' % i, od):
          if not od in newOutputData:
            newOutputData.append(od)
            
    #for bk in bkLFNs:
    #  newBKLFNs.append(bk)
    outputData = newOutputData
    #bkLFNs = newBKLFNs

  if not outputData:
    gLogger.info('No output data LFN(s) constructed')
  else:
    gLogger.verbose('Created the following output data LFN(s):\n%s' % (string.join(outputData,'\n')))
  gLogger.verbose('Log file path is:\n%s' % logFilePath[0])
  gLogger.verbose('Log target path is:\n%s' % logTargetPath[0])
  #if bkLFNs:
  #  gLogger.verbose('BookkeepingLFN(s) are:\n%s' %(string.join(bkLFNs,'\n')))
  if debugLFNs:
    gLogger.verbose('DebugLFN(s) are:\n%s' % (string.join(debugLFNs, '\n')))
  jobOutputs = {'ProductionOutputData' : outputData, 'LogFilePath' : logFilePath,
                'LogTargetPath' : logTargetPath, 'DebugLFNs' : debugLFNs}
  return S_OK(jobOutputs)

#############################################################################
def getLogPath(paramDict):
  """ Can construct log file paths even if job fails e.g. no output files available.
  """
  result = checkForMandatoryKeys(paramDict, ['PRODUCTION_ID', 'JOB_ID', 'LogFilePath'])
  if not result['OK']:
    return result

  productionID = paramDict['PRODUCTION_ID']
  jobID = paramDict['JOB_ID']
  #need to built logPath from logFilePath, as it's not there, and must be as in method above
  logPathtemp = paramDict['LogFilePath'].split("/")
  logPath = os.path.join(*logPathtemp[0:-1])
  logFilePath = paramDict['LogFilePath']
  logTargetPath = ['%s/%s_%s.tar' % (logPath, str(productionID).zfill(8), str(int(jobID)/1000).zfill(3))]
  #Get log file path - unique for all modules

  gLogger.verbose('Log file path is:\n%s' % logFilePath)
  gLogger.verbose('Log target path is:\n%s' % logTargetPath)
  jobOutputs = {'LogFilePath' : logFilePath, 'LogTargetPath' : logTargetPath}
  return S_OK(jobOutputs)

#############################################################################
def constructUserLFNs(jobID, vo, owner, outputFiles, outputPath):
  """ This method is used to supplant the standard job wrapper output data policy
      for ILC.  The initial convention adopted for user output files is the following:
      If outputpath is not defined:
      <vo>/user/<initial e.g. s>/<owner e.g. sposs>/<yearMonth e.g. 2010_02>/<subdir>/<fileName>
      Otherwise:
      <vo>/user/<initial e.g. s>/<owner e.g. sposs>/<outputPath>/<fileName>
  """
  initial = owner[:1]
  subdir = str(jobID/1000)  
  timeTup = datetime.date.today().timetuple() 
  yearMonth = '%s_%s' % (timeTup[0], string.zfill(str(timeTup[1]), 2))
  outputLFNs = {}
  if not vo:
    #res = gConfig.getOption("/DIRAC/VirtualOrganization", "ilc")
    res = getVOfromProxyGroup()
    if not res['OK']:
      gLogger.error('Could not get VO from CS, assuming ilc')
      vo = 'ilc'
    else:
      vo = res['Value']
  ops = Operations(vo = vo)
  lfn_prefix = ops.getValue("LFNUserPrefix", "user")
      
  #Strip out any leading or trailing slashes but allow fine structure
  if outputPath:
    outputPathList = string.split(outputPath, os.sep)
    newPath = []
    for i in outputPathList:
      if i:
        newPath.append(i)
    outputPath = string.join(newPath, os.sep)
  
  if not type(outputFiles) == types.ListType:
    outputFiles = [outputFiles]
    
  for outputFile in outputFiles:
    #strip out any fine structure in the output file specified by the user, restrict to output file names
    #the output path field can be used to describe this    
    outputFile = outputFile.replace('LFN:', '')
    lfn = ''
    if outputPath:
      lfn = os.sep+os.path.join(vo, lfn_prefix, initial, owner, outputPath + os.sep + os.path.basename(outputFile))
    else:
      lfn = os.sep+os.path.join(vo, lfn_prefix, initial, owner, yearMonth, subdir, str(jobID)) + os.sep + os.path.basename(outputFile)
    outputLFNs[outputFile] = lfn
  
  outputData = outputLFNs.values()
  if outputData:
    gLogger.info('Created the following output data LFN(s):\n%s' % (string.join(outputData, '\n')))
  else:
    gLogger.info('No output LFN(s) constructed')
    
  return S_OK(outputData)

#############################################################################
# def _makeProductionPath(JOB_ID, LFN_ROOT, typeName, mode, prodstring, log = False):
#   """ Constructs the path in the logical name space where the output
#       data for the given production will go. In
#   """
#   result = LFN_ROOT + '/' + typeName.upper() + '/' + prodstring + '/'
#   if log:
#     try:
#       jobid = int(JOB_ID)
#       jobindex = string.zfill(jobid/10000, 4)
#     except:
#       jobindex = '0000'
#     result += jobindex
#   return result

#############################################################################
# def _makeProductionLfn(JOB_ID, LFN_ROOT, filetuple, mode, prodstring):
#   """ Constructs the logical file name according to LHCb conventions.
#       Returns the lfn without 'lfn:' prepended.
#   """
#   gLogger.debug('Making production LFN for JOB_ID %s, LFN_ROOT %s, mode %s, prodstring %s for\n%s' %(JOB_ID, LFN_ROOT, mode, prodstring, str(filetuple)))
#   try:
#     jobid = int(JOB_ID)
#     jobindex = string.zfill(jobid/10000, 4)
#   except:
#     jobindex = '0000'
#   fname = filetuple[0]
#   if re.search('lfn:', fname) or re.search('LFN:', fname):
#     return fname.replace('lfn:', '').replace('LFN:', '')
#   return LFN_ROOT + '/' + filetuple[1].upper() + '/' + prodstring + '/' + jobindex + '/' + filetuple[0]
      
# #############################################################################
# def _getLFNRoot(lfn, lfnprefix='', lfnpostfix=0):
#   """
#   return the root path of a given lfn
#   eg : /ilc/data/CCRC08/00009909 = getLFNRoot(/lhcb/data/CCRC08/00009909/DST/0000/00009909_00003456_2.dst)
#   eg : /ilc/prod/<year>/  = getLFNRoot(None)
#   """
#   #dataTypes = gConfig.getValue('/Operations/Bookkeeping/FileTypes',[])
#   #gLogger.verbose('DataTypes retrieved from /Operations/Bookkeeping/FileTypes are:\n%s' %(string.join(dataTypes,', ')))
#   LFN_ROOT = ''  
#   gLogger.verbose('wf lfn: %s, prefix: %s, postfix: %s' % (lfn, lfnprefix, lfnpostfix))
#   if not lfn:
#     LFN_ROOT = '/ilc/%s/%s' % (lfnprefix, lfnpostfix)
#     gLogger.verbose('LFN_ROOT will be %s' % (LFN_ROOT))
#     return LFN_ROOT
#   lfn = [fname.replace(' ', '').replace('LFN:', '') for fname in lfn.split(';')]
#   lfnroot = lfn[0].split('/')
#   for part in lfnroot:
#   #  if not part in dataTypes:
#     LFN_ROOT += '/%s' % (part)  
#   #  else:
#   #    break
#   LFN_ROOT = cleanUpLFN(LFN_ROOT)
#   if lfnprefix.lower() in ('test', 'debug'):
#     tmpLfnRoot = LFN_ROOT.split("/")
#     if len(tmpLfnRoot) > 2:
#       tmpLfnRoot[2] = lfnprefix
#     else:
#       tmpLfnRoot[-1] = lfnprefix
#     LFN_ROOT = string.join(tmpLfnRoot, os.path.sep)
#   return LFN_ROOT



def checkForMandatoryKeys(paramDict, keys):
  """checks for Mandatory Keys in the paramdict"""
  for k in keys:
    if not k in paramDict:
      return S_ERROR('%s not defined' % k)
  return S_OK

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
