'''
@since: May 30, 2013

@author: sposs
'''
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import SharedArea
from ILCDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from DIRAC.Core.Utilities.Os import getDiskSpace, getDirectorySize

from DIRAC import S_OK, S_ERROR, gLogger
import os, socket

class AnalyseWN(ModuleBase):
  '''
  Module that will dump the host properties, and also the SharedArea content if it's there.
  '''


  def __init__(self):
    '''
    Constructor
    '''
    super(AnalyseWN, self).__init__()
    self.log = gLogger.getSubLogger('AnalyseWN')
    
  def execute(self):
    """ Run the module
    """
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error("Failed to get the input parameters:", result['Message'])
      return result

    if not self.applicationLog:
      self.log.warn("Log file name missing, reverting to default")
      self.applicationLog = "AnalyseWN.log"

    info = []
    try:
      info.append("Host is %s" % socket.gethostname())
    except:
      info.append("Could not determine host")
      
    size = getDiskSpace()
    if size>0:
      info.append("Local disk is %s MB"% size)
      
    fileName = '/proc/cpuinfo'
    if os.path.exists( fileName ):
      f = open( fileName, 'r' )
      cpu = f.readlines()
      f.close()
      nCPU = 0
      for line in cpu:
        if line.find( 'cpu MHz' ) == 0:
          nCPU += 1
          freq = line.split()[3]
        elif line.find( 'model name' ) == 0:
          CPUmodel = line.split( ': ' )[1].strip()
      info.append('CPU (model)    = %s' % CPUmodel)
      info.append('CPU (MHz)      = %s x %s' % ( nCPU, freq ))
      
    fileName = '/proc/meminfo'
    if os.path.exists( fileName ):
      f = open( fileName, 'r' )
      mem = f.readlines()
      f.close()
      freeMem = 0
      for line in mem:
        if line.find( 'MemTotal:' ) == 0:
          totalMem = int( line.split()[1] )
        if line.find( 'MemFree:' ) == 0:
          freeMem += int( line.split()[1] )
        if line.find( 'Cached:' ) == 0:
          freeMem += int( line.split()[1] )
      info.append( 'Memory (kB)    = %s' % totalMem )
      info.append( 'FreeMem. (kB)  = %s' % freeMem )
      
    fs = os.statvfs( "." )
    # bsize;    /* file system block size */
    # frsize;   /* fragment size */
    # blocks;   /* size of fs in f_frsize units */
    # bfree;    /* # free blocks */
    # bavail;   /* # free blocks for non-root */
    # files;    /* # inodes */
    # ffree;    /* # free inodes */
    # favail;   /* # free inodes for non-root */
    # flag;     /* mount flags */
    # namemax;  /* maximum filename length */
    diskSpace = fs[4] * fs[0] / 1024 / 1024
    info.append( 'DiskSpace (MB) = %s' % diskSpace )
      
    sha = SharedArea()    
    if not sha:
      info.append("No shared Area found here")
    else:
      info.append("Shared Area found: %s" % sha)
      info.append("Content:")
      sha_list = os.listdir(sha)
      for item in sha_list:
        info.append("   %s"% item)
      sha_size = getDirectorySize(sha)
      if sha_size:
        info.append("It uses %s MB of disk"% sha_size)
    
    
    if (os.path.isdir("/cvmfs/ilc.cern.ch")):
      info.append("Has CVMFS")
    
    try:
      of = open(self.applicationLog, "w")
      of.write("\n".join(info))
      of.close()
    except:
      self.log.error("Could not create the log file")
      return S_ERROR("Failed saving the site info")
    
    return S_OK()
  