"""
User Job class. Used to define user jobs!

Example usage:

>>> from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
>>> from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
>>> myDiracInstance = DiracILC( withRepo=False )
>>> myJob = UserJob()
>>> ...
>>> myJob.append( myMarlinApp )
>>> myJob.submit(myDiracInstance)

:author: Stephane Poss
:author: Remi Ete
:author: Ching Bon Lam
"""

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo

from DIRAC import S_OK

import types

__RCSID__ = "$Id$"

class UserJob(Job):
  """ User job class. To be used by users, not for production.
  """
  def __init__(self, script = None):
    super(UserJob, self).__init__( script )
    self.type = 'User'
    self.diracinstance = None
    self.usergroup = ['ilc_user', 'calice_user']
    self.proxyinfo = getProxyInfo()
    
  def submit(self, diracinstance = None, mode = "wms"):
    """ Submit call: when your job is defined, and all applications are set, you need to call this to
    add the job to DIRAC.

    :param diracinstance: DiracILC instance
    :type diracinstance: ~ILCDIRAC.Interfaces.API.DiracILC.DiracILC
    :param str mode: "wms" (default), "agent", or "local"

    .. note ::
      The *local* mode means that the job will be run on the submission machine. Use this mode for testing of submission scripts

    """
    #Check the credentials. If no proxy or not user proxy, return an error
    if not self.proxyinfo['OK']:
      self.log.error("Not allowed to submit a job, you need a %s proxy." % self.usergroup)
      return self._reportError("Not allowed to submit a job, you need a %s proxy." % self.usergroup,
                               self.__class__.__name__)
    if self.proxyinfo['Value'].has_key('group'):
      group = self.proxyinfo['Value']['group']
      if not group in self.usergroup:
        self.log.error("Not allowed to submit a job, you need a %s proxy." % self.usergroup)
        return self._reportError("Not allowed to submit job, you need a %s proxy." % self.usergroup,
                                 self.__class__.__name__)
    else:
      self.log.error("Could not determine group, you do not have the right proxy.")       
      return self._reportError("Could not determine group, you do not have the right proxy.")

    res = self._addToWorkflow()
    if not res['OK']:
      return res
    self.oktosubmit = True
    if not diracinstance:
      self.diracinstance = DiracILC()
    else:
      self.diracinstance = diracinstance
    return self.diracinstance.submit(self, mode)
    
  #############################################################################
  def setInputData( self, lfns ):
    """Specify input data by Logical File Name (LFN).

    Input files specified via this function will be automatically staged if necessary.

    Example usage:

    >>> job = UserJob()
    >>> job.setInputData(['/ilc/prod/whizard/processlist.whiz'])

    :param lfns: Logical File Names
    :type lfns: Single LFN string or list of LFNs
    """
    if type( lfns ) == list and len( lfns ):
      for i in xrange( len( lfns ) ):
        lfns[i] = lfns[i].replace( 'LFN:', '' )
      #inputData = map( lambda x: 'LFN:' + x, lfns )
      inputData = lfns #because we don't need the LFN: for inputData, and it breaks the 
      #resolution of the metadata in the InputFilesUtilities
      inputDataStr = ';'.join( inputData )
      description = 'List of input data specified by LFNs'
      self._addParameter( self.workflow, 'InputData', 'JDL', inputDataStr, description )
    elif type( lfns ) == type( ' ' ):  #single LFN
      description = 'Input data specified by LFN'
      self._addParameter( self.workflow, 'InputData', 'JDL', lfns, description )
    else:
      kwargs = {'lfns':lfns}
      return self._reportError( 'Expected lfn string or list of lfns for input data', **kwargs )

    return S_OK()
   
  def setInputSandbox(self, flist):
    """ Add files to the input sandbox, can be on the local machine or on the grid

    >>> job = UserJob()
    >>> job.setInputSandbox( ['LFN:/ilc/user/u/username/libraries.tar.gz',
    >>>                       'mySteeringFile.xml'] )

    :param flist: Files for the inputsandbox
    :type flist: `python:list` or `str`
    """
    if type(flist) == type(""):
      flist = [flist]
    if not type(flist) == type([]) :
      return self._reportError("File passed must be either single file or list of files.") 
    self.inputsandbox.extend(flist)
    return S_OK()

  #############################################################################
  def setOutputData(self, lfns, OutputPath = '', OutputSE = ''):
    """For specifying output data to be registered in Grid storage.  If a list
    of OutputSEs are specified the job wrapper will try each in turn until
    successful.

    Example usage:

    >>> job = UserJob()
    >>> job.setOutputData(['Ntuple.root'])

    :param lfns: Output data file or files
    :type lfns: Single `str` or `python:list` of strings ['','']
    :param str OutputPath: Optional parameter to specify the Path in the Storage, postpended to /ilc/user/u/username/
    :param OutputSE: Optional parameter to specify the Storage Element to store data or files, e.g. CERN-SRM
    :type OutputSE: `python:list` or `str`
    """
    kwargs = {'lfns' : lfns, 'OutputSE' : OutputSE, 'OutputPath' : OutputPath}
    if type(lfns) == list and len(lfns):
      outputDataStr = ';'.join(lfns)
      description = 'List of output data files'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', outputDataStr, description)
    elif type(lfns) == type(" "):
      description = 'Output data file'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', lfns, description)
    else:
      return self._reportError('Expected file name string or list of file names for output data', **kwargs)

    if OutputSE:
      description = 'User specified Output SE'
      if type(OutputSE) in types.StringTypes:
        OutputSE = [OutputSE]
      elif type(OutputSE) != types.ListType:
        return self._reportError('Expected string or list for OutputSE', **kwargs)
      OutputSE = ';'.join(OutputSE)
      self._addParameter(self.workflow, 'UserOutputSE', 'JDL', OutputSE, description)

    if OutputPath:
      description = 'User specified Output Path'
      if not type(OutputPath) in types.StringTypes:
        return self._reportError('Expected string for OutputPath', **kwargs)
      # Remove leading "/" that might cause problems with os.path.join
      while OutputPath[0] == '/': 
        OutputPath = OutputPath[1:]
      if OutputPath.count("ilc/user"):
        return self._reportError('Output path contains /ilc/user/ which is not what you want', **kwargs)
      self._addParameter(self.workflow, 'UserOutputPath', 'JDL', OutputPath, description)

    return S_OK()
  
  #############################################################################
  def setOutputSandbox( self, files ):
    """Specify output sandbox files.  If specified files are over 10MB, these
    may be uploaded to Grid storage with a notification returned in the
    output sandbox.

    .. Note ::
       Sandbox files are removed after 2 weeks.

    Example usage:

    >>> job = UserJob()
    >>> job.setOutputSandbox(['*.log','*.sh', 'myfile.txt'])

    Use the output sandbox only for small files. Larger files should be stored
    on the grid and downloaded later if necessary. See :func:`setOutputData`

    :param files: Output sandbox files
    :type files: Single `str` or `python:list` of strings ['','']

    """
    if type( files ) == list and len( files ):
      fileList = ";".join( files )
      description = 'Output sandbox file list'
      self._addParameter( self.workflow, 'OutputSandbox', 'JDL', fileList, description )
    elif type( files ) == type( " " ):
      description = 'Output sandbox file'
      self._addParameter( self.workflow, 'OutputSandbox', 'JDL', files, description )
    else:
      kwargs = {'files' : files}
      return self._reportError( 'Expected file string or list of files for output sandbox contents', **kwargs )

    return S_OK()
    
  def setILDConfig(self,version):
    """ Define the Configuration package to obtain
    """
    appName = 'ILDConfig'
    self._addSoftware(appName.lower(), version)
    
    self._addParameter( self.workflow, 'ILDConfigPackage', 'JDL', appName+version, 'ILDConfig package' )
    return S_OK()
  
  