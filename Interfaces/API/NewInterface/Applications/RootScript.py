"""
Root Script Application: use a script in the Root application framework

"""
__RCSID__ = "$Id"

from ILCDIRAC.Interfaces.API.NewInterface.Applications import _Root
from DIRAC import S_OK
import os,types

class RootScript(_Root):
  """ Run a script (root executable or shell) in the root application environment.

  Example:

  >>> rootsc = RootScript()
  >>> rootsc.setScript("myscript.exe")
  >>> rootsc.setArguments("some command line arguments")

  The ExtraCLIArguments is not used here, only use the Arguments
  """
  def __init__(self, paramdict = None):
    super(RootScript, self).__init__( paramdict )
    self._modulename = "RootExecutableAnalysis"
    self.appname = 'root'
    self._moduledescription = 'Root application script'


  def setScript(self, executable):
    """ Define executable to use

    @param executable: Script to run on. Can be shell or root executable. Must be a local file.
    @type executable: string
    """
    self._checkArgs( { 'executable' : types.StringTypes } )

    self.script = executable
    if os.path.exists(executable) or executable.lower().count("lfn:"):
      self.inputSB.append(executable)
    return S_OK()
