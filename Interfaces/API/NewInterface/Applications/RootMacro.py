"""
Root Macro Application: use a macro in the Root application framework
"""
__RCSID__ = "$Id"

from ILCDIRAC.Interfaces.API.NewInterface.Applications import _Root
from DIRAC import S_OK
import os,types

class RootMacro(_Root):
  """ Run a root macro in the root application environment.

  Example:

  >>> rootmac = RootMacro()
  >>> rootmac.setMacro("mymacro.C")
  >>> rootmac.setArguments("some command line arguments")

  The setExtraCLIArguments is not available here, use the Arguments
  """
  def __init__(self, paramdict = None):
    super(RootMacro, self).__init__( paramdict )
    self._modulename = "RootMacroAnalysis"
    self.appname = 'root'
    self._moduledescription = 'Root macro execution'


  def setMacro(self, macro):
    """ Define macro to use

    :param string macro: Macro to run on. Must be a local C file.
    """
    self._checkArgs( { 'macro' : types.StringTypes } )

    self.script = macro
    if os.path.exists(macro) or macro.lower().count("lfn:"):
      self.inputSB.append(macro)
    return S_OK()
