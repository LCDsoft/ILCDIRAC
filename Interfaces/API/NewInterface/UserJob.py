'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from DIRAC import S_OK,S_ERROR

class UserJob(Job):
  def __init__(self, script = None):
    Job.__init__(self, script)
    
  def setInputData(self):
    """ Inherited from DIRAC.Job
    """
    return S_OK() 

  def setInputSandbox(self,flist):
    """ Mostly inherited from DIRAC.Job
    """
    self.inputsandbox = flist
    return S_OK()

  def setOutputData(self):
    """ Inherited from DIRAC.Job
    """
    return S_OK() 

  def setOutputSandbox(self):
    """ Inherited from DIRAC.Job
    """
    return S_OK()
  
  def _jobSpecificParams(self):
    """ Inherited from DIRAC.Job
    """
    return S_OK()