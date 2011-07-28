'''
Created on Jul 28, 2011

@author: Stephane Poss
'''

from ILCDIRAC.Interfaces.API.NewInterface.Job import Job
from DIRAC import S_OK,S_ERROR

class UserJob(Job):
  def __init__(self):
    Job.__init__(self)
    
  def setInputData(self):
    return S_OK() 

  def setInputSandbox(self,flist):
    self.inputsandbox = flist
    return S_OK()

  def setOutputData(self):
    return S_OK() 

  def setOutputSandbox(self):
    return S_OK()
  
  def _jobSpecificParams(self):
    return S_OK()