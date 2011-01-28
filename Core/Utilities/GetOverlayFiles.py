'''
Created on Jan 28, 2011

@author: sposs
'''
from DIRAC import gLogger
import os

def getOverlayFiles():
  localfiles = []
  if not os.path.exists("./overlay"):
    gLogger.error('overlay directory does not exists')
    return localfiles
  curdir = os.getcwd()
  os.chdir("./overlay")
  listdir = os.listdir(os.getcwd())
  for item in listdir:
    if item.count('.slcio'):
      localfiles.append(os.getcwd()+os.sep+item)
  os.chdir(curdir)
  return localfiles