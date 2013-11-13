'''
Run the StdHepCutJava utility

Apply a set of cuts on input stdhep files

@since: Apr 10, 2013

@author: Stephane Poss
'''
__RCSID__ = "$Id$"

from DIRAC                                                import gLogger
from ILCDIRAC.Workflow.Modules.StdHepCut                  import StdHepCut

import os

class StdHepCutJava(StdHepCut):
  """ Apply cuts on stdhep files, based on L. Weuste utility, rewritten in java by C. Grefe.
  """
  def __init__(self):
    super(StdHepCutJava, self).__init__()
    self.log = gLogger.getSubLogger( "StdhepCutJava" )
    self.applicationName = 'stdhepcutjava'
        
  def prepareScript(self, mySoftDir):
    """ Overloaded from stdhepcuts
    """
    self.scriptName = '%s_%s_Run_%s.sh' % (self.applicationName, self.applicationVersion, self.STEP_NUMBER)
    if os.path.exists(self.scriptName): 
      os.remove(self.scriptName)
    script = open(self.scriptName, 'w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    if os.path.exists("lib"):
      script.write("declare -x CLASSPATH=./lib:$CLASSPATH\n")
    script.write('echo =========\n')
    script.write('echo java version :\n')
    script.write('java -version\n')
    script.write('env | sort >> localEnv.log\n')      
    script.write('echo =============================\n')
    extraopts = ""
    if self.MaxNbEvts:
      extraopts = '-m %s' % self.MaxNbEvts
    comm = "java -Xmx1536m -Xms256m -jar %s %s -o %s -c %s  *.stdhep\n" % (mySoftDir, extraopts, 
                                                                           self.OutputFile, self.SteeringFile)
    self.log.info("Running %s" % comm)
    script.write(comm)
    script.write('declare -x appstatus=$?\n')    
    script.write('exit $appstatus\n')
    script.close()
    