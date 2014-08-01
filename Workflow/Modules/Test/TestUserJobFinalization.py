#!/usr/env python

"""Test user jobfinalization"""


from ILCDIRAC.Workflow.Modules.UserJobFinalization import UserJobFinalization
from DIRAC import gLogger
from DIRAC.Core.Base import Script
Script.parseCommandLine()


gLogger.setLevel("DEBUG")
gLogger.showHeaders(True)

MYUJF = UserJobFinalization()
MYUJF.workflow_commons['TotalSteps'] = 1
MYUJF.workflow_commons['Owner'] = 'sailer'
MYUJF.workflow_commons['VO'] = 'ilc'
MYUJF.workflow_commons['UserOutputSE'] = ""
MYUJF.step_commons['STEP_NUMBER'] = 1
MYUJF.enable = False
MYUJF.log.setLevel("DEBUG")
MYUJF.workflow_commons["UserOutputSE"] = "CERN-SRM;CERN-DIP-3"

MYUJF.ignoreapperrors = True
MYUJF.userOutputData  = ['file1.txt','file2.txt']

RESULT = MYUJF.execute()
if 'Message' in RESULT:
  print RESULT['Message']
else:
  print "No output from execute"
