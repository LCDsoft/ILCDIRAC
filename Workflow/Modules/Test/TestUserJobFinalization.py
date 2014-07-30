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
MYUJF.workflow_commons['IgnoreAppError'] = True
MYUJF.workflow_commons['UserOutputData'] = ['file1.txt','file2.txt']
MYUJF.step_commons['STEP_NUMBER'] = 1
MYUJF.enable = False
MYUJF.log.setLevel("DEBUG")
MYUJF.workflow_commons["UserOutputSE"] = "CERN-SRM;CERN-DIP-3"

RESULT = MYUJF.execute()
if 'Message' in RESULT:
  print RESULT['Message']
else:
  print "No output from execute"
