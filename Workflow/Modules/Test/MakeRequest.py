#!/usr/env python

"""
Make a RemovalRequest for the new System
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.RequestManagementSystem.Client.Request import Request

from DIRAC.RequestManagementSystem.Client.File              import File
from DIRAC.RequestManagementSystem.Client.Operation         import Operation
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.RequestManagementSystem.Client.ReqClient         import ReqClient


def myRequest():
  """Create a request and put it to the db"""

  request = Request()
  request.RequestName = 'myAwesomeRemovalRequest.xml'
  request.JobID = 0
  request.SourceComponent = "myScript"

  remove = Operation()
  remove.Type = "RemoveFile"

  lfn = "/ilc/user/s/sailer/test.txt"
  rmFile = File()
  rmFile.LFN = lfn
  remove.addFile( rmFile )

  request.addOperation( remove )
  isValid = RequestValidator().validate( request )
  if not isValid['OK']:
    raise RuntimeError( "Failover request is not valid: %s" % isValid['Message'] )
  else:
    print "It is a GOGOGO"
    requestClient = ReqClient()
    result = requestClient.putRequest( request )
    print result

if __name__ == "__main__":
  myRequest()
