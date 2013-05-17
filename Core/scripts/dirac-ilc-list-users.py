#!/bin/env python
""" List the VO members
"""



def setUser(opt):
  from DIRAC import S_OK
  global username
  username = opt
  return S_OK()

if __name__ == "__main__":
  try:
    import suds
  except:
    print "Run easy_install suds"
    exit(1)

  from DIRAC.Core.Base import Script
  username = None
  Script.registerSwitch("u:", "UserName", "Family name of the user", setUser)  
  Script.parseCommandLine()
  
  from DIRAC.Core.Security.VOMSService import VOMSService
  from DIRAC import gLogger, exit as dexit
  v = VOMSService()
  res = v.admListMembers()
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  users = res['Value']
  for user in users:
    if not username:
      gLogger.notice("%s, %s, %s" % (user['DN'], user['CA'], user['mail']))
    else:
      if user['DN'].lower().count(username.lower()):
        gLogger.notice("%s, %s, %s" % (user['DN'], user['CA'], user['mail']))
        break 
