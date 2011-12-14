"""
Upload SLIC version and publish
"""
from DIRAC.Core.Base import Script

version = ''
tarballloc = ''

Script.registerSwitch("v:","version=","version")
Script.registerSwitch("t:","tarball=","path to local tar ball")
Script.setUsageMessage( sys.argv[0]+'-v v2r11p2 -t path_to_tar_ball')
Script.parseCommandLine()
switches = Script.getUnprocessedSwitches()
for switch in switches:
  opt = switch[0]
  arg = switch[1]
  if opt in ('v','version'):
    version  = arg
  if opt in ('t','tarball'):
    tarballloc = arg

if not tarballloc and not version:
  Script.showHelp()
  sys.exit(2)

if not os.path.exists(tarballloc):
  print "Cannot find the tar ball %s"%tarballoc
  sys.exit(2)


