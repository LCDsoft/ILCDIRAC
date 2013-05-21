#/bin/env python

from DIRAC.Core.Base import Script

from DIRAC import S_OK, S_ERROR
class Params(object):
  def __init__(self):
    self.uname = ''
    self.groups = []
    self.DN = ''
    self.CN = ''
    self.Email = ''
  def setUName(self,opt):
    self.uname = opt
    return S_OK()
  def setGroup(self,opt):
    self.groups = opt.split(",")
    return S_OK()
  def setDN(self,opt):
    self.DN = opt
    return S_OK()
  def setCN(self,opt):
    self.CN = opt
    return S_OK()
  def setEmail(self,opt):
    if not opt.find( '@' ) > 0:
      return S_ERROR('Not a valid mail address')
    self.Email = opt
    return S_OK()
  def registerSwitches(self):
    Script.registerSwitch("U:", "UserName=", "DIRAC user name", self.setUName)
    Script.registerSwitch("G:","Groups=","DIRAC groups in which to add the new user, comma separated", self.setGroup)
    Script.registerSwitch("D:","DN=","user DN",self.setDN)  
    Script.registerSwitch("C:","CN=","user CN (or CA)",self.setCN)
    Script.registerSwitch("E:","Email=","User mail",self.setEmail)
    Script.setUsageMessage("%s -U toto -G ilc_user,private_pilot -D /something/ -C /somethingelse/ -E toto@aplace.com" % Script.scriptName)
    
if __name__=="__main__":
  clip = Params()
  clip.registerSwitches()
  Script.parseCommandLine()
  from DIRAC.Interfaces.API.DiracAdmin import voName
  from DIRAC import gLogger, gConfig, exit as dexit
  
  if not clip.uname or not clip.CN or not clip.groups or not clip.DN or not clip.Email:
    gLogger.error("All attributes are mandatory")
    Script.showHelp()
    dexit(1)
  userProps = {'DN': clip.DN, 'Email': clip.Email, 'CN': clip.CN, 'Groups': clip.groups}
    
  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()
  exitCode = 0
  errorList = []
    
  if not diracAdmin.csModifyUser( clip.uname, userProps, createIfNonExistant = True )['OK']:
    errorList.append( ( "add user", "Cannot register user %s" % userName ) )
    exitCode = 255
  else:
    result = diracAdmin.csCommitChanges()
    if not result[ 'OK' ]:
      errorList.append( ( "commit", result[ 'Message' ] ) )
      exitCode = 255

  for error in errorList:
    gLogger.error( "%s: %s" % error )
  if exitCode:
    dexit(exitCode)  
    
  #Now try to figure out in which VOs the user must be, and create the catalog entries accordingly
  from DIRAC.ConfigurationSystem.Client.Helpers  import Registry
  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  fc = FileCatalogClient()
  res = fc.addUser(clip.uname)
  if not res['OK']:
    gLogger.error("Failed to add user to FC, but it does not really matter:", res['Message'])
    gLogger.error("Add by hand (in the FC-CLI: user add %s)" %(clip.uname) )

  bpath = ''  
  for grp in clip.groups:
    bpath = '/'
    voName = Registry.getVOForGroup(grp)
    if not voName:
      gLogger.error("NO VO for group", grp )
      continue
    bpath += voName+"/"
    lfnprefix = gConfig.getValue("/Operations/%s/LFNUserPrefix" % voName, "")
    if lfnprefix:
      bpath += lfnprefix+"/"
    bpath += clip.uname[0]+"/"+clip.uname+"/"

    res = fc.createDirectory(bpath)
    if not res['OK']:
      gLogger.error(res['Message'])
      continue
    
    res = fc.changePathGroup({ bpath: { "Group": grp } }, False)
    if not res['OK']:
      gLogger.error(res['Message'])
      
    res = fc.changePathOwner({ bpath: {"Owner": clip.uname } }, False)
    if not res['OK']:
      gLogger.error(res['Message'])
    
    res = fc.setMetadata(bpath, {"Owner":clip.uname})
    if not res['OK']:
      gLogger.error(res['Message'])  
    
  dexit(0)  