#/bin/env python

"""
Registers a user in ILCDIRAC. Registers him in the CS, the FileCatalog, and in the e-group. 
For that last functionality to be work, you need to have the sections 
/Security/egroupAdmin and /Security/egroupPass options in a dirac.cfg. 
Ideally, the local dirac.cfg (~/.dirac.cfg) can be used. It's also possible to pass the value with
-o /Security/egroupAdmin=something -o /Security/egroupPass=something
"""

try:
  import suds
except:
  print "suds module missing: Install it with easy_install suds"
  exit(1)

from DIRAC.Core.Base import Script

from DIRAC import S_OK, S_ERROR
class Params(object):
  def __init__(self):
    self.uname = ''
    self.groups = []
    self.DN = ''
    self.CN = ''
    self.Email = ''
    self.cernid = ''
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
  def setCERNID(self,opt):
    self.cernid = opt
    return S_OK()
  def registerSwitches(self):
    Script.registerSwitch("U:", "UserName=", "DIRAC user name", self.setUName)
    Script.registerSwitch("G:","Groups=","DIRAC groups in which to add the new user, comma separated", self.setGroup)
    Script.registerSwitch("D:","DN=","user DN",self.setDN)  
    Script.registerSwitch("C:","CN=","user CN (or CA)",self.setCN)
    Script.registerSwitch("E:","Email=","User mail",self.setEmail)
    Script.registerSwitch("","CCID=","CERN CC user ID (if any)", self.setCERNID)
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
      
  #Adding user to e-group
  try:
    client = suds.client.Client(url='https://cra-ws.cern.ch/cra-ws/CraEgroupsWebService.wsdl')
  except:
    gLogger.error("Failed to get the WSDL client")
    gLogger.error("User registration in e-group must be done manually")
    dexit(1)
  #Now get the admin account
  username = gConfig.getValue("/Security/egroupAdmin","")
  password = gConfig.getValue("/Security/egroupPass","")
  if not username or not password:
    gLogger.error("Missing configuration parameters: username or password for WSDL interactions")
    dexit(1)
    
  user = client.factory.create("ns1:MemberType")
  comm = "phonebook --login %s --terse firstname --terse surname --terse ccid --terse email" % clip.username
  from DIRAC.Core.Utilities.Subprocess import shellCall
  res = shellCall(0, comm)
  if not res['OK']:
    gLogger.error("Failed getting user info:",res['Message'])
    gLogger.error("Please add user in e-group by hand.")
    dexit(1)
  if res['Value'][0]:  
    gLogger.error("phonebook command returned an error:",res['Value'][2])
    gLogger.error("Please add user in e-group by hand.")
    dexit(1)
  output = res['Value'][1]
  if output:
    output = output.split("\n")
    if len(output)>2:
      gLogger.error("This user has many accounts, please choose the right one and register by hand")
      gLogger.error("%s"%output)
      dexit(1)
    user_fname = output[0].split(";")[0] #firstname
    user_sname = output[0].split(";")[1] # surname
    user['PrimaryAccount'] = clip.username.upper()
    user['ID'] = output[0].split(";")[2] # CCID
    user['Type'] = 'Person'
    user['Name'] = '%s, %s' %(user_sname.upper(), user_fname)
    user['Email'] = output[0].split(";")[3] #email
  else:
    gLogger.notice("User %s does not appear to be in the CERN phonebook, will register as 'External'")
    user['ID'] = clip.uname
    user['Type'] = 'External'
    user['Email'] = clip.Email

  userl = client.factory.create("ns1:MembersType")
  userl.Member.append(user)
  
  res = client.service.addEgroupMembers(username,password,'ilc-dirac',userl, False)
  
  dexit(0)  