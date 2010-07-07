# $HeadURL$
# $Id$

__RCSID__ = "$Id$"
"""
  Update Users and Groups from VOMS on CS : special ILC flavour, by s Poss
"""
import os
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC.ConfigurationSystem.Client.CSAPI          import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Core.Security.VOMSService                 import VOMSService
from DIRAC.Core.Security                             import Locations, X509Chain
from DIRAC.Core.Utilities                            import List, Subprocess
from DIRAC                                           import S_OK, S_ERROR, gConfig

class UsersAndGroups( AgentModule ):

  def initialize( self ):
    self.am_setOption( "PollingTime", 3600 * 6 ) # Every 6 hours
    self.vomsSrv = VOMSService()
    self.proxyLocation = os.path.join( self.am_getOption( "WorkDirectory" ), ".volatileId" )
    return S_OK()
  
#  def __generateProxy( self ):
#    self.log.info( "Generating proxy..." )
#    certLoc = Locations.getHostCertificateAndKeyLocation()
#    if not certLoc:
#      self.log.error( "Can not find certificate!" )
#      return False
#    chain = X509Chain.X509Chain()
#    result = chain.loadChainFromFile( certLoc[0] )
#    if not result[ 'OK' ]:
#      self.log.error( "Can not load certificate file", "%s : %s" % ( certLoc[0], result[ 'Message' ] ) )
#      return False
#    result = chain.loadKeyFromFile( certLoc[1] )
#    if not result[ 'OK' ]:
#      self.log.error( "Can not load key file", "%s : %s" % ( certLoc[1], result[ 'Message' ] ) )
#      return False
#    result = chain.generateProxyToFile( self.proxyLocation, 3600 )
#    if not result[ 'OK' ]:
#      self.log.error( "Could not generate proxy file", result[ 'Message' ] )
#      return False
#    self.log.info( "Proxy generated" )
#    return True
    
#  def getLFCRegisteredDNs( self ):
#    #Request a proxy
#    if gConfig._useServerCertificate():
#      if not self.__generateProxy():
#        return False
#    #Execute the call
#    cmdEnv = dict( os.environ )
#    cmdEnv['LFC_HOST'] = 'lfc-lcd.cern.ch'
#    if os.path.isfile( self.proxyLocation ):
#      cmdEnv[ 'X509_USER_PROXY' ] = self.proxyLocation
#    lfcDNs = []
#    try:
#      retlfc = Subprocess.systemCall( 0, ( 'lfc-listusrmap', ), env = cmdEnv )
#      if not retlfc['OK']:
#        self.log.fatal( 'Can not get LFC User List', retlfc['Message'] )
#        return retlfc
#      if retlfc['Value'][0]:
#        self.log.fatal( 'Can not get LFC User List', retlfc['Value'][2] )
#        return S_ERROR( "lfc-listusrmap failed" )
#      else:
#        for item in List.fromChar( retlfc['Value'][1], '\n' ):
#          dn = item.split( ' ', 1 )[1]
#          lfcDNs.append( dn )
#      return S_OK( lfcDNs )
#    finally:
#      if os.path.isfile( self.proxyLocation ):
#        self.log.info( "Destroying proxy..." )
#        os.unlink( self.proxyLocation )

#  def checkLFCRegisteredUsers( self, usersData ):
#    self.log.info( "Checking LFC registered users" )
#    usersToBeRegistered = {}
#    result = self.getLFCRegisteredDNs()
#    if not result[ 'OK' ]:
#      self.log.error( "Could not get a list of registered DNs from LFC", result[ 'Message' ] )
#      return result
#    lfcDNs = result[ 'Value' ]
#    for user in usersData:
#      if usersData[ user ][ 'DN' ] not in lfcDNs:
#        self.log.info( 'DN %s need to be registered in LFC for user %s' % ( usersData[user]['DN'], user ) )
#        if user not in usersToBeRegistered:
#          usersToBeRegistered[ user ] = []
#        usersToBeRegistered[ user ].append( usersData[user]['DN'] )
#
#    address = self.am_getOption( 'MailTo', 'lcd-vo-admin@cern.ch' )
#    fromAddress = self.am_getOption( 'mailFrom', 'stephane.poss@cern.ch' )
#    if usersToBeRegistered:
#      subject = 'New LFC Users found'
#      self.log.info( subject, ", ".join( usersToBeRegistered ) )
#      body = 'Command to add new entries into LFC: \n'
#      body += 'login to volcd01 and run : \n'
#      body += 'source /afs/cern.ch/lhcb/software/releases/LBSCRIPTS/prod/InstallArea/scripts/LbLogin.csh \n\n'
#      for lfcuser in usersToBeRegistered:
#        for lfc_dn in usersToBeRegistered[lfcuser]:
#          body += 'add_DN_LFC --userDN="' + lfc_dn.strip() + '" --nickname=' + lfcuser + '\n'
#
#    NotificationClient().sendMail( address, 'UsersAndGroupsAgent: %s' % subject, body, fromAddress )

  def execute( self ):

    #Get DIRAC VOMS Mapping
    self.log.info( "Getting DIRAC VOMS mapping" )
    mappingSection = '/Registry/VOMS/Mapping'
    ret = gConfig.getOptionsDict( mappingSection )
    if not ret['OK']:
      self.log.fatal( 'No VOMS to DIRAC Group Mapping Available' )
      return ret
    vomsMapping = ret['Value']
    self.log.info( "There are %s registered voms mappings in DIRAC" % len( vomsMapping ) )

    #Get VOMS VO name
    self.log.info( "Getting VOMS VO name" )
    result = self.vomsSrv.admGetVOName()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve VOMS VO name' )
    voNameInVOMS = result[ 'Value' ]
    self.log.info( "VOMS VO Name is %s" % voNameInVOMS )

    #Get VOMS roles
    self.log.info( "Getting the list of registered roles in VOMS" )
    result = self.vomsSrv.admListRoles()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve registered roles in VOMS' )
    rolesInVOMS = result[ 'Value' ]
    self.log.info( "There are %s registered roles in VOMS" % len( rolesInVOMS ) )

    #Map VOMS roles
    vomsRoles = {}
    for role in rolesInVOMS:
      role = "%s/%s" % ( voNameInVOMS, role )
      groupsForRole = []
      for group in vomsMapping:
        if vomsMapping[ group ] == role:
          groupsForRole.append( group )
      if groupsForRole:
        vomsRoles[ role ] = { 'Groups' : groupsForRole, 'Users' : [] }
    self.log.info( "DIRAC valid VOMS roles are:\n\t", "\n\t ".join( vomsRoles.keys() ) )

    #Get DIRAC users
    self.log.info( "Getting the list of registered users in DIRAC" )
    csapi = CSAPI()
    ret = csapi.listUsers()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve current list of Users' )
      return ret
    currentUsers = ret['Value']

    ret = csapi.describeUsers( currentUsers )
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve current User description' )
      return ret
    currentUsers = ret['Value']
    self.log.info( "There are %s registered users in DIRAC" % len( currentUsers ) )

    #Get VOMS user entries
    self.log.info( "Getting the list of registered user entries in VOMS" )
    result = self.vomsSrv.admListMembers()
    if not ret['OK']:
      self.log.fatal( 'Could not retrieve registered user entries in VOMS' )
    usersInVOMS = result[ 'Value' ]
    self.log.info( "There are %s registered user entries in VOMS" % len( usersInVOMS ) )

    #Consolidate users by nickname
    usersData = {}
    newUserNames = []
    knownUserNames = []
    obsoleteUserNames = []
    self.log.info( "Retrieving usernames..." )
    usersInVOMS.sort()
    for user in usersInVOMS:
      result = self.vomsSrv.attGetUserNickname( user[ 'DN' ], user[ 'CA' ] )
      if not result[ 'OK' ]:
        self.log.error( "Could not get nickname for DN %s" % user[ 'DN' ] )
        return result
      userName = result[ 'Value' ]
      if not userName:
        self.log.error( "Empty nickname for DN %s" % user[ 'DN' ] )
        continue
      self.log.info( " Found username %s" % ( userName ) )
      if userName not in usersData:
        usersData[ userName ] = { 'DN': [], 'CA': [], 'Email': [], 'Groups' : [] }
      for key in ( 'DN', 'CA', 'mail' ):
        value = user[ key ]
        if value:
          if key == "mail":
            List.appendUnique( usersData[ userName ][ 'Email' ], value )
          else:
            usersData[ userName ][ key ].append( value )
      if userName not in currentUsers:
        List.appendUnique( newUserNames, userName )
      else:
        List.appendUnique( knownUserNames, userName )

    for user in currentUsers:
      if user not in usersData:
        self.log.info( 'User %s is no longer valid' % user )
        obsoleteUserNames.append( user )

    if newUserNames:
      self.log.info( "There are %s new users" % len( newUserNames ) )
    else:
      self.log.info( "There are no new users" )

    #Get the list of users for each group
    result = csapi.listGroups()
    if not result[ 'OK' ]:
      self.log.error( "Could not get the list of groups in DIRAC", result[ 'Message' ] )
      return result
    staticGroups = result[ 'Value' ]
    self.log.info( "Mapping users in VOMS to groups" )
    for vomsRole in vomsRoles:
      self.log.info( "  Getting users for role %s" % vomsRole )
      groupsForRole = vomsRoles[ vomsRole ][ 'Groups' ]
      vomsMap = vomsRole.split( "Role=" )
      vomsGroup = "Role=".join( vomsMap[:-1] )
      for g in groupsForRole:
        if g in staticGroups:
          staticGroups.pop( staticGroups.index( g ) )
      if vomsGroup[-1] == "/":
        vomsGroup = vomsGroup[:-1]
      vomsRole = "Role=%s" % vomsMap[-1]
      result = self.vomsSrv.admListUsersWithRole( vomsGroup, vomsRole )
      if not result[ 'OK' ]:
        self.log.error( "Could not get list of users for VOMS %s" % ( vomsMapping[ group ] ), result[ 'Message' ] )
        return result
      numUsersInGroup = 0
      for vomsUser in result[ 'Value' ]:
        for userName in usersData:
          if vomsUser[ 'DN' ] in usersData[ userName ][ 'DN' ]:
            numUsersInGroup += 1
            usersData[ userName ][ 'Groups' ].extend( groupsForRole )
      self.log.info( "  There are %s users in group(s) %s" % ( numUsersInGroup, ",".join( groupsForRole ) ) )

    self.log.info( "Checking static groups" )
    for group in staticGroups:
      self.log.info( "  Checking static group %s" % group )
      numUsersInGroup = 0
      result = csapi.listUsers( group )
      if not result[ 'OK' ]:
        self.log.error( "Could not get the list of users in DIRAC group %s" % group , result[ 'Message' ] )
        return result
      for userName in result[ 'Value' ]:
        if userName in usersData:
          numUsersInGroup += 1
          usersData[ userName ][ 'Groups' ].append( group )
      self.log.info( "  There are %s users in group %s" % ( numUsersInGroup, group ) )

    #Do the CS Sync
    self.log.info( "Updating CS..." )
    ret = csapi.downloadCSData()
    if not ret['OK']:
      self.log.fatal( 'Can not update from CS', ret['Message'] )
      return ret

    for user in usersData:
      csUserData = dict( usersData[ user ] )
      for k in ( 'DN', 'CA', 'Email' ):
        csUserData[ k ] = ", ".join( csUserData[ k ] )
      result = csapi.modifyUser( user, csUserData, createIfNonExistant = True )
      if not result[ 'OK' ]:
        self.log.error( "Cannot modify user %s" % user )

    if obsoleteUserNames:
      self.log.info( "Deleting %s users" % len( obsoleteUserNames ) )
      csapi.deleteUsers( obsoleteUserNames )

    result = csapi.commitChanges()
    if not result[ 'OK' ]:
      self.log.error( "Could not commit configuration changes", result[ 'Message' ] )
      return result
    self.log.info( "Configuration committed" )

    #LFC Check
    #if self.am_getOption( "LFCCheckEnabled", True ):
    #  result = self.checkLFCRegisteredUsers( usersData )
    #  if not result[ 'OK' ]:
    #    return result

    return S_OK()
