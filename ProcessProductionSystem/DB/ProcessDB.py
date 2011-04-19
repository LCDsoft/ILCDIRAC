###########################################################################
# $HeadURL: $
###########################################################################

""" ProcessDB for ProcessProduction System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB

class ProcessDB ( DB ):
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    self.dbname = 'ProcessDB'
    DB.__init__( self, self.dbname, 'ProcessProduction/ProcessDB', maxQueueSize  )
    self.ProdTypes = ['MCGeneration',"MCSimulation","MCReconstruction"]
  
  ##################################################################
  ### Getter methods
  def checkSoftware( self, AppName, AppVersion, connection = False ):
    """ Check if specified software exists
    """
    connection = self.__getConnection( connection )
    req = "SELECT idSoftware FROM Software WHERE AppName='%s' AND AppVersion='%s';"%(AppName,AppVersion)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not get software")
    if len(res['Value']):
      return res
    else:
      return S_ERROR("Could not find any software %s %s"%(AppName,AppVersion))
    
      
  def getSoftwares(self,connection = False):
    """ Return the list of softwares/version available, and valid
    """
    connection = self.__getConnection( connection )
    req = "SELECT idSoftware,AppName,AppVersion,Comment FROM Software WHERE Valid = TRUE;"
    res = self._query( req, connection )
    if not res['OK']:
      return res
    apps = {}
    for idSoftware,AppName,AppVersion,Comment in res['Value']:
      app = {}
      app['Name'] = AppName
      app['Version'] = AppVersion
      app['Comment'] = Comment
      ##Check for dependency
      req = "SELECT idDependency FROM DependencyRelation WHERE idSoftware=%s;"%(idSoftware)
      res = self._query( req, connection )
      depid = 0
      if not len(res['Value']):
          depid=0
      else:
        depid = res['Value'][0]
      app['Dependency'] = depid
      apps[idSoftware] = app
      
    return S_OK(apps)  
      
  def getProcessInfo(self, ProcessName, connection = False):
    """ Get the process info 
    """
    connection = self.__getConnection( connection )
    req = "SELECT idProcesses,ProcessName,Detail FROM Processes WHERE ProcessName='%s';"%(ProcessName)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    process= {}
    for idProcess,ProcessName,Detail in res['Value']:
      process[idProcess] = {"ProcessName":ProcessName,"Detail":Detail}
    return S_OK(process)
    
  
  def getProcessInfoForProdID(self, ProdID, connection = False):
    """ Get the process used for the specified production ID
    """
    connection = self.__getConnection( connection )
    
  
  def getProdIDsForProcess(self, ProcessName, connection = False):
    """ Get the productions that produced or used the specified process
    """
    connection = self.__getConnection( connection )
    
  
  def getProductionResults(self, ProdID, connection = False):
    """ Get the Cross Section, etc. for the specified production
    """
    connection = self.__getConnection( connection )
    
  
  def getTemplate(self, ProcessName, WhizardVersion, connection = False):
    """ Get the template name that describes the process specified in the whizard version specified
    """
    connection = self.__getConnection( connection )
    ##Check that process exists
    process = self.getProcessInfo(ProcessName, connection)
    if not process['OK']:
      return S_ERROR("Process %s does not exist"%ProcessName)
    ##Check software
    whizard = self.checkSoftware('Whizard', WhizardVersion, connection)
    if not whizard['OK']:
      return S_ERROR('Whizard %s does not exist'% WhizardVersion)
    
    req = "SELECT Template FROM Processes_has_Software WHERE idProcesses = (SELECT idProcesses FROM Processes WHERE ProcessName='%s') \
           AND idSoftware = (SELECT idSoftware FROM Software WHERE AppName='Whizard' AND AppVersion='%s');"% ( ProcessName, WhizardVersion)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    return S_OK(res['Value'][0])
  
  ##################################################################
  # Setter methods
  def addSoftware( self, AppName, AppVersion, Comment="", Path, connection = False ):
    """ Add the specified software if it does not exist
    """
    connection = self.__getConnection( connection )
    res = self.checkSoftware(AppName, AppVersion, connection)
    if res['OK']:
      return S_ERROR('Application %s, Version %s already defined in the database'%(AppName,AppVersion))
        
    res = self._escapeString( Comment )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Comment" )
    Comment = res['Value']  
    req = "INSERT INTO Software (AppName,AppVersion,Comment,Path,Defined) VALUES ('%s','%s','%s','%s',UTC_TIMESTAMP());"%(AppName,AppVersion,Comment,Path)
    res = self._update( req, connection )
    if not res['OK']:
      return res
    return res
  
  def addDependency(self, AppName, AppVersion, DepName, DepVersion, autodeclare = False, connection = False):
    """ Declare a dependency between 2 softwares 
    """
    connection = self.__getConnection( connection )
    res = self.checkSoftware(AppName, AppVersion, connection)
    if not res:
      return S_ERROR('Application %s, Version %s not defined in the database'%(AppVersion,AppName))
    appid = res['Value']
    res = self.checkSoftware(DepName, DepVersion, connection)
    if not res:
      if autodeclare:
        return S_ERROR("Auto declare of dependencies not implemented, please unset autodeclare.")
        #res =self.addSoftware(DepName, DepVersion, connection=connection)
        #if not res['OK']:
        #  return S_ERROR("Could not add dependency %s %s" % ( DepName, DepVersion) )
      else:
        return S_ERROR('Dependency %s, Version %s not defined in the database'%(DepVersion,DepName))
    depid = res['Value']  
    req = "INSERT INTO DependencyRelation VALUES (%s,%s);"%(appid,depid)
    res = self._update( req, connection )
    if not res['OK']:
      return res
    return res  
  
  def addSteeringFile(self, FileName, Path= None, connection = False):
    """ Add a new steering file
    """
    connection = self.__getConnection( connection )
    req = "INSERT INTO SteeringFiles (FileName) VALUES ('%s');" % FileName
    res = self._update( req, connection )
    if not res['OK']:
      return res
    res = self._query("SELECT LAST_INSERT_ID();",connection)    
    return res
 
  
  def addProcess(self, ProcessName, ProcessDetail, WhizardVers, Template, connection = False):
    """ Declare a new process or if it exists, declare relation to new whizard
    """
    connection = self.__getConnection( connection )
    res = self._escapeString( Template )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Template" )
    Template = res['Value']  
    
    ##Will need the row where the Process is, and create it is needed.
    processID = None
    res = self.getProcessInfo(ProcessName, connection)
    if not res['OK']:
      ## Add process in DB
      req = "INSERT INTO Processes (ProcessName,Detail) VALUES ('%s','%s');" % (ProcessName, ProcessDetail)
      res = self._update( req, connection )
      if not res['OK']:
        return res
      #Get the ProcessID: last row inserted
      req = "SELECT LAST_INSERT_ID()"
      res = self._update( req, connection )
      if not res['OK']:
        return res
      ProcessID = res['Value']
    else:
      ProcessID,ProcessName,ProcessDetail = res['Value']
        
    ##Get the software ID
    req = "SELECT idSoftware FROM Softwares WHERE AppName='Whizard' AND AppVersion='%s';"%(WhizardVers)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR('Whizard version %s not found in DB, make sure you declared it')
    SoftwareID = res['Value']
    
    req = "INSERT INTO Processes_has_Software (idProcesses,idSoftware,Template) VALUES ( %s, %s, '%s');"% (ProcessID,SoftwareID,Template) 
    res = self._update( req, connection )
    return res
  
  def addProductionData(self, ProdDataDict, connection = False):
    """ Declare a new Production
    """
    ProdID = ProdDataDict['ProdID']
    ProdType = ProdDataDict['Type']
    ProcessName = ProdDataDict['Process']
    Path = ProdDataDict['Path']
    AppName = ProdDataDict['AppName']
    AppVersion = ProdDataDict['AppVersion']

    SteeringFile = None
    if ProdDataDict.has_key('SteeringFile'):
      SteeringFile = ProdDataDict['SteeringFile']

    if not ProdType in self.ProdTypes:
      return S_ERROR("Production type %s not available"%(ProdType))
    connection = self.__getConnection( connection )
    
    #Get the ProcessID
    req = "SELECT idProcesses FROM Processes WHERE ProcessName='%s';"% ( ProcessName )
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR('Could not find Process %s'%ProcessName)
    if not len(res['Value']):
      return S_ERROR('Could not find Process %s'%ProcessName)
    ProcessID = res['Value'][0]
    
    ##Create the ProcessData
    req = "INSERT INTO ProcessData (idProcesses,Path) VALUES (%s,'%s');" % (ProcessID,Path)
    res = self._update( req, connection )
    if not res['OK']:
      return S_ERROR("Could not insert ProcessData into DB")
    #Get that line's ID
    req = "SELECT LAST_INSERT_ID();"
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR('Failed to get last insert ID')
    ProcessDataID = res['Value'][0]
    
    #Declare new production
    req = "INSERT INTO Productions (idSoftware,idProcessData,ProdID,Type) VALUES \
           ( (SELECT idSoftware FROM Software WHERE AppName='%s' AND AppVersion='%s'), \
           %d, %d, '%s');" % (AppName, AppVersion, ProcessDataID, ProdID, ProdType)
    res = self._update( req, connection ) 
    if not res['OK']:
      return res
    ##In Case there is a steering file
    if SteeringFile:
      req = "SELECT idfiles FROM SteeringFiles WHERE FileName='%s';" % SteeringFile
      res = self._query( req, connection )
      if not len(res['Value']):
        res = self.addSteeringFile( SteeringFile, connection = connection)
      idSteering = res['Value'][0]
      req = "INSERT INTO SteeringFiles_has_ProcessData (idfiles,idProcessData) VALUES ( %s, %s);"% (idSteering,ProcessDataID)
      res = self._query( req, connection )

    return S_OK()  
           
  ########################################################################
  # Update methods
  def updateCrossSection(self, ProcessDict, connection = False):
    """ Update the cross section of the given process in the given production
    """
    connection = self.__getConnection( connection )

    #ProcessName = ProcessDict['ProcessName']
    ProdID = ProcessDict['ProdID']
    App = ProcessDict['AppName']
    CrossSection = ProcessDict['CrossSection']

    req = "SELECT idProcessData,CrossSection,Files FROM ProcessData WHERE \
           idProcessData=(SELECT idProcessData FROM Productions WHERE ProdID=%s AND AppName=%s);"%( ProdID, App )
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not find matching ProcessData for Production %s" % ProdID)
    processDataID,OldCrossSection,OldFiles = res['Value']
    temp_crosssection = OldCrossSection*OldFiles
    Files = OldFiles + 1
    CrossSection += temp_crosssection
    CrossSection = CrossSection/Files
    req = "UPDATE ProcessData SET CrossSection=%s,Files=%s WHERE idProcessData=%s;" %( CrossSection, Files, processDataID)
    res = self._update( req, connection ) 
    if not res['OK']:
      return res
    return S_OK()

  def changeSoftwareStatus ( self, AppName, AppVersion, Comment, Status=False, connection = False ):
    """ Change validity of software
    """
    connection = self.__getConnection( connection )
    res = self._escapeString( Comment )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Comment" )
    Comment = res['Value']      
    res = self.checkSoftware(AppName, AppVersion, connection)
    if not res:
      return S_ERROR('Application %s, Version %s not defined in the database'%(AppVersion,AppName))
    new_status='FALSE'
    if Status:
      new_status='TRUE'
    req = "UPDATE Software SET Valid=%s,UpdateComment='%s',LastUpdate=UTC_TIMESTAMP() WHERE AppName='%s' AND AppVersion='%s';"%(new_status,Comment,AppName,AppVersion)
    res = self._update( req, connection )
    if not res['OK']:
      return res
    ##Now update also dependent software (Only to FALSE)
    if not Status:
      req = "SELECT idSoftware FROM DependencyRelation WHERE idDependency = (SELECT idSoftware FROM Software WHERE AppName='%s' AND AppVersion='%s');" % (AppName,AppVersion)
      res = self._query( req, connection )
      if not res['OK'] or not len(res['Value']):
        return S_ERROR('Could not find any dependency')
      for id in res['Value']:
        req = "UPDATE Software SET Valid=FALSE,UpdateComment='Dependency inheritance',LastUpdate=UTC_TIMESTAMP() WHERE idSoftware = %s;"%id
        res = self._update( req, connection )
        if not res['OK']:
          return res
    return res

  #####################################################################
  # Private methods

  def __getConnection( self, connection ):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn( "Failed to get MySQL connection", res['Message'] )
    return connection
  