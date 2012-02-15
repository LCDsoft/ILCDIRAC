###########################################################################
# $HeadURL: $
###########################################################################

""" ProcessDB for ProcessProduction System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
from DIRAC.Core.Utilities.List                                         import stringListToString, intListToString, sortList

class ProcessDB ( DB ):
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    self.dbname = 'ProcessDB'
    DB.__init__( self, self.dbname, 'ProcessProduction/ProcessDB', maxQueueSize  )
    self.ProdTypes = ['MCGeneration',"MCSimulation","MCReconstruction"]
    self.SoftwareParams = ['Path','Valid','AppName','AppVersion','Platform']
    self.ProcessDataParams = ['CrossSection','NbEvts','Path','Files','Polarisation']
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      self.log.fatal( "Cannot initialize DB!", result[ 'Message' ] )
  
  def __initializeDB(self):
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal
    
    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesToCreate = {}
    
    if not 'Software' in tablesInDB:
      tablesToCreate['Software'] = { 'Fields' :{ 'idSoftware':'INT NOT NULL AUTO_INCREMENT',
                                                 'AppName' : 'VARCHAR(45) NOT NULL',
                                                 'AppVersion' : 'VARCHAR(45) NOT NULL',
                                                 'Platform' :  'VARCHAR(45) NOT NULL',
                                                 'Valid' : 'TINYINT(1)  NOT NULL DEFAULT TRUE',
                                                 'Comment' : 'VARCHAR(255) NULL',
                                                 'UpdateComment' : 'VARCHAR(255) NULL',
                                                 'Defined' : 'DATETIME',
                                                 'LastUpdate' : 'DATETIME',
                                                 'Path' : 'VARCHAR(512) NOT NULL'
                                                },
                                     'PrimaryKey': 'idSoftware',
                                     'Indexes' : { 'Application' : ['AppName','AppVersion']
                                                  },
                                     'UniqueIndexes' : {'idSoftware_UNIQUE':['idSoftware']}
                                    }
    
    if not 'Processes' in tablesInDB:
      tablesToCreate['Processes'] = { 'Fields' : { 'idProcesses' : 'INT NOT NULL AUTO_INCREMENT',
                                                  'ProcessName' : 'VARCHAR(45) NOT NULL',
                                                  'Detail' : 'VARCHAR(45) NULL'
                                                  },
                                      'PrimaryKey': 'idProcesses',
                                      'Indexes' : { 'ProcessName' : ['ProcessName']},
                                      'UniqueIndexes' : {'idProcesses_UNIQUE':['idProcesses'],
                                                         'ProcessName_UNIQUE':['ProcessName']}                         
                                     }
    
    if not 'Processes_has_Software' in tablesInDB:
      tablesToCreate['Processes_has_Software'] = { 'Fields' : { 'idProcesses' : 'INT NOT NULL',
                                                                'idSoftware' : 'INT NOT NULL'
                                                               },
                                                    'PrimaryKey' : ['idProcesses','idSoftware'],
                                                    'Indexes' : { 'fk_Processes_has_Software_Software1' :['idSoftware'],
                                                                  'fk_Processes_has_Software_Processes1' : ['idProcesses'] },
                                                    'ForeignKeys' : {'idSoftware':'Software.idSoftware',
                                                                     'idProcesses':'Processes.idProcesses'}
                                                  }
    if not 'ProcessData' in tablesInDB:
      tablesToCreate['ProcessData'] = { 'Fields' : { 'CrossSection': 'DOUBLE(10,6) NULL DEFAULT 0',
                                                     'Path' : 'VARCHAR(255) NULL',
                                                     'NbEvts' : 'INT NULL DEFAULT 0',
                                                     'Files' : 'INT NULL DEFAULT 0',
                                                     'idProcessData' : 'INT NOT NULL AUTO_INCREMENT',
                                                     'idProcesses' : 'INT NOT NULL',
                                                     'Polarisation' : 'VARCHAR(10) NULL'
                                                    },
                                        'PrimaryKey' : ['idProcessData','idProcesses'],
                                        'Indexes' : { 'fk_ProcessData_Processes1' : ['idProcesses']},
                                        'UniqueIndexes' : { 'idProcessData_UNIQUE' : ['idProcessData']},
                                        'ForeignKeys' : { 'idProcesses' : 'Processes.idProcesses'}
                                       }
    
    if not 'Productions' in tablesInDB:
      tablesToCreate['Productions'] = { 'Fields' : {'idSoftware':'INT NOT NULL',
                                                    'idProcessData':'INT NOT NULL',
                                                    'ProdID':'INT NOT NULL',
                                                    'ProdDetail': 'VARCHAR(255) BINARY NULL',
                                                    'idProduction' : 'INT NOT NULL AUTO_INCREMENT',
                                                    'Type':'VARCHAR(45) NOT NULL'
                                                    },
                                        'PrimaryKey': ['idProduction','idProcessData'],
                                        'Indexes' : {'fk_Software_has_ProcessData_Software1':['idSoftware'],
                                                     'fk_Productions_ProcessData1':['idProcessData'],
                                                     'ProdID':['ProdID']},
                                        'ForeignKeys': {'idSoftware':'Software.idSoftware','idProcessData':'ProcessData.idProcessData'}
                                       }
      
    if not 'SteeringFiles' in tablesInDB:
      tablesToCreate['SteeringFiles'] = { 'Fields' : {'idFile': 'INT NOT NULL AUTO_INCREMENT',
                                                       'FileName': 'VARCHAR(45) NOT NULL'
                                                       },
                                          'PrimaryKey':  ['idFile','FileName'],
                                          'UniqueIndexes' : {'FileName_UNIQUE':['FileName']}
                                          }
    if not 'SteeringFiles_has_ProcessData' in tablesInDB:
      tablesToCreate['SteeringFiles_has_ProcessData'] = { 'Fields' :{ 'idFile': 'INT NOT NULL',
                                                                      'idProcessData': 'INT NOT NULL'
                                                                     },
                                                          'PrimaryKey' : ['idfiles','idProcessData'],
                                                          'Indexes' :{'fk_SteeringFiles_has_ProcessData_ProcessData1':['idProcessData'],
                                                                      'fk_SteeringFiles_has_ProcessData_SteeringFiles1':['idFile']},
                                                          'ForeignKeys': { 'idFile' : 'SteeringFiles.idFile',
                                                                          'idProcessData':'ProcessData.idProcessData'}
                                                         }
    if not 'DependencyRelation' in tablesInDB:
      tablesToCreate['DependencyRelation'] = { 'Fields' : { 'idSoftware' : 'INT NOT NULL',
                                                            'idDependency' : 'INT NOT NULL',
                                                            'idDependencyRelation' : 'INT NOT NULL AUTO_INCREMENT'
                                                           },
                                               'PrimaryKey' : 'idDependencyRelation',
                                               'Indexes' : {'fk_Software_has_Software_Software2':['idDependency'],
                                                            'fk_Software_has_Software_Software1': ['idSoftware']},
                                               'ForeignKeys': {'idSoftware':'Software.idSoftware','idDependency':'Software.idSoftware'}             
                                              }
    
    if not 'ProductionRelation' in tablesInDB:
      tablesToCreate['ProductionRelation'] = { 'Fields' : { 'idRelation':'INT NOT NULL AUTO_INCREMENT',
                                                            'idMotherProd' : 'INT NOT NULL',
                                                            'idDaughterProd' : 'INT NOT NULL'
                                                           },
                                               'PrimaryKey': 'idRelation',
                                               'Indexes' : {'Daughter':['idDaughterProd'],'Mother':['idMotherProd']},
                                               'ForeignKeys': { 'idMotherProd':"Productions.idProduction", 
                                                               "idDaughterProd":"Productions.idProduction"}
                                              }
       
    if tablesToCreate:
      return self._createTables( tablesToCreate ) 
    return S_OK()
  ##################################################################
  ### Getter methods
  def _checkSoftware( self, AppName, AppVersion, Platform, connection = False ):
    """ Check if specified software exists
    """
    connection = self.__getConnection( connection )
    extrareqs = ''
    if not Platform is 'any':
      extrareqs = " AND Platform='%s'"%Platform
    req = "SELECT idSoftware FROM Software WHERE AppName='%s' AND AppVersion='%s' %s;"%(AppName,AppVersion, extrareqs)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not get software")
    if len(res['Value']):
      return res
    else:
      return S_ERROR("Could not find any software %s %s"%(AppName,AppVersion))
    
  def getSoftwareParams(self,AppName,AppVersion,Platform, Params, connection = False):    
    """ Check status of given software
    """
    for param in Params:
      if not param in self.SoftwareParams:
        return S_ERROR("Parameter %s is not valid"%param)
    connection = self.__getConnection( connection )
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if not res['OK']:
      return res
    extrareqs = ''
    if not Platform is 'any':
      extrareqs = " AND Platform='%s'"%Platform
    
    req = "SELECT %s FROM Software WHERE AppName='%s' AND AppVersion='%s' %s"%(intListToString( Params ),AppName,AppVersion,extrareqs)
    res = self._query( req, connection )
    if not res['OK']:
      return res
    reslist = []  
    for row in res['Value']:
      resdict = {}
      count = 0
      for item in row:
        resdict[Params[count]]=item
        count += 1
      reslist.append(resdict)
    return S_OK(reslist)
  
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
      
  def getProcessInfo(self, ProcessName, Params, connection = False):
    """ Get the process info 
    """
    connection = self.__getConnection( connection )
    req = "SELECT %s FROM Processes WHERE ProcessName='%s';"%(intListToString( Params ), ProcessName)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    row = res['Value'][0]
    resdict = {}
    count = 0
    for item in row:
      resdict[Params[count]]=item
      count += 1
    return S_OK(resdict)
  
  def getProdIDsForProcess(self, ProcessName, connection = False):
    """ Get the productions that produced or used the specified process
    """
    connection = self.__getConnection( connection )
    Params = ['idProcesses']
    res = self.getProcessInfo(ProcessName, Params, connection)
    if not res['OK']:
      return res
    processID = res['Value']['idProcesses']
    req = "SELECT ProdID,Type FROM Production WHERE idProcessData IN (SELECT idProcessData FROM ProcessData WHERE idProcesses = %s)"%(processID)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    ProdIDs = []
    for row in res['Value']:
      resdict = {}
      resdict['ProdID']=row[0]
      resdict['Type']=row[1]
      ProdIDs.append(resdict)
    return S_OK(ProdIDs)
  
  def getProductionResults(self, ProdID, Params, connection = False):
    """ Get the Cross Section, etc. for the specified production
    """
    connection = self.__getConnection( connection )
    for param in Params:
      if not param in self.ProcessDataParams:
        return S_ERROR("%s is not valid"%param)

    Params.append('idProcesses')  
    req = 'SELECT %s FROM ProcessData WHERE idProcessData IN (SELECT idProcessData FROM Productions WHERE ProdID=%s)'%(intListToString( Params ), ProdID)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    
    row = res['Value'][0]
    resdict = {}
    count = 0
    for item in row:
      resdict[Params[count]]=item
      count += 1
    req = "SELECT ProcessName,Detail FROM Processes WHERE idProcesses = %s"%(resdict['idProcesses'])
    res = self._query( req, connection )
    if not res['OK']:
      return res
    row = res['Value'][0]
    resdict['ProcessName'] = row[0]
    resdict['Detail'] = row[1]
    req = "SELECT Type FROM Productions WHERE ProdID=%s"%(ProdID)
    res = self._query( req, connection )
    if not res['OK']:
      return res
    row = res['Value'][0]
    resdict['Type'] = row[0]
    resdict['ProdID'] = ProdID
    req = 'SELECT FileName FROM SteeringFiles WHERE idfiles IN (SELECT idfiles FROM SteeringFiles_has_ProcessData WHERE idProcessData IN (SELECT idProcessData FROM Productions WHERE ProdID=%s))'%ProdID
    res = self._query( req, connection )
    if not res['OK']:
      return res
    resdict['SteeringFile'] = []
    for row in res['Value']:
      if len(row):
        resdict['SteeringFile'].append(row[0])
    return S_OK(resdict)
  
  def getTemplate(self, ProcessName, WhizardVersion, connection = False):
    """ Get the template name that describes the process specified in the whizard version specified
    """
    connection = self.__getConnection( connection )
    ##Check that process exists
    Params = ['idProcesses','ProcessName']
    res = self.getProcessInfo(ProcessName, Params, connection)
    if not res['OK']:
      return S_ERROR("Process %s does not exist"%ProcessName)
    processID = res['Value']['idProcesses']
    ##Check software
    Params = ['idSoftware']
    Platform = 'any'
    res = self.getSoftwareParams('Whizard', WhizardVersion, Platform, Params, connection)
    if not res['OK']:
      return res
    whizardID  = res['Value']['idProcesses']
    
    req = "SELECT Template FROM Processes_has_Software WHERE idProcesses = %s \
           AND idSoftware = %s;"% ( processID, whizardID)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    return S_OK(res['Value'][0])
  
  ##################################################################
  # Setter methods
  def addSoftware( self, AppName, AppVersion, Platform, Comment, Path, connection = False ):
    """ Add the specified software if it does not exist
    """
    connection = self.__getConnection( connection )
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if res['OK']:
      return S_ERROR('Application %s, Version %s already defined in the database'%(AppName,AppVersion))
        
    res = self._escapeString( Comment )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Comment" )
    Comment = res['Value']  
    req = "INSERT INTO Software (AppName,AppVersion,Platform,Comment,Path,Defined) VALUES ('%s','%s','%s','%s','%s',UTC_TIMESTAMP());"%(AppName,AppVersion,Platform,Comment,Path)
    res = self._update( req, connection )
    if not res['OK']:
      return res
    return res
  
  def addDependency(self, AppName, AppVersion, DepName, DepVersion, Platform, autodeclare = False, connection = False):
    """ Declare a dependency between 2 softwares 
    """
    connection = self.__getConnection( connection )
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if not res:
      return S_ERROR('Application %s, Version %s not defined in the database'%(AppVersion,AppName))
    appid = res['Value'][0][0]
    res = self._checkSoftware(DepName, DepVersion, Platform, connection)
    if not res:
      if autodeclare:
        return S_ERROR("Auto declare of dependencies not implemented, please unset autodeclare.")
        #res =self.addSoftware(DepName, DepVersion, connection=connection)
        #if not res['OK']:
        #  return S_ERROR("Could not add dependency %s %s" % ( DepName, DepVersion) )
      else:
        return S_ERROR('Dependency %s, Version %s not defined in the database'%(DepVersion,DepName))
    depid = res['Value'][0][0]
    req = "INSERT INTO DependencyRelation (idSoftware,idDependency) VALUES (%s,%s);"%(appid,depid)
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
    return S_OK(res['lastRowId'])
 
  
  def addProcess(self, ProcessName, ProcessDetail, WhizardVers, Template, connection = False):
    """ Declare a new process or if it exists, declare relation to new whizard
    """
    connection = self.__getConnection( connection )
    res = self._escapeString( Template )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Template" )
    Template = res['Value']  
    
    ##Will need the row where the Process is, and create it is needed.
    ProcessID = None
    Params = ['idProcesses','ProcessName','Detail']
    res = self.getProcessInfo(ProcessName, Params, connection)
    if not res['OK']:
      ## Add process in DB
      req = "INSERT INTO Processes (ProcessName,Detail) VALUES ('%s','%s');" % (ProcessName, ProcessDetail)
      res = self._update( req, connection )
      if not res['OK']:
        return res
      #Get the ProcessID: last row inserted
      ProcessID = res['lastRowId']
    else:
      ProcessID = res['Value']['idProcesses']
      ProcessName = res['Value']['ProcessName']
      ProcessDetail = res['Value']['Detail']
        
    ##Get the software ID
    req = "SELECT idSoftware FROM Software WHERE AppName='Whizard' AND AppVersion='%s';"%(WhizardVers)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR('Whizard version %s not found in DB, make sure you declared it'%(WhizardVers))
    row = res['Value'][0]
    SoftwareID = row[0]
    
    req = "INSERT INTO Processes_has_Software (idProcesses,idSoftware,Template) VALUES ( %s, %s, '%s');"% (ProcessID,SoftwareID,Template) 
    res = self._update( req, connection )
    return res
  
  def addCut(self,cutsdict={},connection=False):
    """ Add a set of cut
    """
    connection = self.__getConnection( connection )

  
  def addProductionData(self, ProdDataDict, connection = False):
    """ Declare a new Production
    """
    ProdID = ProdDataDict['ProdID']
    ProdType = ProdDataDict['Type']
    ProcessName = ProdDataDict['Process']
    Path = ProdDataDict['Path']
    AppName = ProdDataDict['AppName']
    AppVersion = ProdDataDict['AppVersion']
    Platform   = ProdDataDict['Platform']
    SteeringFile = None
    if ProdDataDict.has_key('SteeringFile'):
      SteeringFile = ProdDataDict['SteeringFile']
    InheritsFrom = None
    if ProdDataDict.has_key('InheritsFrom'):
      InheritsFrom = ProdDataDict['InheritsFrom']

    if not ProdType in self.ProdTypes:
      return S_ERROR("Production type %s not available"%(ProdType))
    connection = self.__getConnection( connection )
    
    #Get the ProcessID
    Params = ['idProcesses']
    res = self.getProcessInfo(ProcessName, Params, connection)
    if not res['OK']:
      return res
    ProcessID = res['Value']['idProcesses']
    
    ##Get Software ID
    Params = ['idSoftware']
    res = self.getSoftwareParams(AppName, AppVersion, Platform, Params, connection)
    if not res['OK']:
      return res
    SoftwareID = res['Value']['idSoftware']
    
    ##Create the ProcessData
    req = "INSERT INTO ProcessData (idProcesses,Path) VALUES (%s,'%s');" % (ProcessID,Path)
    res = self._update( req, connection )
    if not res['OK']:
      return S_ERROR("Could not insert ProcessData into DB")
    #Get that line's ID
    #req = "SELECT LAST_INSERT_ID();"
    #res = self._query( req, connection )
    #if not res['OK']:
    #  return S_ERROR('Failed to get last insert ID')
    ProcessDataID = res['lastRowId']
    
    #Declare new production
    req = "INSERT INTO Productions (idSoftware,idProcessData,ProdID,Type) VALUES \
           ( %d, %d, %d, '%s');" % (SoftwareID, ProcessDataID, ProdID, ProdType)
    res = self._update( req, connection ) 
    if not res['OK']:
      return res
    prod_insert_ID = res['lastRowId']
    ##In Case there is a steering file
    if SteeringFile:
      req = "SELECT idfiles FROM SteeringFiles WHERE FileName='%s';" % SteeringFile
      res = self._query( req, connection )
      if not len(res['Value']):
        res = self.addSteeringFile( SteeringFile, connection = connection)
      idSteering = res['Value'][0]
      req = "INSERT INTO SteeringFiles_has_ProcessData (idfiles,idProcessData) VALUES ( %s, %s);"% (idSteering,ProcessDataID)
      res = self._update( req, connection )
    if InheritsFrom:
      req = 'INSERT INTO ProductionRelation (idMotherProd,idDaughterProd) VALUES ((SELECT idProduction FROM Productions WHERE ProdID=%s),%s)'%(InheritsFrom,prod_insert_ID)
      res = self._update( req, connection )
      if not res['OK']:
        return res
    return S_OK()  
           
  ########################################################################
  # Update methods
  def updateCrossSection(self, ProcessDict, connection = False):
    """ Update the cross section of the given process in the given production
    """
    connection = self.__getConnection( connection )

    #ProcessName = ProcessDict['ProcessName']
    ProdID = ProcessDict['ProdID']
    AppName = ProcessDict['AppName']
    AppVersion = ProcessDict['AppVersion']
    Platform = ProcessDict['Platform']
    Params = ['idSoftware']
    res = self.getSoftwareParams(AppName, AppVersion, Platform, Params, connection)
    if not res['OK']:
      return res    
    SoftwareID= res['Value']['idSoftware']

    CrossSection = ProcessDict['CrossSection']
    
    req = "SELECT idProcessData,CrossSection,Files FROM ProcessData WHERE \
           idProcessData IN (SELECT idProcessData FROM Productions \
           WHERE ProdID=%s AND idSoftware = %d);"%( ProdID, SoftwareID)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not find matching ProcessData for Production %s" % ProdID)
    for row in res['Value']:
      processDataID,OldCrossSection,OldFiles = row
      if OldFiles == None:
        OldFiles = 0
      temp_crosssection = OldCrossSection*OldFiles
      Files = OldFiles + 1
      CrossSection += temp_crosssection
      CrossSection = CrossSection/Files
      req = "UPDATE ProcessData SET CrossSection=%s,Files=%s WHERE idProcessData=%s;" %( CrossSection, Files, processDataID)
      res = self._update( req, connection ) 
      if not res['OK']:
        return res
    return S_OK()

  def changeSoftwareStatus ( self, AppName, AppVersion, Platform, Comment, Status=False, connection = False ):
    """ Change validity of software
    """
    connection = self.__getConnection( connection )
    res = self._escapeString( Comment )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Comment" )
    Comment = res['Value']      
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if not res['OK']:
      return res
    new_status='FALSE'
    if Status:
      new_status='TRUE'
    req = "UPDATE Software SET Valid=%s,UpdateComment='%s',LastUpdate=UTC_TIMESTAMP() WHERE AppName='%s' AND AppVersion='%s' AND Platform='%s';"%(new_status,Comment,AppName,AppVersion,Platform)
    res = self._update( req, connection )
    if not res['OK']:
      return res
    ##Now update also dependent software (Only to FALSE)
    if not Status:
      req = "SELECT idSoftware FROM DependencyRelation WHERE idDependency = (SELECT idSoftware FROM Software WHERE AppName='%s' AND AppVersion='%s' AND Platform='%s');" % (AppName,AppVersion,Platform)
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
  