""" ProcessDB for ProcessProduction System
"""
__RCSID__ = "$Id$"

from DIRAC                                                             import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
from DIRAC.Core.Utilities.List                                         import intListToString
#pylint: skip-file
class ProcessDB ( DB ):
  """ DB for the ProcessProductionSystem
  """
  def __init__( self ):
    """ 
    """
    self.dbname = 'ProcessDB'
    DB.__init__( self, self.dbname, 'ProcessProduction/ProcessDB' )
    self.ProdTypes = ['MCGeneration', "MCSimulation", "MCReconstruction"]
    self.SoftwareParams = ['Path', 'Valid', 'AppName', 'AppVersion', 'Platform']
    self.ProcessDataParams = ['CrossSection', 'NbEvts', 'Path', 'Files', 'Polarisation']
    
    self.SiteStatuses = ['OK', 'Banned']
    self.Operations = ['Installation', 'Removal']
    self.OperationsStatus = ['Done', 'Running', 'Waiting', 'Failed']      
    
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
                                                          'PrimaryKey' : ['idFile','idProcessData'],
                                                          'Indexes' :{'fk_SteeringFiles_has_ProcessData_ProcessData1':['idProcessData'],
                                                                      'fk_SteeringFiles_has_ProcessData_SteeringFiles1':['idFile']},
                                                          'ForeignKeys': { #'idFile' : 'SteeringFiles.idFile',
                                                                          'idProcessData':'ProcessData.idProcessData'}
                                                         }
    if not 'DependencyRelation' in tablesInDB:
      tablesToCreate['DependencyRelation'] = { 'Fields' : { 'idSoftware' : 'INT NOT NULL',
                                                            'idDependency' : 'INT NOT NULL',
                                                            'idDependencyRelation' : 'INT NOT NULL AUTO_INCREMENT'
                                                           },
                                               'PrimaryKey' : 'idDependencyRelation',
                                               'Indexes' : {'fk_Software_has_Software_Software2':['idDependency'],
                                                            'fk_Software_has_Software_Software1': ['idSoftware']}
                                               #'ForeignKeys': {'idSoftware':'Software.idSoftware','idDependency':'Software.idSoftware'}             
                                              }
    
    if not 'ProductionRelation' in tablesInDB:
      tablesToCreate['ProductionRelation'] = { 'Fields' : { 'idRelation':'INT NOT NULL AUTO_INCREMENT',
                                                            'idMotherProd' : 'INT NOT NULL',
                                                            'idDaughterProd' : 'INT NOT NULL'
                                                           },
                                               'PrimaryKey': 'idRelation',
                                               'Indexes' : {'Daughter':['idDaughterProd'],'Mother':['idMotherProd']}
                                               #'ForeignKeys': { 'idMotherProd':"Productions.idProduction", 
                                               #                "idDaughterProd":"Productions.idProduction"}
                                              }
    if not 'Sites' in tablesInDB:
      tablesToCreate['Sites'] = { 'Fields' : { 'idSite' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                               'SiteName' : 'VARCHAR(32) NOT NULL',
                                               'Status' : 'ENUM("OK","Banned") DEFAULT "OK"'
                                              } ,
                                  'PrimaryKey': 'idSite',
                                  'Indexes': { 'SiteName': ['SiteName'],
                                               'Status': ['Status']
                                               }
                                  }
      
    if not "ApplicationStatusAtSite" in tablesInDB:
      tablesToCreate['ApplicationStatusAtSite']  = { 'Fields' : { 'idStatus' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                                  'idSoftware': 'INT NOT NULL',
                                                                  'idSite' : 'INTEGER UNSIGNED NOT NULL',
                                                                  'Status' : 'ENUM("NotAvailable","Installed","Installing") DEFAULT "NotAvailable"'
                                                                 },
                                                    'ForeignKeys': {'idSoftware':'Software.idSoftware',
                                                                    'idSite':'Sites.idSite'},             
                                                    'PrimaryKey' : 'idStatus',
                                                    'Indexes': { 'Status' : ['Status'],
                                                                'idSoftware' : ['idSoftware'],
                                                                'idSite' : ['idSite']
                                                                }
                                                    }
    if not "SoftwareOperations" in tablesInDB:
      tablesToCreate['SoftwareOperations']  = { 'Fields' : { 'OpID': 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                     'JobID' : 'INTEGER UNSIGNED NOT NULL',
                                                     'idSoftware' : 'INT NOT NULL',
                                                     'idSite' : 'INTEGER UNSIGNED NOT NULL',
                                                     'Operation' : 'ENUM("Installation","Removal") DEFAULT "Installation"',
                                                     'Status' : 'ENUM("Done","Waiting","Failed","Running") DEFAULT "Waiting"'
                                                    },
                                        'ForeignKeys': {'idSoftware':'Software.idSoftware',
                                                        'idSite':'Sites.idSite'},    
                                        'PrimaryKey' : 'OpID',
                                        'Indexes': { 'Status' : ['Status'],
                                                     'Operation' : ['Operation'],
                                                     'idSite'  : ['idSite'],
                                                     'JobID' : ['JobID'],
                                                     'idSoftware' : ['idSoftware']
                                                     }
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
    fields = ['AppName','AppVersion']
    values = [AppName, AppVersion]
    #extrareqs = ''
    if not Platform == 'any':
      #extrareqs = " AND Platform='%s'"%Platform
      fields.append('Platform')
      values.append(Platform)
      
    #req = "SELECT idSoftware FROM Software WHERE AppName='%s' AND AppVersion='%s' %s;"%(AppName,AppVersion, extrareqs)
    #res = self._query( req, connection )
    res = self._getFields('Software', ['idSoftware'], fields, values, conn = connection)
    if not res['OK']:
      return S_ERROR("Could not get software")
    if len(res['Value']):
      return res
    else:
      return S_ERROR("Could not find any software %s %s" % (AppName, AppVersion))
    
  def getSoftwareParams(self, AppName, AppVersion, Platform, Params, connection = False):    
    """ Check status of given software
    """
    for param in Params:
      if not param in self.SoftwareParams:
        return S_ERROR("Parameter %s is not valid" % param)
    connection = self.__getConnection( connection )
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if not res['OK']:
      return res
    idSoftware = res['Value'][0][0] #This is the first entry of the first tuple
    #extrareqs = ''
    #if not Platform == 'any':
    #  extrareqs = " AND Platform='%s'"%Platform
    
    #req = "SELECT %s FROM Software WHERE idSoftware= %s"%(intListToString( Params ),AppName,AppVersion,extrareqs)
    #res = self._query( req, connection )
    res = self._getFields('Software', Params, ['idSoftware'], [idSoftware], conn = connection)
    if not res['OK']:
      return res
    reslist = []  
    for row in res['Value']:
      resdict = {}
      count = 0
      for item in row:
        resdict[Params[count]] = item
        count += 1
      reslist.append(resdict)
    return S_OK(reslist)
  
  def getSoftwares(self, connection = False):
    """ Return the list of softwares/version available, and valid
    """
    connection = self.__getConnection( connection )
    req = "SELECT idSoftware,AppName,AppVersion,Comment FROM Software WHERE Valid = TRUE;"
    res = self._query( req, connection )
    if not res['OK']:
      return res
    apps = {}
    for idSoftware, AppName, AppVersion, Comment in res['Value']:
      app = {}
      app['Name'] = AppName
      app['Version'] = AppVersion
      app['Comment'] = Comment
      ##Check for dependency
      #req = "SELECT idDependency FROM DependencyRelation WHERE idSoftware=%s;"%(idSoftware)
      #res = self._query( req, connection )
      res = self._getFields('DependencyRelation', ['idDependency'], ['idSoftware'], [idSoftware], conn = connection )
      depid = 0
      if not len(res['Value']):
        depid = 0
      else:
        depid = res['Value'][0]
      app['Dependency'] = depid
      apps[idSoftware] = app
      
    return S_OK(apps)  
      
  def getProcessInfo(self, ProcessName, Params, connection = False):
    """ Get the process info 
    """
    connection = self.__getConnection( connection )
    #req = "SELECT %s FROM Processes WHERE ProcessName='%s';"%(intListToString( Params ), ProcessName)
    #res =  self._query( req, connection )
    res = self._getFields('Processes', Params, ['ProcessName'], [ProcessName], conn = connection)
    if not res['OK']:
      return res
    row = res['Value'][0]
    resdict = {}
    count = 0
    for item in row:
      resdict[Params[count]] = item
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
    req = "SELECT ProdID,Type FROM Production WHERE idProcessData IN (SELECT idProcessData FROM ProcessData WHERE idProcesses = %s);" % (processID)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    ProdIDs = []
    for row in res['Value']:
      resdict = {}
      resdict['ProdID'] = row[0]
      resdict['Type'] = row[1]
      ProdIDs.append(resdict)
    return S_OK(ProdIDs)
  
  def getProductionResults(self, ProdID, Params, connection = False):
    """ Get the Cross Section, etc. for the specified production
    """
    connection = self.__getConnection( connection )
    for param in Params:
      if not param in self.ProcessDataParams:
        return S_ERROR("%s is not valid" % param)

    Params.append('idProcesses')  
    req = 'SELECT %s FROM ProcessData WHERE idProcessData IN (SELECT idProcessData FROM Productions WHERE ProdID=%s);' % (intListToString( Params ), ProdID)
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    
    row = res['Value'][0]
    resdict = {}
    count = 0
    for item in row:
      resdict[Params[count]] = item
      count += 1
    #req = "SELECT ProcessName,Detail FROM Processes WHERE idProcesses = %s"%(resdict['idProcesses'])
    #res = self._query( req, connection )
    res = self._getFields('Processes', ['ProcessName', 'Detail'], ['idProcesses'], [resdict['idProcesses']], conn = connection)
    if not res['OK']:
      return res
    row = res['Value'][0]
    resdict['ProcessName'] = row[0]
    resdict['Detail'] = row[1]
    #req = "SELECT Type FROM Productions WHERE ProdID=%s"%(ProdID)
    #res = self._query( req, connection )
    res = self._getFields('Productions', ['Type'], ['ProdID'], [ProdID], conn = connection )
    if not res['OK']:
      return res
    row = res['Value'][0]
    resdict['Type'] = row[0]
    resdict['ProdID'] = ProdID
    req = 'SELECT FileName FROM SteeringFiles WHERE idfiles IN (SELECT idfiles FROM SteeringFiles_has_ProcessData \
           WHERE idProcessData IN (SELECT idProcessData FROM Productions WHERE ProdID=%s));' % ProdID
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
      return S_ERROR("Process %s does not exist" % ProcessName)
    processID = res['Value']['idProcesses']
    ##Check software
    Params = ['idSoftware']
    Platform = 'any'
    res = self.getSoftwareParams('Whizard', WhizardVersion, Platform, Params, connection)
    if not res['OK']:
      return res
    whizardID = res['Value']['idProcesses']
    
    #req = "SELECT Template FROM Processes_has_Software WHERE idProcesses = %s \
    #       AND idSoftware = %s;"% ( processID, whizardID)
    #res =  self._query( req, connection )
    res = self._getFields('Processes_has_Software', ['Template'], ['idProcesses','idSoftware'], [processID, whizardID], conn = connection)
    if not res['OK']:
      return res
    return S_OK(res['Value'][0])
 
  def getInstallSoftwareTask(self, connection = None):
    """ Find a task to perform: install new software.
    """
    connection = self.__getConnection( connection )
    
    #now get what is already available at each site
    req = "SELECT idSoftware,idSite FROM ApplicationStatusAtSite WHERE idSoftware IN (SELECT idSoftware FROM Software WHERE Valid=TRUE) \
           AND idSite IN (SELECT idSite FROM Sites WHERE Status='OK') AND ApplicationStatusAtSite.Status='NotAvailable';"
    res =  self._query( req, connection )
    if not res['OK']:
      return res
    rows = res['Value']
    soft_dict = {}
    for row in rows:
      if not soft_dict.has_key(row[0]): 
        soft_dict[row[0]] = {}
        res  = self._getFields('Software', ['AppName', 'AppVersion', 'Platform'],
                               ['idSoftware'], [row[0]], conn = connection)
        soft_dict[row[0]]['AppName'], soft_dict[row[0]]['AppVersion'], soft_dict[row[0]]['Platform'] = res['Value'][0]
      if not   soft_dict[row[0]].has_key('Sites'):
        soft_dict[row[0]]['Sites'] = []
      res =   self._getFields('Sites', ['SiteName'], ['idSite'], [row[1]], conn = connection )
      if len(res['Value']):
        sitename = res['Value'][0][0]
        soft_dict[row[0]]['Sites'].append(sitename)    
          
    return S_OK(soft_dict)
  
  def getJobs(self, connection = None):
    """ Return list of JobIDs for update
    """
    connection = self.__getConnection( connection )
  
    res = self._getFields('SoftwareOperations', ['JobID', 'idSoftware', 'Site'], [], [], conn = connection)
    rows = res['Value']

    resjobs = []
    
    for row in rows:
      jobdict = {}
      jobdict['JobID'] = row[0]
      res = self._getFields('Software', ['AppName', 'AppVersion', 'Platform'], ['idSoftware'], [row[1]], conn = connection)
      jobdict['Site'] = row[2]
      row = res['Value'][0]
      if len(row):
        jobdict['AppName'] = row[0]
        jobdict['AppVersion'] = row[1]
        jobdict['Platform'] = row[2]
        resjobs.append(jobdict)
        
    return S_OK(resjobs)
  ##################################################################
  # Setter methods
  def addSoftware( self, AppName, AppVersion, Platform, Comment, Path, connection = False ):
    """ Add the specified software if it does not exist
    """
    connection = self.__getConnection( connection )
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if res['OK']:
      return S_ERROR('Application %s, Version %s already defined in the database' % (AppName, AppVersion))
        
    res = self._escapeString( Comment )
    if not res['OK']:
      return S_ERROR( "Failed to parse the Comment" )
    Comment = res['Value']  
    #req = "INSERT INTO Software (AppName,AppVersion,Platform,Comment,Path,Defined) VALUES ('%s','%s','%s','%s','%s',UTC_TIMESTAMP());"%(AppName,AppVersion,Platform,Comment,Path)
    #res = self._update( req, connection )
    res = self._insert('Software', ['AppName', 'AppVersion', 'Platform', 'Comment', 'Path', 'Defined'],
                       [AppName, AppVersion, Platform, Comment, Path, 'UTC_TIMESTAMP()'], connection)
    if not res['OK']:
      return res
    idsoft = res['lastRowId']
    
    ##getSites
    res = self._getFields("Sites", ["idSite"], [], [], conn = connection)
    if not len(res['Value']):
      return S_OK({"Message" : "Could not get sites"})
    rows = res['Value']
    for row in rows:
      res = self._insert("ApplicationStatusAtSite", ['idSoftware', 'idSite'], [idsoft, row[0]], connection)

    ##ApplicationStatusAtSite
    
    return res
  
  def addDependency(self, AppName, AppVersion, DepName, DepVersion, Platform, autodeclare = False, connection = False):
    """ Declare a dependency between 2 softwares 
    """
    connection = self.__getConnection( connection )
    res = self._checkSoftware(AppName, AppVersion, Platform, connection)
    if not res:
      return S_ERROR('Application %s, Version %s not defined in the database' % (AppVersion, AppName))
    appid = res['Value'][0][0]
    res = self._checkSoftware(DepName, DepVersion, Platform, connection)
    if not res:
      if autodeclare:
        return S_ERROR("Auto declare of dependencies not implemented, please unset autodeclare.")
        #res =self.addSoftware(DepName, DepVersion, connection=connection)
        #if not res['OK']:
        #  return S_ERROR("Could not add dependency %s %s" % ( DepName, DepVersion) )
      else:
        return S_ERROR('Dependency %s, Version %s not defined in the database' % (DepVersion, DepName))
    depid = res['Value'][0][0]
    #req = "INSERT INTO DependencyRelation (idSoftware,idDependency) VALUES (%s,%s);"%(appid,depid)
    #res = self._update( req, connection )
    res = self._insert('DependencyRelation', ['idSoftware', 'idDependency'], [appid, depid], connection)
    if not res['OK']:
      return res
    return res  
  
  def addSteeringFile(self, FileName, Path = None, connection = False):
    """ Add a new steering file
    """
    connection = self.__getConnection( connection )
    #req = "INSERT INTO SteeringFiles (FileName) VALUES ('%s');" % FileName
    #res = self._update( req, connection )
    res = self._insert('SteeringFiles', ['FileName'], [FileName], connection)
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
    Params = ['idProcesses', 'ProcessName', 'Detail']
    res = self.getProcessInfo(ProcessName, Params, connection)
    if not res['OK']:
      ## Add process in DB
      #req = "INSERT INTO Processes (ProcessName,Detail) VALUES ('%s','%s');" % (ProcessName, ProcessDetail)
      res = self._insert('Processes', ['ProcessName', 'Detail'], [ProcessName, ProcessDetail], connection)
      #res = self._update( req, connection )
      if not res['OK']:
        return res
      #Get the ProcessID: last row inserted
      ProcessID = res['lastRowId']
    else:
      ProcessID = res['Value']['idProcesses']
      ProcessName = res['Value']['ProcessName']
      ProcessDetail = res['Value']['Detail']
        
    ##Get the software ID
    #req = "SELECT idSoftware FROM Software WHERE AppName='Whizard' AND AppVersion='%s';"%(WhizardVers)
    #res = self._query( req, connection )
    res = self._getFields('Software', ['idSoftware'], ['AppName', 'AppVersion'],
                          ['Whizard', WhizardVers], conn = connection )
    if not res['OK']:
      return S_ERROR('Whizard version %s not found in DB, make sure you declared it' % (WhizardVers))
    row = res['Value'][0]
    SoftwareID = row[0]
    
    #req = "INSERT INTO Processes_has_Software (idProcesses,idSoftware,Template) VALUES ( %s, %s, '%s');"% (ProcessID,SoftwareID,Template) 
    #res = self._update( req, connection )
    res = self._insert('Processes_has_Software', ['idProcesses', 'idSoftware', 'Template'], [ProcessID, SoftwareID, Template], connection)
    return res

  def addSite(self, siteName, connection = False ):
    """ Add a new site
    """
    connection = self.__getConnection( connection )
    res = self._getFields('Sites', ['idSite'], ['SiteName'], [siteName], conn = connection)
    if not len(res['Value']):
      res = self._insert('Sites', ['SiteName'], [siteName], connection)
      idSite = res['lastRowId']
      res = self._getFields("Software", ['idSoftware'], [], [], conn = connection)
      for idsoft in [t[0] for t in res['Value']] :
        res = self._insert('ApplicationStatusAtSite', ['idSite', 'idSoftware'], [idSite, idsoft], connection)
    return S_OK()

  def addOrUpdateJob(self, jobdict, connection = False ):
    """ Add a new job: operation 
    """
    connection = self.__getConnection( connection )
    
    jobkeys = ['Status', 'JobID', 'Site', 'AppName', 'AppVersion', 'Platform']
    for key in jobkeys:
      if not jobdict.has_key(key):
        return S_ERROR("Missing mandatory parameter %s" % key)
      
    softid = 0  
    res = self._checkSoftware(jobdict['AppName'], jobdict['AppVersion'], jobdict['Platform'], connection)  
    if res['OK']:
      if len(res['Value']):
        softid = res['Value'][0][0]
    siteid = 0    
    res = self._getFields('idSite', 'Sites', ['SiteName'], [jobdict['Site']], conn = connection)
    if len(res['Value']):
      siteid = res['Value'][0][0]    
    if not siteid or not softid:
      return S_ERROR("Could not find either site or software")  
    #Check that job is new or not
    res = self._getFields('SoftwareOperations', ['OpID'], ['JobID'], [jobdict['JobID']], conn = connection)
    if not res['OK']:
      return res
    if len(res['Value']):
      opID = res['Value'][0][0] 
      status = jobdict['Status']
      if not status in self.OperationsStatus:
        status = 'Waiting'
      if not status == 'Failed' and not status == 'Done':   
        req = "UPDATE SoftwareOperations SET Status='%s' WHERE OpID=%s;" % (status, opID)
        res = self._update( req, connection )
        if not status == 'Running':
          req = 'UPDATE ApplicationStatusAtSite SET Status="Installing" WHERE idSoftware=%s AND idSite=%s;' % (softid, siteid)
          res = self._update( req, connection )
      elif status == 'Done' or status == 'Failed':
        res = self._removeJob(opID, connection)
    else:
      #when new
      if jobdict.has_key('Operation'):
        if not jobdict['Operation'] in self.Operations:
          return S_ERROR("Operation %s is not supported" % jobdict['Operation'])
      res = self._insert('SoftwareOperations', ['JobID', 'idSoftware', 'idSite'], [jobdict['JobID'], softid, siteid], connection)
    return res
  
  def _removeJob(self, opID, connection):
    connection = self.__getConnection( connection )    
    req = 'DELETE FROM SoftwareOperations WHERE OpID=%s;' % (opID)
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
    Platform   = ProdDataDict['Platform']
    SteeringFile = None
    if ProdDataDict.has_key('SteeringFile'):
      SteeringFile = ProdDataDict['SteeringFile']
    InheritsFrom = None
    if ProdDataDict.has_key('InheritsFrom'):
      InheritsFrom = ProdDataDict['InheritsFrom']

    if not ProdType in self.ProdTypes:
      return S_ERROR("Production type %s not available" % (ProdType))
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
    #req = "INSERT INTO ProcessData (idProcesses,Path) VALUES (%s,'%s');" % (ProcessID,Path)
    #res = self._update( req, connection )
    res = self._insert('ProcessData', ['idProcesses', 'Path'], [ProcessID, Path], connection)
    if not res['OK']:
      return S_ERROR("Could not insert ProcessData into DB")
    #Get that line's ID
    #req = "SELECT LAST_INSERT_ID();"
    #res = self._query( req, connection )
    #if not res['OK']:
    #  return S_ERROR('Failed to get last insert ID')
    ProcessDataID = res['lastRowId']
    
    #Declare new production
    #req = "INSERT INTO Productions (idSoftware,idProcessData,ProdID,Type) VALUES \
    #       ( %d, %d, %d, '%s');" % (SoftwareID, ProcessDataID, ProdID, ProdType)
    #res = self._update( req, connection ) 
    res = self._insert('Productions', ['idSoftware', 'idProcessData', 'ProdID', 'Type'],
                       [SoftwareID, ProcessDataID, ProdID, ProdType], connection)
    if not res['OK']:
      return res
    prod_insert_ID = res['lastRowId']
    ##In Case there is a steering file
    if SteeringFile:
      #req = "SELECT idfiles FROM SteeringFiles WHERE FileName='%s';" % SteeringFile
      #res = self._query( req, connection )
      res = self._getFields('SteeringFiles', ['idfiles'], ['FileName'], [SteeringFile], conn = connection )
      if not len(res['Value']):
        res = self.addSteeringFile( SteeringFile, connection = connection)
      idSteering = res['Value'][0]
      #req = "INSERT INTO SteeringFiles_has_ProcessData (idfiles,idProcessData) VALUES ( %s, %s);"% (idSteering,ProcessDataID)
      #res = self._update( req, connection )
      res = self._insert('SteeringFiles_has_ProcessData', ['idfiles', 'idProcessData'], [idSteering, ProcessDataID], connection)
    if InheritsFrom:
      req = 'INSERT INTO ProductionRelation (idMotherProd,idDaughterProd) VALUES ((SELECT idProduction FROM Productions WHERE ProdID=%s),%s);' % (InheritsFrom, prod_insert_ID)
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
    SoftwareID = res['Value']['idSoftware']

    CrossSection = ProcessDict['CrossSection']
    
    req = "SELECT idProcessData,CrossSection,Files FROM ProcessData WHERE \
           idProcessData IN (SELECT idProcessData FROM Productions \
           WHERE ProdID=%s AND idSoftware = %d);" % ( ProdID, SoftwareID)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not find matching ProcessData for Production %s" % ProdID)
    for row in res['Value']:
      processDataID, OldCrossSection, OldFiles = row
      if OldFiles == None:
        OldFiles = 0
      temp_crosssection = OldCrossSection*OldFiles
      Files = OldFiles + 1
      CrossSection += temp_crosssection
      CrossSection = CrossSection/Files
      req = "UPDATE ProcessData SET CrossSection=%s,Files=%s WHERE idProcessData=%s;" % ( CrossSection, Files, processDataID)
      res = self._update( req, connection ) 
      if not res['OK']:
        return res
    return S_OK()

  def changeSoftwareStatus ( self, AppName, AppVersion, Platform, Comment, Status = False, connection = False ):
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
    idsoft = res['Value'][0][0]
    new_status = 'FALSE'
    if Status:
      new_status = 'TRUE'
    req = "UPDATE Software SET Valid=%s,UpdateComment='%s',LastUpdate=UTC_TIMESTAMP() WHERE idSoftware=%s;" % (new_status, 
                                                                                                               Comment, 
                                                                                                               idsoft)
    res = self._update( req, connection )
    if not res['OK']:
      return res
    ##Now update also dependent software (Only to FALSE)
    if not Status:
      req = "SELECT idSoftware FROM DependencyRelation WHERE idDependency = %s;" % (idsoft)
      res = self._query( req, connection )
      if not res['OK'] or not len(res['Value']):
        return S_ERROR('Could not find any dependency')
      for sid in [t[0] for t in res['Value']] :
        req = "UPDATE Software SET Valid=FALSE,UpdateComment='Dependency inheritance',LastUpdate=UTC_TIMESTAMP() WHERE idSoftware = %s;" % sid
        res = self._update( req, connection )
        if not res['OK']:
          return res
    return res

  def changeSiteStatus(self, sitedict, connection = False ):
    """ Mark site as banned or active
    """
    connection = self.__getConnection( connection )
    if not sitedict.has_key('Status') or not sitedict.has_key('SiteName'):
      return S_ERROR("Missing mandatory key Status or SiteDict")
    if not sitedict['Status'] in self.SiteStatuses:
      return S_ERROR("Status %s is not a valid site status" % sitedict['Status'])
    
    res = self._getFields('Sites', ['SiteName'], ['SiteName'], [sitedict['SiteName']], conn = connection)
    rows = res['Value']
    if not len(rows):
      res = self.addSite(sitedict['SiteName'], connection)
      if not res['OK']:
        return res
    
    query = 'UPDATE Sites SET Status="%s" WHERE SiteName="%s";' % (sitedict['Status'], sitedict['SiteName'])
    res = self._update(query, connection)
    if not res['OK']:
      return res
    return S_OK()
  
  def reportOK(self, jobdict, connection = False ):
    """ Report if application is OK to use or not at a given site
    """
    connection = self.__getConnection( connection )
    if not jobdict.has_key('JobID') or not jobdict.has_key('AppName') or not jobdict.has_key('AppVersion') or not jobdict.has_key('Platform'):
      return S_ERROR("Missing key")

    statusdict = {}
    statusdict['Status'] = True
    statusdict.update(jobdict)
    res = self._updateStatus(statusdict)
    if not res['OK']:
      return res    
    
    return S_OK()
  
  def reportFailed(self, jobdict, connection = False ):
    """ Report that application installation failed at the given site
    """
    connection = self.__getConnection( connection )
    if not jobdict.has_key('JobID') or not jobdict.has_key('AppName') or not jobdict.has_key('AppVersion') or not jobdict.has_key('Platform'):
      return S_ERROR("Missing key")
    statusdict = {}
    statusdict['Status'] = False
    statusdict.update(jobdict)
    res = self._updateStatus(statusdict)
    if not res['OK']:
      return res
    return S_OK()
  
  def _updateStatus(self, statusdict, connection = False ):
    """ Touch the DB
    """
    connection = self.__getConnection( connection )        
    
    softid = 0  
    res = self._checkSoftware(statusdict['AppName'], statusdict['AppVersion'], statusdict['Platform'], connection)  
    if res['OK']:
      if len(res['Value']):
        softid = res['Value'][0][0]
            
    siteid = 0    
    res = self._getFields('idSite', 'SoftwareOperations', ['JobID'], [statusdict['JobID']], conn = connection)    
    if len(res['Value']):
      siteid = res['Value'][0][0]    
    if not siteid or not softid:
      return S_ERROR("Could not find either site or software")  
    
    if statusdict['Status']:
      req = 'UPDATE ApplicationStatusAtSite SET Status="Installed" WHERE idSite=%s AND idSoftware=%s;' % (siteid, softid)
      res = self._update( req, connection )
    else:
      req = 'UPDATE ApplicationStatusAtSite SET Status="NotAvailable" WHERE idSite=%s AND idSoftware=%s;' % (siteid, softid)
      res = self._update( req, connection )
    return S_OK()
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
  