""" DB for Overlay System
"""
__RCSID__ = None

from DIRAC                                                             import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

class OverlayDB ( DB ):
  """ DB for OverlaySystem
  """
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    self.ops = Operations()
    self.dbname = 'OverlayDB'
    self.logger = gLogger.getSubLogger('OverlayDB')
    DB.__init__( self, self.dbname, 'Overlay/OverlayDB', maxQueueSize  )
    self._createTables( { "OverlayData" : { 'Fields' : { 'Site' : "VARCHAR(255) UNIQUE NOT NULL",
                                                         'NumberOfJobs' : "INTEGER DEFAULT 0"
                                                       },
                                            'PrimaryKey' : 'Site',
                                            'Indexes': {'Index':['Site']}
                                          }
                        }
                      )
    limits = self.ops.getValue("/Overlay/MaxConcurrentRunning", 200)
    self.limits = {}
    self.limits["default"] = limits
    res = self.ops.getSections("/Overlay/Sites/")
    sites = []
    if res['OK']:
      sites = res['Value']
    for tempsite in sites:
      res = self.ops.getValue("/Overlay/Sites/%s/MaxConcurrentRunning" % tempsite, 200)
      self.limits[tempsite] = res
    self.logger.info("Using the following restrictions : %s" % self.limits)

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
  
  def _checkSite(self, site, connection = False ):
    """ Check the number of jobs running at a given site.
    """
    connection = self.__getConnection( connection )
    
    req = "SELECT NumberOfJobs FROM OverlayData WHERE Site='%s';" % (site)
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not get site")
    if len(res['Value']):
      return res
    else:
      return S_ERROR("Could not find any site %s"%(site))
    
  def _addSite(self, site, connection = False ):
    """ Add a new site to the DB
    """ 
    connection = self.__getConnection( connection )
    req = "INSERT INTO OverlayData (Site,NumberOfJobs) VALUES ('%s',1);" % site
    res = self._update( req, connection )
    if not res['OK']:
      return res
    return res

  def _limitForSite(self, site):
    """ Get the current limit of jobs for a given site.
    """
    if site in self.limits.keys():
      return self.limits[site]   
    return self.limits['default']

  def _addNewJob(self, site, nbjobs, connection = False ):
    """ Add a new running job in the DB
    """
    connection = self.__getConnection( connection )
    nbjobs += 1  
    req = "UPDATE OverlayData SET NumberOfJobs=%s WHERE Site='%s';" % (nbjobs, site)
    self._update( req, connection )
    return S_OK()

### Methods to fix the site
  def getSites(self, connection = False):
    """ Return the list of sites known to the service
    """
    connection = self.__getConnection( connection )
    req = 'SELECT Site From OverlayData;'
    res = self._query( req, connection )
    if not res['OK']:
      return S_ERROR("Could not get sites")
    sites = []
    for row in res['Value']:
      sites.append(row[0])
    return S_OK(sites)

  def setJobsAtSites(self, sitedict, connection = False):
    """ As name suggests: set the number of jobs running at the site.
    """
    connection = self.__getConnection( connection )
    for site, nbjobs in sitedict.items():
      req = "UPDATE OverlayData SET NumberOfJobs=%i WHERE Site='%s';" % (int(nbjobs), site)
      res = self._update( req, connection )
      if not res['OK']:
        return S_ERROR("Could not set number of jobs at site %s" % site)
      
    return S_OK()
### Useful methods for the users
  
  def getJobsAtSite(self, site, connection = False ):
    """ Get the number of jobs currently run
    """
    connection = self.__getConnection( connection )   
    nbjobs = 0
    res = self._checkSite(site, connection)
    if not res['OK']:
      return S_OK(nbjobs)
    nbjobs = res['Value'][0][0]
    return S_OK(nbjobs)

### Important methods
  
  def canRun(self, site, connection = False ):
    """ Can the job run at that site?
    """
    connection = self.__getConnection( connection )
    res = self._checkSite(site, connection)
    nbjobs = 0
    if not res['OK']:
      self._addSite(site, connection)
      nbjobs = 1
    else:
      nbjobs = res['Value'][0][0]
    if nbjobs < self._limitForSite(site):
      res = self._addNewJob(site, nbjobs, connection)
      if not res['OK']:
        return res
      return S_OK(True)
    else:
      return S_OK(False)
  
  def jobDone(self, site, connection = False ):
    """ Remove a job from the DB, should not remove a job from the DB 
        if the Site does not exist, but this should never happen
    """
    connection = self.__getConnection( connection )
    res = self._checkSite(site, connection)
    if not res['OK']:
      return res
    nbjobs = res['Value'][0][0]
    if nbjobs == 1:
      return S_OK()
    nbjobs -= 1
    req = "UPDATE OverlayData SET NumberOfJobs=%s WHERE Site='%s';" % (nbjobs, site)
    res = self._update( req, connection )
    if not res['OK']:
      return res   
    return S_OK()    
  