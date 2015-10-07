""" Executor to check which Sites have the proper software installed

I.e. reject OSG sites when desy CVMFS is mandatory
Maybe reject KEK when slc6 is mandatory

"""

__RCSID__ = "$Id$"

from DIRAC import S_OK,S_ERROR

from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor

class SoftwareVersions( OptimizerExecutor ):
  """
  The specific Optimizer must provide the following methods:
  - initializeOptimizer() before each execution cycle
  - optimizeJob() - the main method called for each job
  """

  @classmethod
  def initializeOptimizer( cls ):
    """Initialize specific parameters for SoftwareVersions.
    """
    # cls.failedMinorStatus = cls.ex_getOption( '', 'Input Data Not Available' )
    # #this will ignore failover SE files
    # cls.checkFileMetadata = cls.ex_getOption( 'CheckFileMetadata', True )

    cls.__dataManDict = {}
    cls.__fcDict = {}
    cls.__SEToSiteMap = {}
    cls.__lastCacheUpdate = 0
    cls.__cacheLifeTime = 600

    return S_OK()


  def optimizeJob( self, jid, jobState ):

    software = jobState.getAttribute( "SoftwarePackages" )
    self.log.notice ( "Software: %s ", software )
    softBanned = []

    bannedSites = jobState.getAttribute( "BannedSite" )
    self.log.notice ( "BannedSites: %s ", bannedSites )

    newBannedSites = bannedSites + softBanned

    jobState.setAttribute( "BannedSites" , newBannedSites )

    self.log.notice( " Done SoftwareVersioning ")


    result = jobState.setStatus( "SoftwareCheck",
                                 "Done",
                                 appStatus = "",
                                 source = self.ex_optimizerName() )
    if not result[ 'OK' ]:
      return result

    
    return self.setNextOptimizer()
