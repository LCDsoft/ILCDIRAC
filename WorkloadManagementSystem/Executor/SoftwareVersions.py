""" Executor to check which Sites have the proper software installed

Based on some part of the SoftwarePackage name we ban a lists of sites.
Arbitrary number of BanLists can be created.
In the CS:
BanLists is a list of strings, for each string create two more options.
<string>Reason
<string>Sites
Where Reason is the substring of the softwarePackage that is looked for and Sites is a lists of sites to be banned if the software package includes the substring

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
    cls.__softToBanned = {}
    cls.__lastCacheUpdate = 0
    cls.__cacheLifeTime = 600

    banLists = cls.ex_getOption( 'BanLists', [] )
    cls.log.notice( banLists )

    for banList in banLists:
      resReason = cls.ex_getOption( banList+"Reason", '' )
      resSites = cls.ex_getOption( banList+"Sites", [] )
      cls.__softToBanned[resReason] = resSites

    cls.log.notice( "BanLists:%s " % cls.__softToBanned )

    return S_OK()


  def optimizeJob( self, jid, jobState ):

    result = jobState.getManifest()
    if not result['OK']:
      return S_ERROR( "Could not retrieve manifest: %s" % result[ 'Message' ] )
    manifest = result['Value']

    software = manifest.getOption( "SoftwarePackages" )
    self.log.verbose( "SoftwarePackages: %s " % software )
    if isinstance( software , basestring ):
      software = [ software ]

    if software:
      self.checkSoftware( manifest, software )

    result = jobState.setStatus( "SoftwareCheck",
                                 "Done",
                                 appStatus = "",
                                 source = self.ex_optimizerName() )
    if not result[ 'OK' ]:
      return result

    self.log.verbose( "Done SoftwareVersioning")

    return self.setNextOptimizer( jobState )


  def checkSoftware(self, manifest, software ):
    """check if there are softwarepackages needed for the job and ban all sites
    if there is some prohibitions for that package

    """

    bannedSites = manifest.getOption( "BannedSites", [] )
    if not bannedSites:
      bannedSites = manifest.getOption( "BannedSite", [] )

    self.log.verbose( "Original BannedSites: %s " % bannedSites )

    softBanned = set()
    for reason, sites in self.__softToBanned.iteritems():
      for package in software:
        self.log.verbose( "Checking %s against %s " % ( reason, package ) )
        if reason in package:
          softBanned.update(sites)

    newBannedSites = set(bannedSites).union(softBanned)

    manifest.setOption( "BannedSites" , ", ".join(newBannedSites) )

    self.log.notice( "Updated BannedSites: %s" % ", ".join(newBannedSites) )
    return
