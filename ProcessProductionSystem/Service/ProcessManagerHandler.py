###########################################################################
# $HeadURL: $
###########################################################################

""" Services for ProcessProduction System
"""
__RCSID__ = " $Id: $ "

from DIRAC                                              import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from types import StringTypes, ListType, TupleType, LongType, IntType, DictType, BooleanType

from ILCDIRAC.ProcessProductionSystem.DB.ProcessDB import ProcessDB
#pylint: skip-file
# This is a global instance of the ProcessDB class
processDB = False

def initializeProcessManagerHandler( serviceInfo ):

  global processDB
  processDB = ProcessDB()
  return S_OK()

class ProcessManagerHandler(RequestHandler):
######################################################################
#               Get methods
######################################################################
  types_getSoftwares = []
  def export_getSoftwares(self):
    """ Get all software as a dictionnary
    """
    return processDB.getSoftwares()
  
  types_getProcessInfo = [StringTypes, [ListType, TupleType]]
  def export_getProcessInfo(self, ProcessName, Params ):
    """Get info for a given process
    """
    return processDB.getProcessInfo(ProcessName, Params )
  
  types_getProductionDetails = [[LongType, IntType], [ListType, TupleType]]
  def export_getProductionDetails(self, ProdID, Params):
    """ Get the details of a given production
    """
    return processDB.getProductionResults(ProdID, Params)
  
  types_getTemplate = [StringTypes, StringTypes]
  def export_getTemplate(self, ProcessName, WhizVersion):
    """ Get the proper template
    """
    return processDB.getTemplate(ProcessName, WhizVersion)

  types_getSoftwareParams = [StringTypes, StringTypes, StringTypes, [ListType, TupleType]]
  def export_getSoftwareParams(self, AppName, AppVersion, Platform, Params):
    """ Get the given software status
    """
    return processDB.getSoftwareParams(AppName, AppVersion, Platform, Params)
  
  types_getInstallSoftwareTask = []
  def export_getInstallSoftwareTask(self):
    """ Obtain a new task: when new software is installed. it's needed to install it everywhere.
    """
    return processDB.getInstallSoftwareTask()
#######################################################################
#              Add methods
#######################################################################
  types_addSoftware = [ StringTypes, StringTypes, StringTypes, StringTypes, StringTypes]
  def export_addSoftware(self, AppName, AppVersion, Platform, Comment, Path):
    """ Add new software in the DB
    """
    return processDB.addSoftware(AppName, AppVersion, Platform, Comment, Path)
    
  types_addDependency = [StringTypes, StringTypes, StringTypes, StringTypes, StringTypes]
  def export_addDependency(self, AppName, AppVersion, DepName, DepVersion, Platform):
    """ Add a dependency between softwares
    """
    return processDB.addDependency(AppName, AppVersion, DepName, DepVersion, Platform)

  types_addProcess = [StringTypes, StringTypes, StringTypes, StringTypes]
  def export_addProcess(self, ProcessName, ProcessDetail, WhizardVers, Template):
    """ Add a new process
    """
    return processDB.addProcess(ProcessName, ProcessDetail, WhizardVers, Template)
 
  types_addSteeringFile = [StringTypes, StringTypes]
  def export_addSteeringFile(self, FileName, Path = ''):
    """ Declare a steering file
    """
    return processDB.addSteeringFile(FileName, Path)
  
  types_addProductionData = [DictType]
  def export_addProductionData(self, ProdDataDict):
    """ Add a new Production data object
    """
    if (not ProdDataDict.has_key("ProdID") 
        or not ProdDataDict.has_key("Process") 
        or not ProdDataDict.has_key("Path") 
        or not ProdDataDict.has_key("AppName")
        or not ProdDataDict.has_key("AppVersion")
        or not ProdDataDict.has_key('Platform')
        ):
      return S_ERROR('Incorrect dictionary structure')
    return processDB.addProductionData(ProdDataDict)
  
  types_addsite = [StringTypes]
  def export_addSite(self, sitename):
    """ Add a site
    """
    return processDB.addSite(sitename)
  
  types_addOrUpdateJob = [DictType]
  def export_addOrUpdateJob(self, jobdict):
    """ Add a job
    """
    return processDB.addOrUpdateJob(jobdict)
#######################################################################
#              Change methods
#######################################################################
  types_updateCrossSection = [DictType]
  def export_updateCrossSection(self, ProcessDict):
    """ Update the cross section for the given process and software version
    """
    if not (ProcessDict.has_key('ProdID') and ProcessDict.has_key('AppName') and ProcessDict.has_key('CrossSection')):
      return S_ERROR("Missing essential dictionary info")
    return processDB.updateCrossSection(ProcessDict)
    
  types_changeSoftwareStatus = [StringTypes, StringTypes, StringTypes, StringTypes, BooleanType]
  def export_changeSoftwareStatus(self, AppName, AppVersion, Platform, Comment, Status = False):
    """ Change the status of a software, by feault to False
    """
    return processDB.changeSoftwareStatus(AppName, AppVersion, Platform, Comment, Status)
  
  types_changeSiteStatus = [DictType]
  def export_changeSiteStatus(self, sitedict):
    return processDB.changeSiteStatus(sitedict)

  types_reportOK = [DictType]
  def export_reportOK(self, jobdict):
    return processDB.reportOK(jobdict)

  types_reportFailed = [DictType]
  def export_reportFailed(self, jobdict):
    return processDB.reportFailed(jobdict)

