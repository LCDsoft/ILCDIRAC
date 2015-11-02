"""ILCDirac version of the Transformation Agent

.. deprecated:: 23.1

"""
__RCSID__ = "$Id$"
from DIRAC.Core.Utilities.ThreadSafe                                import Synchronizer
from DIRAC.TransformationSystem.Agent.TransformationAgent           import TransformationAgent


from DIRAC import S_OK
import time, re

AGENT_NAME = 'ILCTransformation/ILCTransformationAgent'
gSynchro = Synchronizer()

class ILCTransformationAgent( TransformationAgent ):
  """ Usually subclass of AgentModule
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    TransformationAgent.__init__( self, *args, **kwargs )
  
  def _getDataReplicasRM( self, transID, lfns, clients, active = True, ignoreMissing = False ):
    """ Get the replicas for the LFNs and check their statuses, using the replica manager
    """
    method = '_getDataReplicasRM'

    startTime = time.time()
    self._logVerbose( "Getting replicas for %d files from catalog" % len( lfns ),
                      method = method, transID = transID )
    
    fdict = {}
    for lfnName in lfns:
      lfn = lfnName.split(":")[0]
      if not lfn in fdict:
        fdict[lfn] = []
      fdict[lfn].append(lfnName)
      
    if active:
      res = clients['DataManager'].getActiveReplicas( fdict.keys() )
    else:
      res = clients['DataManager'].getReplicas( fdict.keys() )
    if not res['OK']:
      return res
    replicas = res['Value']
    # Prepare a dictionary for all LFNs
    dataReplicas = {}
    for lfn in lfns:
      dataReplicas[lfn] = []
    self._logInfo( "Replica results for %d files obtained in %.2f seconds" % ( len( lfns ), time.time() - startTime ),
                   method = method, transID = transID )
    #If files are neither Successful nor Failed, they are set problematic in the FC
    problematicLfns = [lfn for lfn in fdict.keys() if lfn not in replicas['Successful'] and lfn not in replicas['Failed']]
    if problematicLfns:
      self._logInfo( "%d files found problematic in the catalog" % len( problematicLfns ) )
      res = clients['TransformationClient'].setFileStatusForTransformation( transID, 'ProbInFC', problematicLfns )
      if not res['OK']:
        self._logError( "Failed to update status of problematic files: %s." % res['Message'],
                        method = method, transID = transID )
    # Create a dictionary containing all the file replicas
    for lfn, replicaDict in replicas['Successful'].items():
      ses = replicaDict.keys()
      for se in ses:
        #### This should definitely be included in the SE definition (i.e. not used for transformations)
        for filename in fdict[lfn]:
          dataReplicas[filename].append( se )
    # Make sure that file missing from the catalog are marked in the transformation DB.
    missingLfns = []
    for lfn, reason in replicas['Failed'].items():
      if re.search( "No such file or directory", reason ):
        self._logVerbose( "%s not found in the catalog." % lfn, method = method, transID = transID )
        for filename in fdict[lfn]:
          missingLfns.append( filename )
    if missingLfns:
      self._logInfo( "%d files not found in the catalog" % len( missingLfns ) )
      res = clients['TransformationClient'].setFileStatusForTransformation( transID, 'MissingInFC', missingLfns )
      if not res['OK']:
        self._logError( "Failed to update status of missing files: %s." % res['Message'],
                        method = method, transID = transID )
    return S_OK( dataReplicas )
