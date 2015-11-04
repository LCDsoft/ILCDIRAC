'''
Update the transformation files of active transformations given an InputDataQuery fetched from the Transformation Service.

Possibility to speedup the query time by only fetching files that were added since the last iteration.
Use the CS option RefreshOnly (False by default) and set the DateKey (empty by default) to the meta data
key set in the DIRAC FileCatalog.

.. deprecated:: 23.1

'''

import time, datetime

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.TransformationSystem.Agent.InputDataAgent                      import InputDataAgent
from DIRAC.Core.Utilities.List                                            import sortList

AGENT_NAME = 'ILCTransformation/ILCInputDataAgent'

class ILCInputDataAgent( InputDataAgent ):
  """ILCDirac version of the InputDataAgent"""
  def __init__( self, *args, **kwargs ):
    ''' An AgentModule class for ILCDIRAC (this string is needed for allowing installation)
    '''
    InputDataAgent.__init__( self, *args, **kwargs )

  ##############################################################################
  def execute( self ):
    ''' Main execution method
    '''

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( {'Status' : 'Active', 
                                                   'Type' : self.transformationTypes } )
    if not result['OK']:
      gLogger.error( "ILCInputDataAgent.execute: Failed to get transformations.", result['Message'] )
      return S_OK()

    # Process each transformation
    for transDict in result['Value']:
      transID = long( transDict['TransformationID'] )
      res = self.transClient.getTransformationInputDataQuery( transID )
      if not res['OK']:
        if res['Message'] == 'No InputDataQuery found for transformation':
          gLogger.info( "InputDataAgent.execute: No input data query found for transformation %d" % transID )
        else:
          gLogger.error( "InputDataAgent.execute: Failed to get input data query for %d" % transID, res['Message'] )
        continue
      inputDataQuery = res['Value']

      if self.refreshonly:
        # Determine the correct time stamp to use for this transformation
        if self.timeLog.has_key( transID ):
          if self.fullTimeLog.has_key( transID ):
            # If it is more than a day since the last reduced query, make a full query just in case
            if ( datetime.datetime.utcnow() - self.fullTimeLog[transID] ) < datetime.timedelta( seconds = self.fullUpdatePeriod ):
              timeStamp = self.timeLog[transID]
              if self.dateKey:
                inputDataQuery[self.dateKey] = ( timeStamp - datetime.timedelta( seconds = 10 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
              else:
                gLogger.error( "DateKey was not set in the CS, cannot use the RefreshOnly" )
            else:
              self.fullTimeLog[transID] = datetime.datetime.utcnow()
        self.timeLog[transID] = datetime.datetime.utcnow()
        if not self.fullTimeLog.has_key( transID ):
          self.fullTimeLog[transID] = datetime.datetime.utcnow()

      # Perform the query to the metadata catalog
      gLogger.verbose( "Using input data query for transformation %d: %s" % ( transID, str( inputDataQuery ) ) )
      start = time.time()
      result = self.metadataClient.findFilesByMetadata( inputDataQuery )
      rtime = time.time() - start
      gLogger.verbose( "Metadata catalog query time: %.2f seconds." % ( rtime ) )
      if not result['OK']:
        gLogger.error( "InputDataAgent.execute: Failed to get response from the metadata catalog", result['Message'] )
        continue
      lfnList = result['Value']

      # Check if the number of files has changed since the last cycle
      nlfns = len( lfnList )
      gLogger.info( "%d files returned for transformation %d from the metadata catalog" % ( nlfns, int( transID ) ) )
      if self.fileLog.has_key( transID ):
        if nlfns == self.fileLog[transID]:
          gLogger.verbose( 'No new files in metadata catalog since last check' )
      self.fileLog[transID] = nlfns

      ##Now take care of the Sliced transformations
      final_list = []
      if 'Plugin' in transDict:
        if transDict['Plugin'] in ['Sliced','SlicedLimited']:
          gLogger.verbose('Processing Sliced transformation')
          res = self.transClient.getTransformationParameters(transID, ['EventsPerTask'])
          if not res['OK']:
            gLogger.error("Failed getting the EventsPerSlice parameter", res['Message'])
            continue
          evts_slice = float(res['Value'])
          if evts_slice > 0:
            broke = False
            for lfn in lfnList:
              res = self.metadataClient.getFileUserMetadata(lfn)
              if not res['OK']:
                gLogger.error("Failed getting file metadata", res['Message'])
                broke = True
                break
              fmeta = res['Value']
              if not 'NumberOfEvents' in fmeta:
                gLogger.error("Number of events not defined for ", lfn)
                broke = True
                break
              nb_evts_in_file = float(fmeta['NumberOfEvents'])
              slice_nb = 0
              remaining_evts = nb_evts_in_file
              start_evt_in_slice = 0
              while remaining_evts > 0:
                start_evt_in_slice = slice_nb*evts_slice
                slice_nb += 1
                remaining_evts -= evts_slice
                final_list.append(lfn+":%s" % ( int( start_evt_in_slice ) ) ) ##This is where the magic happens
                #gLogger.verbose("Added ", final_list[-1])
                
            if broke:
              gLogger.error("Cannot proceed with this transformation")
              continue
          else:#All events in the files
            final_list = lfnList
        else:#plugin = Sliced
          final_list = lfnList
          
      # Add any new files to the transformation
      addedLfns = []
      if lfnList:
        gLogger.verbose( 'Processing %d lfns for transformation %d' % ( len( final_list ), transID ) )
        # Add the files to the transformation
        gLogger.verbose( 'Adding %d lfns for transformation %d' % ( len( final_list ), transID ) )
        result = self.transClient.addFilesToTransformation( transID, sortList( final_list ) )
        if not result['OK']:
          gLogger.warn( "InputDataAgent.execute: failed to add lfns to transformation", result['Message'] )
          self.fileLog[transID] = 0
        else:
          if result['Value']['Failed']:
            for lfn, error in res['Value']['Failed'].items():
              gLogger.warn( "InputDataAgent.execute: Failed to add %s to transformation" % lfn, error )
          if result['Value']['Successful']:
            for lfn, status in result['Value']['Successful'].items():
              if status == 'Added':
                addedLfns.append( lfn )
            gLogger.info( "InputDataAgent.execute: Added %d files to transformation" % len( addedLfns ) )

    return S_OK()
