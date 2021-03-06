########################################################################
# File :    DownloadInputData.py
# Author :  Stuart Paterson
########################################################################

""" The Download Input Data module wraps around the Replica Management
    components to provide access to datasets by available site protocols as
    defined in the CS for the VO.
"""

import os
import tempfile
import random

from DIRAC                                                          import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Client import Client
from DIRAC.Resources.Storage.StorageElement                         import StorageElement
from DIRAC.Core.Utilities.Os                                        import getDiskSpace
from DIRAC.DataManagementSystem.Utilities.DMSHelpers                import DMSHelpers

__RCSID__ = "$Id$"

COMPONENT_NAME = 'DownloadInputData'

def _isCached( lfn, seName ):
  result = StorageElement( seName ).getFileMetadata( lfn )
  if not result['OK']:
    return False
  if lfn in result['Value']['Failed']:
    return False
  metadata = result['Value']['Successful'][lfn]
  return metadata.get( 'Cached', metadata['Accessible'] )

class DownloadInputData( object ):
  """
   retrieve InputData LFN from localSEs (if available) or from elsewhere.
  """

  #############################################################################
  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )
    self.inputData = argumentsDict['InputData']
    self.configuration = argumentsDict['Configuration']
    # Warning: this contains not only the SEs but also the file metadata
    self.fileCatalogResult = argumentsDict['FileCatalog']
    # By default put each input data file into a separate directory
    self.inputDataDirectory = argumentsDict.get( 'InputDataDirectory', 'PerFile' )
    self.jobID = None
    self.counter = 1
    self.availableSEs = DMSHelpers().getStorageElements()

  #############################################################################
  def execute( self, dataToResolve = None ):
    """This method is called to download the requested files in the case where
       enough local disk space is available.  A buffer is left in this calculation
       to leave room for any produced files.
    """

    # Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']

    self.jobID = self.configuration.get( 'JobID' )

    if dataToResolve:
      self.log.verbose( 'Data to resolve passed directly to DownloadInputData module' )
      self.inputData = dataToResolve  # e.g. list supplied by another module

    self.inputData = sorted( lfn.replace( 'LFN:', '' ) for lfn in self.inputData )
    self.log.info( 'InputData to be downloaded is:\n%s' % '\n'.join( self.inputData ) )

    replicas = self.fileCatalogResult['Value']['Successful']

    # Problematic files will be returned and can be handled by another module
    failedReplicas = set()
    # For the unlikely case that a file is found on two SEs at the same site
    # disk-based replicas are favoured.
    downloadReplicas = {}
    # determine Disk and Tape SEs
    diskSEs = set()
    tapeSEs = set()
    for localSE in [se for se in localSEList if se]:
      seStatus = StorageElement( localSE ).getStatus()
      if not seStatus['OK']:
        return seStatus
      seStatus = seStatus['Value']
      if seStatus['Read'] and seStatus['DiskSE']:
        diskSEs.add( localSE )
      elif seStatus['Read'] and seStatus['TapeSE']:
        tapeSEs.add( localSE )

    for lfn, reps in replicas.iteritems():
      if lfn not in self.inputData:
        self.log.verbose( 'LFN %s is not in requested input data to download' )
        failedReplicas.add( lfn )
        continue

      if not ( 'Size' in reps and 'GUID' in reps ):
        self.log.error( 'Missing LFN metadata', "%s %s" % ( lfn, str( reps ) ) )
        failedReplicas.add( lfn )
        continue

      # Get and remove size and GUIS
      size = reps.pop( 'Size' )
      guid = reps.pop( 'GUID' )
      # Remove all other items that are not SEs
      for item in reps.keys():
        if item not in self.availableSEs:
          reps.pop( item )
      downloadReplicas[lfn] = {'SE':[], 'Size':size, 'GUID':guid}
      # First get Disk replicas
      for seName in diskSEs:
        if seName in reps:
          downloadReplicas[lfn]['SE'].append( seName )
      # If no disk replicas, take tape replicas
      if not downloadReplicas[lfn]['SE']:
        for seName in tapeSEs:
          if seName in reps and _isCached( lfn, seName ):
            # Only consider replicas that are cached
            downloadReplicas[lfn]['SE'].append( seName )

    totalSize = 0
    verbose = self.log.verbose( 'Replicas to download are:' )
    for lfn, reps in downloadReplicas.iteritems():
      self.log.verbose( lfn )
      if not reps['SE']:
        self.log.info( 'Failed to find data at local SEs, will try to download from anywhere', lfn )
        reps['SE'] = ''
      else:
        if len( reps['SE'] ) > 1:
          # if more than one SE is available randomly select one
          random.shuffle( reps['SE'] )
        # get SE and pfn from tuple
        reps['SE'] = reps['SE'][0]
      totalSize += int( reps.get( 'Size', 0 ) )
      if verbose:
        for item, value in sorted( reps.items() ):
          if value:
            self.log.verbose( '\t%s %s' % ( item, value ) )

    self.log.info( 'Total size of files to be downloaded is %s bytes' % ( totalSize ) )
    for lfn in failedReplicas:
      self.log.warn( 'Not all file metadata (SE,PFN,Size,GUID) was available for LFN', lfn )

    # Now need to check that the list of replicas to download fits into
    # the available disk space. Initially this is a simple check and if there is not
    # space for all input data, no downloads are attempted.
    result = self.__checkDiskSpace( totalSize )
    if not result['OK']:
      self.log.warn( 'Problem checking available disk space:\n%s' % ( result ) )
      return result

    if not result['Value']:
      report = 'Not enough disk space available for download: %s / %s bytes' % ( result['Value'], totalSize )
      self.log.warn( report )
      self.__setJobParam( COMPONENT_NAME, report )
      return S_OK( { 'Failed': self.inputData, 'Successful': {}} )

    resolvedData = {}
    localSECount = 0
    for lfn, info in downloadReplicas.iteritems():
      seName = info['SE']
      guid = info['GUID']
      reps = replicas.get( lfn, {} )
      if seName:
        result = StorageElement( seName ).getFileMetadata( lfn )
        if not result['OK']:
          self.log.error( "Error getting metadata", result['Message'] )
          failedReplicas.add( lfn )
          continue
        if lfn in result['Value']['Failed']:
          self.log.error( 'Could not get Storage Metadata for %s at %s: %s' % ( lfn, seName, result['Value']['Failed'][lfn] ) )
          failedReplicas.add( lfn )
          continue
        metadata = result['Value']['Successful'][lfn]
        if metadata.get( 'Lost', False ):
          error = "PFN has been Lost by the StorageElement"
        elif metadata.get( 'Unavailable', False ):
          error = "PFN is declared Unavailable by the StorageElement"
        elif seName in tapeSEs and not metadata.get( 'Cached', metadata['Accessible'] ):
          error = "PFN is no longer in StorageElement Cache"
        else:
          error = ''
        if error:
          self.log.error( error, lfn )
          failedReplicas.add( lfn )
          continue

        self.log.info( 'Preliminary checks OK, download %s from %s:' % ( lfn, seName ) )
        result = self._downloadFromSE( lfn, seName, reps, guid )
        if not result['OK']:
          self.log.error( "Download failed", "Tried downloading from SE %s: %s" % ( seName, result['Message'] ) )
      else:
        result = {'OK':False}

      if not result['OK']:
        reps.pop( seName, None )
        # Check the other SEs
        if reps:
          self.log.info( 'Trying to download from any SE' )
          result = self._downloadFromBestSE( lfn, reps, guid )
          if not result['OK']:
            self.log.error( "Download from best SE failed", "Tried downloading %s: %s" % ( lfn, result['Message'] ) )
            failedReplicas.add( lfn )
        else:
          failedReplicas.add( lfn )
      else:
        localSECount += 1
      if result['OK']:
        # Rename file if downloaded FileName does not match the LFN... How can this happen?
        lfnName = os.path.basename( lfn )
        oldPath = result['Value']['path']
        fileName = os.path.basename( oldPath )
        if lfnName != fileName:
          newPath = os.path.join( os.path.dirname( oldPath ), lfnName )
          os.rename( oldPath, newPath )
          result['Value']['path'] = newPath
        resolvedData[lfn] = result['Value']

    # Report datasets that could not be downloaded
    report = ''
    if resolvedData:
      report += 'Successfully downloaded %d LFN(s)' % len( resolvedData )
      if localSECount != len( resolvedData ):
        report += ' (%d from local SEs):\n' % localSECount
      else:
        report += ' from local SEs:\n'
      report += '\n'.join( sorted( resolvedData ) )
    failedReplicas = sorted( failedReplicas.difference( resolvedData ) )
    if failedReplicas:
      self.log.warn( 'The following LFN(s) could not be downloaded to the WN:\n%s' % 'n'.join( failedReplicas ) )
      report += '\nFailed to download %d LFN(s):\n' % len( failedReplicas )
      report += '\n'.join( failedReplicas )

    if report:
      self.__setJobParam( COMPONENT_NAME, report )

    return S_OK( {'Successful': resolvedData, 'Failed':failedReplicas} )

  #############################################################################
  def __checkDiskSpace( self, totalSize ):
    """Compare available disk space to the file size reported from the catalog
       result.
    """
    diskSpace = getDiskSpace( self.__getDownloadDir( False ) )  # MB
    availableBytes = diskSpace * 1024 * 1024  # bytes
    # below can be a configuration option sent via the job wrapper in the future
    # Moved from 3 to 5 GB (PhC 130822) for standard output file
    bufferGBs = 5.0
    data = bufferGBs * 1024 * 1024 * 1024 # bufferGBs in bytes
    if ( data + totalSize ) < availableBytes:
      msg = 'Enough disk space available (%s bytes)' % ( availableBytes )
      self.log.verbose( msg )
      return S_OK( msg )
    else:
      msg = 'Not enough disk space available for download %s (including %dGB buffer) > %s bytes' \
             % ( ( data + totalSize ), bufferGBs, availableBytes )
      self.log.warn( msg )
      return S_ERROR( msg )

  def __getDownloadDir( self, incrementCounter = True ):
    if self.inputDataDirectory == "PerFile":
      if incrementCounter:
        self.counter += 1
      return tempfile.mkdtemp( prefix = 'InputData_%s' % ( self.counter ), dir = os.getcwd() )
    elif self.inputDataDirectory == "CWD":
      return os.getcwd()
    else:
      return self.inputDataDirectory

  #############################################################################
  def _downloadFromBestSE( self, lfn, reps, guid ):
    """ Download a local copy of a single LFN from a list of Storage Elements.
        This is used as a last resort to attempt to retrieve the file.
    """
    self.log.verbose( "Attempting to download file from all SEs (%s):" % ','.join( reps ), lfn )
    diskSEs = set()
    tapeSEs = set()
    for seName in reps:
      seStatus = StorageElement( seName ).status()
      # FIXME: This is simply terrible - this notion of "DiskSE" vs "TapeSE" should NOT be used here!
      if seStatus['Read'] and seStatus['DiskSE']:
        diskSEs.add( seName )
      elif seStatus['Read'] and seStatus['TapeSE']:
        tapeSEs.add( seName )

    for seName in list( diskSEs ) + list( tapeSEs ):
      if seName in diskSEs or _isCached( lfn, seName ):
        # On disk or cached from tape
        result = self._downloadFromSE( lfn, seName, reps, guid )
        if result['OK']:
          return result
        else:
          self.log.error( "Download failed", "Tried downloading %s from SE %s: %s" % ( lfn, seName, result['Message'] ) )

    return S_ERROR( "Unable to download the file from any SE" )

  #############################################################################
  def _downloadFromSE( self, lfn, seName, reps, guid ):
    """ Download a local copy from the specified Storage Element.
    """
    if not lfn:
      return S_ERROR( "LFN not specified: assume file is not at this site" )

    self.log.verbose( "Attempting to download file %s from %s:" % ( lfn, seName ) )

    downloadDir = self.__getDownloadDir()
    fileName = os.path.basename( lfn )
    for localFile in ( os.path.join( os.getcwd(), fileName ), os.path.join( downloadDir, fileName ) ):
      if os.path.exists( localFile ):
        self.log.info( "File %s already exists locally as %s" % ( fileName, localFile ) )
        fileDict = { 'turl':'LocalData',
                     'protocol':'LocalData',
                     'se':seName,
                     'pfn':reps[seName],
                     'guid':guid,
                     'path': localFile}
        return S_OK( fileDict )

    localFile = os.path.join( downloadDir, fileName )
    result = StorageElement( seName ).getFile( lfn, localPath = downloadDir )
    if not result['OK']:
      self.log.warn( 'Problem getting %s from %s:\n%s' % ( lfn, seName, result['Message'] ) )
      self.__cleanFailedFile( lfn, downloadDir )
      return result
    if lfn in result['Value']['Failed']:
      self.log.warn( 'Problem getting %s from %s:\n%s' % ( lfn, seName, result['Value']['Failed'][lfn] ) )
      self.__cleanFailedFile( lfn, downloadDir )
      return S_ERROR( result['Value']['Failed'][lfn] )
    if lfn not in result['Value']['Successful']:
      self.log.warn( "%s got from %s not in Failed nor Successful???\n" % ( lfn, seName ) )
      self.__cleanFailedFile( lfn, downloadDir )
      return S_ERROR( "Return from StorageElement.getFile() incomplete" )

    if os.path.exists( localFile ):
      self.log.verbose( "File %s successfully downloaded locally to %s" % ( lfn, localFile ) )
      fileDict = {'turl':'Downloaded',
                  'protocol':'Downloaded',
                  'se':seName,
                  'pfn':reps[seName],
                  'guid':guid,
                  'path':localFile}
      return S_OK( fileDict )
    else:
      self.log.warn( 'File does not exist in local directory after download' )
      return S_ERROR( 'OK download result but file missing in current directory' )

  #############################################################################
  def __setJobParam(self, name, value):
    """Wrap around setJobParameter of state update client."""
    if not self.jobID:
      return S_ERROR('JobID not defined')

    jobParam = Client(timeout=120).setJobParameter(int(self.jobID), str(name), str(value),
                                                   url='WorkloadManagement/JobStateUpdate')
    self.log.verbose('setJobParameter(%s,%s,%s)' % (self.jobID, name, value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam

  def __cleanFailedFile( self, lfn, downloadDir ):
    """ Try to remove a file after a failed download attempt """
    filePath = os.path.join( downloadDir, os.path.basename( lfn ) )
    self.log.error( "Trying to remove file after failed download", "Local path: %s " % filePath )
    if os.path.exists( filePath ):
      try:
        os.remove( filePath )
        self.log.error( "Removed file remnant after failed download", "Local path: %s " % filePath )
      except OSError as e:
        self.log.error( "Failed to remove file after failed download", repr(e) )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
