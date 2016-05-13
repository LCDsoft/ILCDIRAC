"""
Utility functions for DD4hep geometry files etc.
"""

import os

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder


class DD4hepMixin( object ):
  """ mixin class for DD4hep functionality """
  
  def _getDetectorXML( self ):
    """returns the path to the detector XML file

    Checks the Configurartion System for the Path to DetectorModels or extracts the input sandbox detector xml files

    :returns: S_OK(PathToXMLFile), S_ERROR
    """

    if os.path.exists( os.path.join( self.detectorModel, self.detectorModel+ ".xml" ) ):
      self.log.notice( "Found detector model: %s" % os.path.join( self.detectorModel, self.detectorModel+ ".xml" ) )
      return S_OK( os.path.join( self.detectorModel, self.detectorModel+ ".xml" ) )
    elif os.path.exists(self.detectorModel + ".zip"):
      self.log.notice( "Found detector model zipFile: %s" % self.detectorModel+ ".zip" )
      return self._extractZip()
    elif os.path.exists(self.detectorModel + ".tar.gz"):
      self.log.notice( "Found detector model tarball: %s" % self.detectorModel+ ".tar.gz" )
      return self._extractTar()
    elif os.path.exists(self.detectorModel + ".tgz"):
      self.log.notice( "Found detector model tarball: %s" % self.detectorModel+ ".tgz" )
      return self._extractTar( extension=".tgz" )

    detectorModels = self.ops.getOptionsDict("/DDSimDetectorModels/%s" % ( self.applicationVersion ) )
    if not detectorModels['OK']:
      self.log.error("Failed to get list of DetectorModels from the ConfigSystem", detectorModels['Message'])
      return S_ERROR("Failed to get list of DetectorModels from the ConfigSystem")

    softwareFolder = getSoftwareFolder(self.platform, self.applicationName, self.applicationVersion)
    if not softwareFolder['OK']:
      return softwareFolder
    softwareRoot = softwareFolder['Value']

    if self.detectorModel in detectorModels['Value']:
      detModelPath = detectorModels['Value'][self.detectorModel]
      if not detModelPath.startswith("/"):
        detModelPath = os.path.join( softwareRoot, detModelPath )
      self.log.info( "Found path for DetectorModel %s in CS: %s "  % ( self.detectorModel, detModelPath ) )
      return S_OK(detModelPath)


    self.log.error('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)
    return S_ERROR('Detector model was not found')
