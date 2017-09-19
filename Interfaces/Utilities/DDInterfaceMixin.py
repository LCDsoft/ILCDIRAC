""" Mixin for DD4hep interface functions """

import types
import os

from DIRAC import S_OK, S_ERROR

class DDInterfaceMixin( object ):
  """Mixin for DD4hep interface functions

  This mixin class requires that the main class should be of :class:`Application` type
  and have the :attr:`detectorModel`, :attr:`_ops`, :attr:`version`, and :attr:`_log` attributes.

     * :attr:`_ops` is an Operations instance
     * :attr:`_log` a gLogger sublogger
     * :attr:`version` is the version of the application
     * :attr:`detectorModel` is set to the name of the detector model as inferred in the :func:`setDetectorModel` function

  """

  def setDetectorModel(self, detectorModel):
    """Define detector model to use for ddsim simulation

    The detector model can be a collection of XML files. Either one has to use a
    detector model provided by LCGeo or DD4hep, which can be found on CVMFS or
    the complete XML needs to be passed as a tarball in the input sandbox or on the grid

    >>> ddsim.setDetectorModel("CLIC_o3_v13") # detector model part of lcgeo

    >>> ddsim.setDetectorModel("CLIC_o3_v13.tar.gz") # local tarball, will be added to input sandbox

    >>> ddsim.setDetectorModel("LFN:/ilc/user/u/username/CLIC_o3_v13.tar.gz") # tarball on the grid

    The tarball name must be detectorModel plus extension.
    The tarball must contain all xml files inside a folder called detectorModel.
    That is the main file is located in *detectorModel/detectorModel.xml*
    
    :param string detectorModel: Detector Model to use for simulation or reconstruction. Can
      be on CVMFS, tarball LFN or inputSandbox tarball
    
    """
    self._checkArgs( { 'detectorModel' : types.StringTypes } )
    extensions = (".zip", ".tar.gz", ".tgz")

    ## file on the grid
    if detectorModel.lower().startswith("lfn:"):
      self.inputSB.append(detectorModel)
      self.detectorModel = os.path.basename(detectorModel) 
      for ext in extensions:
        if detectorModel.endswith(ext):
          self.detectorModel = os.path.basename(detectorModel).replace( ext, '' )
      return S_OK()

    ## local file
    elif detectorModel.endswith( extensions ):
      for ext in extensions:
        if detectorModel.endswith(ext):
          self.detectorModel = os.path.basename(detectorModel).replace( ext, '' )
          break

      if os.path.exists(detectorModel):
        self.inputSB.append(detectorModel)
      else:
        self._log.notice("Specified detector model file does not exist locally, I hope you know what you're doing")
      return S_OK()

    ## DetectorModel is part of the software
    else:
      knownDetectors = self.getKnownDetectorModels()
      if not knownDetectors['OK']:
        self._log.error("Failed to get knownDetectorModels", knownDetectors["Message"] )
        return knownDetectors
      elif detectorModel in knownDetectors['Value']:
        self.detectorModel = detectorModel
      else:
        self._log.error("Unknown detector model: ", detectorModel )
        return S_ERROR( "Unknown detector model in %s: %s" % ( self.appname, detectorModel ) )
    return S_OK()


  def getKnownDetectorModels( self, version=None ):
    """return a list of known detectorModels

    Depends on the version of the software though...

    :param string version: Optional: Software version for which to print the detector models. If not given the version of the application instance is used.
    :returns: S_OK with list of detector models known for this software version, S_ERROR
    
    """
    if version is None and not self.version:
      return S_ERROR( "No software version defined" )
    detectorModels = self._ops.getOptionsDict("/DDSimDetectorModels/%s" % (self.version))
    return detectorModels
