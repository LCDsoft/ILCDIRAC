""" mixin for slic/lcsim commong functions """

import os
import urllib
import zipfile
from DIRAC import S_OK, S_ERROR

from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import unzip_file_into_dir


class CompactMixin( object ):
  """ mixin for detector geometry files used by slic and lcsim """

  def getDetectorModel( self ):
    """ retrieve detector model from web """
    retDown = self.downloadDetectorZip()
    if not retDown['OK']:
      return retDown

    detectorModel = self.detectorModel[:-4] if self.detectorModel.endswith(".zip") else self.detectorModel
    if not os.path.exists(detectorModel + ".zip"):
      self.log.error('Detector model %s was not found neither locally nor on the web, exiting' % detectorModel)
      return S_ERROR('Detector model %s was not found neither locally nor on the web, exiting' % detectorModel)
    try:
      unzip_file_into_dir(open(detectorModel + ".zip"), os.getcwd())
    except (RuntimeError, OSError, zipfile.BadZipfile) as err: #RuntimeError is for zipfile
      os.unlink(detectorModel + ".zip")
      self.log.error('Failed to unzip detector model: ', str(err))
      return S_ERROR('Failed to unzip detector model')
    #unzip detector model
    #self.unzip_file_into_dir(open(detectorModel+".zip"),os.getcwd())
    return S_OK()

  def downloadDetectorZip( self ):
    """ download the detector zip file """
    detector_urls = self.ops.getValue('/SLICweb/SLICDetectorModels', [''])
    if len(detector_urls[0]) < 1:
      self.log.error('Could not find in CS the URL for detector model')
      return S_ERROR('Could not find in CS the URL for detector model')

    detectorModel = self.detectorModel[:-4] if self.detectorModel.endswith(".zip") else self.detectorModel
    if not os.path.exists(detectorModel + ".zip"):
      for detector_url in detector_urls:
        try:
          urllib.urlretrieve("%s%s" % (detector_url, detectorModel + ".zip"),
                             detectorModel + ".zip")
        except IOError as e:
          self.log.error("Download of detector model failed", str(e))
          continue
    return S_OK()
