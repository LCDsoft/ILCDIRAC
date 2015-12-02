""" mixin for slic/lcsim commong functions """

import os
import urllib

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import unzip_file_into_dir


class CompactMixin( object ):
  """ mixin for detector geometry files used by slic and lcsim """

  def getDetectorModel( self ):
    """ retrieve detector model from web """
    detector_urls = self.ops.getValue('/SLICweb/SLICDetectorModels', [''])
    if len(detector_urls[0]) < 1:
      self.log.error('Could not find in CS the URL for detector model')
      return S_ERROR('Could not find in CS the URL for detector model')

    if not os.path.exists(self.detectorModel + ".zip"):
      for detector_url in detector_urls:
        try:
          urllib.urlretrieve("%s%s" % (detector_url, self.detectorModel + ".zip"),
                             self.detectorModel + ".zip")
        except IOError as e:
          self.log.error("Download of detector model failed", str(e))
          continue

    if not os.path.exists(self.detectorModel + ".zip"):
      self.log.error('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)
      return S_ERROR('Detector model %s was not found neither locally nor on the web, exiting' % self.detectorModel)

    try:
      unzip_file_into_dir(open(self.detectorModel + ".zip"), os.getcwd())
    except (RuntimeError, OSError BadZipfile) as err: #RuntimeError is for zipfile
      os.unlink(self.detectorModel + ".zip")
      self.log.error('Failed to unzip detector model: ', str(err))
      return S_ERROR('Failed to unzip detector model')
    #unzip detector model
    #self.unzip_file_into_dir(open(self.detectorModel+".zip"),os.getcwd())
    return S_OK()
