
import unittest

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent import FileStatusTransformationAgent

__RCSID__ = "$Id$"

class TestFileStatusTransformationAgent( unittest.TestCase ):

  def setUp(self):
    pass

  def tearDown(self):
    pass

if __name__ == "__main__":
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestFileStatusTransformationAgent )
  unittest.TextTestRunner( verbosity = 3 ).run( suite )
