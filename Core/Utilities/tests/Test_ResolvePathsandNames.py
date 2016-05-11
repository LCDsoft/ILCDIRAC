import unittest

from ILCDIRAC.Core.Utilities.resolvePathsAndNames import getProdFilename, resolveIFpaths
import os, tempfile, shutil

class ResolvePathsAndNamesTests( unittest.TestCase ):
  def setUp(self):
    """
    Make fake files for the test
    """
    self.inputfiles = ["toto_gen_12345_123.txt"]
    self.dir = tempfile.mkdtemp(dir = ".")
    self.realloc = os.path.join(self.dir,"toto_gen_12345_123.txt")
    with open(self.realloc,"w") as inputf:
      inputf.write("fake file")

  def tearDown(self):
    """ Remove the fake files
    """
    try:
      bd = os.path.dirname(self.realloc)
      os.remove(self.realloc)
      shutil.rmtree(bd)
    except:
      print "failed to remove file"
    
  def test_getnames(self):
    """test ResolvePathsAndNames getNames..........................................................."""
    res = getProdFilename("toto_gen.stdhep", 12345,123)
    self.assertEqual(res,'toto_gen_12345_123.stdhep')
  
  def test_resolvepaths(self):
    """test ResolvePathsAndNames resolvePaths......................................................."""
    res = resolveIFpaths(self.inputfiles)
    self.assertTrue('OK' in res)
    self.assertEqual(res['OK'], True, res)
    self.assertTrue('Value' in res, res.keys() )
    self.assertEqual(res['Value'], [ os.path.abspath(self.realloc) ])
    
    
if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( ResolvePathsAndNamesTests )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  