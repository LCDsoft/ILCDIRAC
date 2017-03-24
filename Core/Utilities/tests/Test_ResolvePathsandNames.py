""" unit tests for resolvePathsAndNames module """

import unittest
import os
import tempfile
import shutil

from ILCDIRAC.Core.Utilities.resolvePathsAndNames import getProdFilename, resolveIFpaths, getProdFilenameFromInput

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

  def test_ildProdSim(self):
    """test getOridFilenameFromInput Sim ..........................................................."""
    inputFile = "/ilc/prod/ilc/ild/test/temp1/gensplit/500-TDR_ws/3f/run001/E0500-TDR_ws.Pae_ell.Gwhizard-1.95.eW.pL.I37537.01_002.stdhep" ## LFN
    outfileOriginal = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws_sim_400859_4.slcio"
    prodID = 1234
    jobID = 321
    expectedOutputLFN = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37537.Pae_ell.eW.pL.n01_002.d_sim_00001234_321.slcio"
    outLFN = getProdFilenameFromInput( inputFile, outfileOriginal, prodID, jobID )
    self.assertEqual( outLFN, expectedOutputLFN )

  def test_ildProdRec(self):
    """test getProdFilenameFromInput Rec ..........................................................."""
    inputFile = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_sim_00001234_12.slcio" ## LFN
    outfileOriginal = "/ilc/prod/ilc/test/ild/rec/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001235/000/rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws_rec_1235_321.slcio"
    prodID = 1235
    jobID = 321
    expectedOutputLFN = "/ilc/prod/ilc/test/ild/rec/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001235/000/rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_rec_00001235_321.slcio"
    outLFN = getProdFilenameFromInput( inputFile, outfileOriginal, prodID, jobID )
    self.assertEqual( outLFN, expectedOutputLFN )

  def test_ildProdDst(self):
    """test getProdFilenameFromInput DST ..........................................................."""
    inputFile = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_sim_00001234_12.slcio" ## LFN
    outfileOriginal = "/ilc/prod/ilc/test/ild/rec/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001235/000/rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws_dst_00001235_321.slcio"
    prodID = 1235
    jobID = 321
    expectedOutputLFN = "/ilc/prod/ilc/test/ild/rec/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001235/000/rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_dst_00001235_321.slcio"
    outLFN = getProdFilenameFromInput( inputFile, outfileOriginal, prodID, jobID )
    self.assertEqual( outLFN, expectedOutputLFN )


    
    
if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( ResolvePathsAndNamesTests )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  