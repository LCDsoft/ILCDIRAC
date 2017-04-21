""" unit tests for resolvePathsAndNames module """

import unittest
import os
import tempfile
import shutil

from ILCDIRAC.Core.Utilities.resolvePathsAndNames import getProdFilename, resolveIFpaths, getProdFilenameFromInput

class ResolvePathsAndNamesTests(unittest.TestCase):
  '''  Test resolvePathsAndNames  '''
  def setUp(self):
    """
    Make fake files for the test
    """
    self.inputfiles = ["toto_gen_12345_123.txt"]
    self.dir = tempfile.mkdtemp(dir=".")
    self.realloc = os.path.join(self.dir, "toto_gen_12345_123.txt")
    with open(self.realloc, "w") as inputf:
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
    res = getProdFilename("toto_gen.stdhep", 12345, 123)
    self.assertEqual(res, 'toto_gen_12345_123.stdhep')
 
  def test_resolvepaths(self):
    """test ResolvePathsAndNames resolvePaths......................................................."""
    res = resolveIFpaths(self.inputfiles)
    self.assertTrue('OK' in res)
    self.assertEqual(res['OK'], True, res)
    self.assertTrue('Value' in res, res.keys())
    self.assertEqual(res['Value'], [os.path.abspath(self.realloc)])

  def test_ildprod_sim(self):
    """test getOridFilenameFromInput Sim ..........................................................."""
    indir = "/ilc/prod/ilc/ild/test/temp1/gensplit/500-TDR_ws/3f/run001/"
    outdir = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/"
    input_file = indir + "E0500-TDR_ws.Pae_ell.Gwhizard-1.95.eW.pL.I37537.01_002.stdhep"
    outfile_original = outdir+"sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws_sim_400859_4.slcio"
    prodid = 1234
    jobid = 321
    outfile_new = outdir + "sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37537.Pae_ell.eW.pL.n01_002.d_sim_00001234_321.slcio"
    out_lfn = getProdFilenameFromInput(input_file, outfile_original, prodid, jobid)
    self.assertEqual(out_lfn, outfile_new)

  def test_ildprod_rec(self):
    """test getProdFilenameFromInput Rec ..........................................................."""
    indir = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/"
    outdir = "/ilc/prod/ilc/test/ild/rec/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001235/000/"
    input_file = indir + "sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_sim_00001234_12.slcio" ## LFN
    outfile_original = outdir + "rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws_rec_1235_321.slcio"
    prodid = 1235
    jobid = 321
    outfile_new = outdir + "rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_rec_00001235_321.slcio"
    out_lfn = getProdFilenameFromInput(input_file, outfile_original, prodid, jobid)
    self.assertEqual(out_lfn, outfile_new)

  def test_ildprod_dst(self):
    """test getProdFilenameFromInput DST ..........................................................."""
    indir = "/ilc/prod/ilc/test/ild/sim/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001234/000/"
    outdir = "/ilc/prod/ilc/test/ild/dst/500-TDR_ws/3f/ILD_o1_v05/v01-19_lcgeo/00001235/000/"
    input_file = indir + "sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_sim_00001234_12.slcio" ## LFN
    outfile_original = outdir + "rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws_dst_00001235_321.slcio"
    prodid = 1235
    jobid = 321
    outfile_new = outdir + "rv01-19_lcgeo.sv01-19_lcgeo.mILD_o1_v05.E500-TDR_ws.I37540.Pae_ell.eB.pR.n01_002.d_dst_00001235_321.slcio"
    out_lfn = getProdFilenameFromInput(input_file, outfile_original, prodid, jobid)
    self.assertEqual(out_lfn, outfile_new)

  def test_ildprod_stdhepsplit(self):
    """test getProdFilenameFromInput Generator stdhep  ..........................................................."""
    indir = "/ilc/prod/ilc/mc-dbd/generated/500-TDR_ws/higgs/"
    outdir = "/ilc/prod/ilc/ild/test/temp1/mc-dbd.disk/ild/splitted/500-TDR_ws/higgs_ffh/00400922/000/"
    input_file = indir + "E500-TDR_ws.Pqqh_ww_4q.Gwhizard-1_95.eL.pR.I106730.001.stdhep"
    outfile_original = outdir + "E500-TDR_ws.I106730.Pqqh_ww_4q.eL.pR_gen_400922_1_035.stdhep"
    prodid = 400922
    jobid = 87
    outfile_new = outdir + "E500-TDR_ws.Pqqh_ww_4q.Gwhizard-1_95.eL.pR.I106730.n001_035.d_gen_00400922_87.stdhep"
    out_lfn = getProdFilenameFromInput(input_file, outfile_original, prodid, jobid)
    self.assertEqual(out_lfn, outfile_new)

  def test_ildprod_genslciopslit(self):
    """test getProdFilenameFromInput Generator slcio ..........................................................."""
    indir = "/ilc/prod/ilc/mc-dbd/generated/500-TDR_ws/higgs/"
    outdir = "/ilc/prod/ilc/ild/test/temp1/mc-dbd.disk/ild/splitted/500-TDR_ws/higgs_ffh/00012930/000/"
    input_file = indir + "E500-TDR_ws.Pqqh_ww_4q.Gwhizard-1_95.eL.pR.I106730.001.slcio"
    outfile_original = outdir + "E500-TDR_ws.I106730.Pqqh_ww_4q.eL.pR_gen_12930_1_093.slcio"
    prodid = 12930
    jobid = 93
    outfile_new = outdir + "E500-TDR_ws.Pqqh_ww_4q.Gwhizard-1_95.eL.pR.I106730.n001_093.d_gen_00012930_93.slcio"
    out_lfn = getProdFilenameFromInput(input_file, outfile_original, prodid, jobid)
    self.assertEqual(out_lfn, outfile_new)

  def test_ildprod_undef_format(self):
    """test getProdFilenameFromInput Undefined file name ( 1st caharacter is not "E", nor "s" nor "r" .........."""
    indir = "/ilc/user/a/amiyamot/mygen/single_particles/jeans/"
    outdir = "/ilc/user/a/amiyamot/splitted/single_particles/jeans/"
    input_file = indir + "mcparticles_PDG130_MOM20GeV.slcio"
    outfile_original = outdir + "mcparticles_PDG130_MOM20GeV_gen_11520_1_093.slcio"
    prodid = 11520
    jobid = 93
    outfile_new = outdir + "mcparticles_PDG130_MOM20GeV_gen_11520_1_093.slcio"
    out_lfn = getProdFilenameFromInput(input_file, outfile_original, prodid, jobid)
    self.assertEqual(out_lfn, outfile_new)

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(ResolvePathsAndNamesTests)
  TESTRESULT = unittest.TextTestRunner(verbosity=2).run(SUITE)
  
