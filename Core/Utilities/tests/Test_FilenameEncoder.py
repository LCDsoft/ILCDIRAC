'''  Test Core.Utilities.FilenameEncoder.py  '''

import unittest

from ILCDIRAC.Core.Utilities.FilenameEncoder      import FilenameEncoder, decodeFilename, makeFilename

class FilenameEncoderTests( unittest.TestCase ):
  ''' FilenameEncoderTests, test FilenameEncoder '''
  def setUp(self):
    """
    Make fake files for the test
    """
    self.encoder = FilenameEncoder()
    self.maxDiff = None

  def test_makeSimNames(self):
    """test FilenameEncoder for stdhep file........................................................."""
    genfile  = "E0500-TDR_ws.Pae_ell.Gwhizard-1.95.eB.pL.I37538.001.stdhep"
    fileitem = decodeFilename( genfile )

    refitem  = { "E":"500-TDR_ws", "P":"ae_ell", "G":"whizard-1_95", "e":"B", "p":"L", "I":"37538", "n":"001" , "F":"stdhep" }
    self.assertEqual( fileitem, refitem )

    fileitem.update( {"s":"v01-14-01-p00", "m":"ILD_o1_v05", "d":"sim", "t":"7642", "j":"132" } )
    diritem={}
    diritem.update( fileitem )
    diritem.update( {"B":"/ilc/prod/ilc/ild/test/temp1",         # Base directory
                     "T":"%8.8d"% ( int(fileitem["t"]) ),        # Directory name for ProdID
                     "J":"%3.3d"% ( int(fileitem["j"])/ 1000 ),  # Sub-directory for job
                     "C":"1f_3f"}  )                             # event class

    energy_machine=diritem["E"].split("-")
    diritem.update( {"D":"SIM",                             # Meta value for data type
                     "w":energy_machine[0],                 # Energy
                     "o":energy_machine[1] } )              # Mcahine parameters

    simfile = self.encoder.convert( "sim", "file", fileitem)
    simdir  = self.encoder.convert( "sim", "dir",  diritem )
    simmeta = self.encoder.convert( "sim", "meta", diritem )

    reffile = "sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I37538.Pae_ell.eB.pL.n001.d_sim_7642_132.slcio"
    refdir  = "/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws/1f_3f/ILD_o1_v05/v01-14-01-p00/00007642/000"

    refmeta = { '/ilc/prod/ilc/ild/test/temp1/sim'                                                       : {'Datatype': 'SIM'},
                '/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws'                                            : {'Energy': '500', 'MachineParams': 'TDR_ws'},
                '/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws/1f_3f'                                      : {'EventClass': '1f_3f'},
                '/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws/1f_3f/ILD_o1_v05'                           : {'DetectorModel': 'ILD_o1_v05'},
                '/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws/1f_3f/ILD_o1_v05/v01-14-01-p00'             : {'ILDConfig': 'v01-14-01-p00'},
                '/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws/1f_3f/ILD_o1_v05/v01-14-01-p00/00007642'    : {'ProdID': '7642'},
                '/ilc/prod/ilc/ild/test/temp1/sim/500-TDR_ws/1f_3f/ILD_o1_v05/v01-14-01-p00/00007642/000': {'kJobNumber': '000'}}

    self.assertEqual( simfile, reffile)
    self.assertEqual( simdir , refdir )
    self.assertEqual( simmeta, refmeta)


  def test_makeRecDstNames(self):
    """Make Rec filenames from sim filename  ............................................................................"""
    simfile = "sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I37538.Pae_ell.eB.pL.n001.d_sim_8642_832.slcio"
    recitem = decodeFilename( simfile )

    refitem = { "s":"v01-14-01-p00", "m":"ILD_o1_v05", "E":"500-TDR_ws", "I":"37538", "P":"ae_ell", "e":"B", "p":"L",
                "n":"001", "d":"sim", "t":"8642", "j":"832", "F":"slcio" }
    self.assertEqual( recitem, refitem )

    recitem.update( {"r":"v01-16-p05_500", "d":"rec", "t":"8643", "j":"1854" } )
    recfile = self.encoder.convert( "rec", "file", recitem )

    diritem={}
    diritem.update( recitem )
    diritem.update( {"B":"/ilc/prod/ilc/ild/test/temp1",        # Base directory
                     "T":"%8.8d"% ( int(recitem["t"]) ),        # Directory name for ProdID
                     "J":"%3.3d"% ( int(recitem["j"])/ 1000 ),  # Sub-directory for job
                     "C":"1f_3f"}  )                            # event class

    energy_machine=diritem["E"].split("-")
    diritem.update( {"D":"REC",                             # Meta value for data type
                     "w":energy_machine[0],                 # Energy
                     "o":energy_machine[1] } )              # Mcahine parameters

    recdir = self.encoder.convert( "rec", "dir", diritem)
    recmeta = self.encoder.convert( "rec", "meta", diritem )

    reffile = "rv01-16-p05_500.sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I37538.Pae_ell.eB.pL.n001.d_rec_8643_1854.slcio"
    refdir  = "/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500/00008643/001"

    refmeta ={ '/ilc/prod/ilc/ild/test/temp1/rec'                                                        : {'Datatype': 'REC'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws'                                             : {'Energy': '500','MachineParams': 'TDR_ws'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/1f_3f'                                       : {'EventClass': '1f_3f'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/1f_3f/ILD_o1_v05'                            : {'DetectorModel': 'ILD_o1_v05'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500'             : {'ILDConfig': 'v01-16-p05_500'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500/00008643'    : {'ProdID': '8643'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500/00008643/001': {'kJobNumber': '001'}}

    self.assertEqual( recfile, reffile)
    self.assertEqual( recdir,  refdir )
    self.assertEqual( recmeta, refmeta)


    dstitem = {}
    dstitem.update( recitem )
    dstitem.update( {"r":"v01-16-p05_500", "d":"dst", "t":"8645", "j":"711" } )
    dstfile = self.encoder.convert( "dst", "file", dstitem )
    self.assertEqual( dstfile, "rv01-16-p05_500.sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I37538.Pae_ell.eB.pL.n001.d_dst_8645_711.slcio")

    dstdiritem={}
    dstdiritem.update( diritem )
    dstdiritem.update( { "d":dstitem["d"], "D":"DST", "t":dstitem["t"], "j":dstitem["j"],
                         "T":"%8.8d"% ( int(dstitem["t"]) ),        # Directory name for ProdID
                         "J":"%3.3d"% ( int(dstitem["j"])/ 1000 )   # Sub-directory for job
                       } )

    dstdir = self.encoder.convert( "dst", "dir", dstdiritem)
    self.assertEqual( dstdir, "/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500/00008645/000")

    dstmeta = self.encoder.convert( "dst", "meta", dstdiritem )
    refmeta= { '/ilc/prod/ilc/ild/test/temp1/dst'                                                        : {'Datatype': 'DST'},
               '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws'                                             : {'Energy': '500', 'MachineParams': 'TDR_ws'},
               '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/1f_3f'                                       : {'EventClass': '1f_3f'},
               '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/1f_3f/ILD_o1_v05'                            : {'DetectorModel': 'ILD_o1_v05'},
               '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500'             : {'ILDConfig': 'v01-16-p05_500'},
               '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500/00008645'    : {'ProdID': '8645'},
               '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/1f_3f/ILD_o1_v05/v01-16-p05_500/00008645/000': {'kJobNumber': '000'}}
    self.assertEqual( dstmeta, refmeta )


  def test_makeRecDstNamesFromOldILDDiracSimFile(self):
    """Make Rec filenames from past dirac sim filename  ............................................................................"""
    simfile = "sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I108687.P6f_xxxyyx.eL.pR_sim_6714_1.slcio"
    recitem = decodeFilename( simfile )

    refitem = { "s":"v01-14-01-p00", "m":"ILD_o1_v05", "E":"500-TDR_ws", "I":"108687", "P":"6f_xxxyyx", "e":"L", "p":"R",
                "d":"sim", "t":"6714", "j":"1", "F":"slcio" }
    self.assertEqual( recitem, refitem )
    self.assertNotIn( "n", recitem ) # No stdhep filenumber information

    recitem.update( {"r":"v01-16-p05_500", "d":"rec", "t":"8643", "j":"1854" } )

    diritem={}
    diritem.update( recitem )
    diritem.update( {"B":"/ilc/prod/ilc/ild/test/temp1",        # Base directory
                     "T":"%8.8d"% ( int(recitem["t"]) ),        # Directory name for ProdID
                     "J":"%3.3d"% ( int(recitem["j"])/ 1000 ),  # Sub-directory for job
                     "C":"6f_xxWW"}  )                            # event class

    energy_machine=diritem["E"].split("-")
    diritem.update( {"D":"REC",                             # Meta value for data type
                     "w":energy_machine[0],                 # Energy
                     "o":energy_machine[1] } )              # Mcahine parameters

    fe      = FilenameEncoder()
    fe.rules["rec"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.d_%d_%t_%j.slcio"  # Apply special rule, because "n" infor. not available.
    fe.rules["dst"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.d_%d_%t_%j.slcio"

    recfile = fe.convert( "rec", "file", recitem )
    recdir  = fe.convert( "rec", "dir" , diritem)
    recmeta = fe.convert( "rec", "meta", diritem )

    reffile = "rv01-16-p05_500.sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I108687.P6f_xxxyyx.eL.pR.d_rec_8643_1854.slcio"
    refdir  = "/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500/00008643/001"
    refmeta = {'/ilc/prod/ilc/ild/test/temp1/rec'                                                          : {'Datatype': 'REC'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws'                                               : {'Energy': '500','MachineParams': 'TDR_ws'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/6f_xxWW'                                       : {'EventClass': '6f_xxWW'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/6f_xxWW/ILD_o1_v05'                            : {'DetectorModel': 'ILD_o1_v05'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500'             : {'ILDConfig': 'v01-16-p05_500'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500/00008643'    : {'ProdID': '8643'},
               '/ilc/prod/ilc/ild/test/temp1/rec/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500/00008643/001': {'kJobNumber': '001'}}

    self.assertEqual( recfile, reffile)
    self.assertEqual( recdir,  refdir )
    self.assertEqual( recmeta, refmeta)

    dstitem = {}
    dstitem.update( recitem )
    dstitem.update( {"r":"v01-16-p05_500", "d":"dst", "t":"8645", "j":"711" } )
    dstfile = fe.convert( "dst", "file", dstitem )

    dstdiritem={}
    dstdiritem.update( diritem )
    dstdiritem.update( { "d":dstitem["d"], "D":"DST", "t":dstitem["t"], "j":dstitem["j"],
                         "T":"%8.8d"% ( int(dstitem["t"]) ),        # Directory name for ProdID
                         "J":"%3.3d"% ( int(dstitem["j"])/ 1000 )   # Sub-directory for job
                       } )

    dstdir  = fe.convert( "dst", "dir", dstdiritem)
    dstmeta = fe.convert( "dst", "meta", dstdiritem )

    reffile = "rv01-16-p05_500.sv01-14-01-p00.mILD_o1_v05.E500-TDR_ws.I108687.P6f_xxxyyx.eL.pR.d_dst_8645_711.slcio"
    refdir  = "/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500/00008645/000"
    refmeta = { '/ilc/prod/ilc/ild/test/temp1/dst'                                                          : {'Datatype': 'DST'},
                '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws'                                               : {'Energy': '500', 'MachineParams': 'TDR_ws'},
                '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/6f_xxWW'                                       : {'EventClass': '6f_xxWW'},
                '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/6f_xxWW/ILD_o1_v05'                            : {'DetectorModel': 'ILD_o1_v05'},
                '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500'             : {'ILDConfig': 'v01-16-p05_500'},
                '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500/00008645'    : {'ProdID': '8645'},
                '/ilc/prod/ilc/ild/test/temp1/dst/500-TDR_ws/6f_xxWW/ILD_o1_v05/v01-16-p05_500/00008645/000': {'kJobNumber': '000'}}

    self.assertEqual( dstfile, reffile)
    self.assertEqual( dstdir,  refdir )
    self.assertEqual( dstmeta, refmeta)

    del fe

  def test_makeRecDstfromDBDSim(self):

    simfile = "sv01-14-01-p00.mILD_o1_v05.E1000-B1b_ws.I200006.P4f_ww_h.eL.pR-00262.slcio"

    fe = FilenameEncoder()
    fe.rules["rec"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.d_%d_%t_%j.slcio"  # Apply special rule, because "n" infor. not available.
    fe.rules["dst"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.d_%d_%t_%j.slcio"

# Encode sim filename in order to build rec/dst files, directories
    recitem=decodeFilename(simfile)

    refitem = { "s":"v01-14-01-p00", "m":"ILD_o1_v05", "E":"1000-B1b_ws", "I":"200006", "P":"4f_ww_h",
                "e":"L", "p":"R", "j":"00262", "F":"slcio" }
    self.assertEqual( recitem, refitem )

    recitem["r"]="v01-16-p03"  # ILDConfig version for Marlin
    recitem["d"]="rec"  # data type
    recitem["t"]="7654" # #Production ID
    recitem["j"]="3232" # Job number
    recdiritem = recitem
    recdiritem["B"]="/ilc/prod/ilc/mc-dbd/ild"
    recdiritem["T"]="%8.8d"% ( int(recitem["t"]) )
    recdiritem["J"]="%3.3d"% ( int(recitem["j"])/ 1000 )
    recdiritem["C"]="4f_WW_hadronic"
    recmetaitem = {}
    recmetaitem.update(recdiritem)
    recmetaitem["D"]="REC"
    energy_machine=recdiritem["E"].split("-")             #
    recmetaitem["w"]=energy_machine[0]                 # Energy
    recmetaitem["o"]=energy_machine[1]                 # Mcahine parameters
#
    recfile = fe.convert( "rec", "file", recitem )
    recdir  = fe.convert( "rec", "dir",  recdiritem )
    recmeta = fe.convert( "rec", "meta", recmetaitem )

    refrecfile="rv01-16-p03.sv01-14-01-p00.mILD_o1_v05.E1000-B1b_ws.I200006.P4f_ww_h.eL.pR.d_rec_7654_3232.slcio"
    refrecdir="/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws/4f_WW_hadronic/ILD_o1_v05/v01-16-p03/00007654/003"

    refrecmeta={'/ilc/prod/ilc/mc-dbd/ild/rec'                                                              : {'Datatype': 'REC'},
                '/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws'                                                  : {'Energy': '1000','MachineParams': 'B1b_ws'},
                '/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws/4f_WW_hadronic'                                   : {'EventClass': '4f_WW_hadronic'},
                '/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws/4f_WW_hadronic/ILD_o1_v05'                        : {'DetectorModel': 'ILD_o1_v05'},
                '/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws/4f_WW_hadronic/ILD_o1_v05/v01-16-p03'             : {'ILDConfig': 'v01-16-p03'},
                '/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws/4f_WW_hadronic/ILD_o1_v05/v01-16-p03/00007654'    : {'ProdID': '7654'},
                '/ilc/prod/ilc/mc-dbd/ild/rec/1000-B1b_ws/4f_WW_hadronic/ILD_o1_v05/v01-16-p03/00007654/003': {'kJobNumber': '003'}  }

    self.assertEqual( recfile, refrecfile)
    self.assertEqual( recdir,  refrecdir )
    self.assertEqual( recmeta, refrecmeta)

    del fe

  def test_getputRules(self):

    newrule = {"dstm":{"file":""}}
    newrule["dstm"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.n%n.slcio"
    newrule["dstm"]["dir"]  = "%B/%d/%E/%C/%m/%r"
    newrule["dstm"]["meta"] = {"%B/%d"                  :{"Datatype":"%D"},
                               "%B/%d/%E"               :{"Energy":"%w", "MachineParams":"%o"},
                               "%B/%d/%E/%C"            :{"EventClass":"%C"},
                               "%B/%d/%E/%C/%m"         :{"DetectorModel":"%m"},
                               "%B/%d/%E/%C/%m/%r"      :{"ILDConfig":"%r"} }
    self.encoder.defineRules(newrule, datatype="dstm")

    self.assertIn( "dstm", self.encoder.rules )
    self.assertIn( "file", self.encoder.rules["dstm"] )
    self.assertIn( "dir" , self.encoder.rules["dstm"] )
    self.assertIn( "meta", self.encoder.rules["dstm"] )

    dstmfilerule = self.encoder.getARule("dstm", category="file")
    self.assertEqual( dstmfilerule, newrule["dstm"]["file"] )
    dstmrule = self.encoder.getARule("dstm")
    self.assertEqual( dstmrule, newrule["dstm"] )

    self.encoder.defineRules( "abcde", datatype="dstm", category="file")
    self.assertNotEqual( self.encoder.rules["dstm"], newrule["dstm"]["file"] )

    self.assertIn( "sim", self.encoder.rules )
    self.encoder.defineRules(newrule)
    self.assertNotIn( "sim", self.encoder.rules )

  def test_makeFilename_double_percents(self) :
    fileformat = "E%Energy%-%MachineParams%.P%P"
    filemeta={"Energy":"250", "MachineParams":"TDR_ws", "P":"4f_WW_h"}
    convstr = makeFilename( fileformat, filemeta, preonly=False)
    self.assertEqual(convstr, "E250-TDR_ws.P%P")

    conv2nd = makeFilename( convstr, filemeta )
    self.assertEqual(conv2nd, "E250-TDR_ws.P4f_WW_h")


if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( FilenameEncoderTests )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
