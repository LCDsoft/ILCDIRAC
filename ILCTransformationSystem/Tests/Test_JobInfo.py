"""Test the JobInfo"""

import unittest
import sys
from StringIO import StringIO

from mock import MagicMock as Mock

from DIRAC import S_OK, S_ERROR, gLogger

from ILCDIRAC.ILCTransformationSystem.Utilities.JobInfo import TaskInfoException, JobInfo

gLogger.setLevel("DEBUG")

__RCSID__ = "$Id$"
#pylint: disable=W0212, E1101
class TestJI( unittest.TestCase ):
  """Test the DataRecoveryAgent"""
  dra = None


  def setUp( self ):
    self.jbi = JobInfo( jobID=123, status="Failed", tID=1234, tType = "MCReconstruction" )
    self.jobMonMock = Mock()

    self.jdl2 = """
[
  LogTargetPath = "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/00006326_015.tar";
  Executable = "$DIRACROOT/scripts/dirac-jobexec";
  TaskID = 15;
  SoftwareDistModule = "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation";
  JobName = "00006326_00000015";
  Priority = 1;
  Platform = "x86_64-slc5-gcc43-opt";
  JobRequirements =
  [
    OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
    VirtualOrganization = "ilc";
    Setup = "ILC-Production";
    CPUTime = 300000;
    OwnerGroup = "ilc_prod";
    Platforms =
    {
      "x86_64-slc6-gcc44-opt",
      "x86_64-slc5-gcc43-opt",
      "slc5_amd64_gcc43",
      "Linux_x86_64_glibc-2.12",
      "Linux_x86_64_glibc-2.5"
    };
    UserPriority = 1;
    Sites =
    {
      "LCG.LAPP.fr",
      "LCG.Freiburg.de",
      "OSG.BNL.us",
      "LCG.Tau.il",
      "LCG.Weizmann.il",
      "OSG.TTU.us",
      "LCG.IFCA-LCG2.es",
      "OSG.CIT.us",
      "LCG.Bristol.uk",
      "LCG.CERN.ch",
      "LCG.Oxford.uk",
      "LCG.IN2P3-IPNL.fr",
      "LCG.Manchester.uk",
      "LCG.Birmingham.uk",
      "LCG.UKI-SOUTHGRID-CAM-HEP.uk",
      "LCG.Bonn.de",
      "LCG.USC-LCG2.es",
      "OSG.UCSDT2.us",
      "OSG.MIT.us",
      "LCG.NIPNE.ro",
      "OSG.SPRACE.br",
      "LCG.UKI-NORTHGRID-LIV-HEP.uk",
      "LCG.GRIF.fr",
      "LCG.UKI-LT2-RHUL.uk",
      "OSG.PNNL.us",
      "LCG.DESY-HH.de",
      "LCG.IN2P3-CC.fr",
      "OSG.Arlington.us",
      "OSG.GridUNESP_CENTRAL.br",
      "LCG.QMUL.uk",
      "LCG.Brunel.uk",
      "OSG.UConn.us",
      "LCG.TECHNION.il",
      "TEST.CERN.ch",
      "LCG.SCOTGRIDDURHAM.uk",
      "LCG.Glasgow.uk",
      "LCG.UKI-LT2-IC-HEP.uk",
      "LCG.RAL-LCG2.uk",
      "LCG.Cracow.pl",
      "OSG.FNAL_FERMIGRID.us",
      "LCG.DESYZN.de",
      "LCG.UKI-SOUTHGRID-RALPP.uk"
    };
    BannedSites = "LCG.KEK.jp";
    SubmitPools = "gLite";
    JobTypes = "MCReconstruction_Overlay";
  ];
  Arguments = "jobDescription.xml -o LogLevel=verbose";
  SoftwarePackages =
  {
    "overlayinput.1",
    "marlin.v0111Prod"
  };
  DebugLFNs = "";
  Status = "Created";
  InputDataModule = "DIRAC.WorkloadManagementSystem.Client.InputDataResolution";
  BannedSites = "LCG.KEK.jp";
  LogLevel = "verbose";
  InputSandbox =
  {
    "jobDescription.xml",
    "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/5d3/92f/5d392f5266a796018ab6774ef84cbd31.tar.bz2"
  };
  SubmitPools = "gLite";
  OwnerName = "sailer";
  StdOutput = "std.out";
  JobType = "MCReconstruction_Overlay";
  GridEnv = "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example";
  TransformationID = 6326;
  DIRACSetup = "ILC-Production";
  StdError = "std.err";
  IS_PROD = "True";
  CPUTime = 300000;
  OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
  JobGroup = 00006326;
  OutputSandbox =
  {
    "std.err",
    "std.out"
  };
  JobID = 15756436;
  Origin = "DIRAC";
  VirtualOrganization = "ilc";
  ProductionOutputData =
  {
    "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/000/yyveyx_o_rec_6326_15.slcio",
    "/ilc/prod/clic/500gev/yyveyx_o/ILD/DST/00006326/000/yyveyx_o_dst_6326_15.slcio"
  };
  Site = "ANY";
  OwnerGroup = "ilc_prod";
  Owner = "sailer";
  MaxCPUTime = 300000;
  LogFilePath = "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/000";
  InputData = "/ilc/prod/clic/500gev/yyveyx_o/ILD/SIM/00006325/000/yyveyx_o_sim_6325_17.slcio";
]
    """

    self.jdlBrokenContent = """
[
  LogTargetPath = "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/00006326_015.tar";
  Executable = "$DIRACROOT/scripts/dirac-jobexec";
  TaskID = muahahaha;
  SoftwareDistModule = "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation";
  JobName = "00006326_00000015";
  Priority = 1;
  Platform = "x86_64-slc5-gcc43-opt";
  JobRequirements =
  [
    OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
    VirtualOrganization = "ilc";
    Setup = "ILC-Production";
    CPUTime = 300000;
    OwnerGroup = "ilc_prod";
    Platforms =
    {
      "x86_64-slc6-gcc44-opt",
      "x86_64-slc5-gcc43-opt",
      "slc5_amd64_gcc43",
      "Linux_x86_64_glibc-2.12",
      "Linux_x86_64_glibc-2.5"
    };
    UserPriority = 1;
    Sites =
    {
      "LCG.LAPP.fr",
      "LCG.Freiburg.de",
      "OSG.BNL.us",
      "LCG.Tau.il",
      "LCG.Weizmann.il",
      "OSG.TTU.us",
      "LCG.IFCA-LCG2.es",
      "OSG.CIT.us",
      "LCG.Bristol.uk",
      "LCG.CERN.ch",
      "LCG.Oxford.uk",
      "LCG.IN2P3-IPNL.fr",
      "LCG.Manchester.uk",
      "LCG.Birmingham.uk",
      "LCG.UKI-SOUTHGRID-CAM-HEP.uk",
      "LCG.Bonn.de",
      "LCG.USC-LCG2.es",
      "OSG.UCSDT2.us",
      "OSG.MIT.us",
      "LCG.NIPNE.ro",
      "OSG.SPRACE.br",
      "LCG.UKI-NORTHGRID-LIV-HEP.uk",
      "LCG.GRIF.fr",
      "LCG.UKI-LT2-RHUL.uk",
      "OSG.PNNL.us",
      "LCG.DESY-HH.de",
      "LCG.IN2P3-CC.fr",
      "OSG.Arlington.us",
      "OSG.GridUNESP_CENTRAL.br",
      "LCG.QMUL.uk",
      "LCG.Brunel.uk",
      "OSG.UConn.us",
      "LCG.TECHNION.il",
      "TEST.CERN.ch",
      "LCG.SCOTGRIDDURHAM.uk",
      "LCG.Glasgow.uk",
      "LCG.UKI-LT2-IC-HEP.uk",
      "LCG.RAL-LCG2.uk",
      "LCG.Cracow.pl",
      "OSG.FNAL_FERMIGRID.us",
      "LCG.DESYZN.de",
      "LCG.UKI-SOUTHGRID-RALPP.uk"
    };
    BannedSites = "LCG.KEK.jp";
    SubmitPools = "gLite";
    JobTypes = "MCReconstruction_Overlay";
  ];
  Arguments = "jobDescription.xml -o LogLevel=verbose";
  SoftwarePackages =
  {
    "overlayinput.1",
    "marlin.v0111Prod"
  };
  DebugLFNs = "";
  Status = "Created";
  InputDataModule = "DIRAC.WorkloadManagementSystem.Client.InputDataResolution";
  BannedSites = "LCG.KEK.jp";
  LogLevel = "verbose";
  InputSandbox =
  {
    "jobDescription.xml",
    "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/5d3/92f/5d392f5266a796018ab6774ef84cbd31.tar.bz2"
  };
  SubmitPools = "gLite";
  OwnerName = "sailer";
  StdOutput = "std.out";
  JobType = "MCReconstruction_Overlay";
  GridEnv = "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example";
  TransformationID = 6326;
  DIRACSetup = "ILC-Production";
  StdError = "std.err";
  IS_PROD = "True";
  CPUTime = 300000;
  OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
  JobGroup = 00006326;
  OutputSandbox =
  {
    "std.err",
    "std.out"
  };
  JobID = 15756436;
  Origin = "DIRAC";
  VirtualOrganization = "ilc";
  ProductionOutputData =
  {
    "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/000/yyveyx_o_rec_6326_15.slcio",
    "/ilc/prod/clic/500gev/yyveyx_o/ILD/DST/00006326/000/yyveyx_o_dst_6326_15.slcio"
  };
  Site = "ANY";
  OwnerGroup = "ilc_prod";
  Owner = "sailer";
  MaxCPUTime = 300000;
  LogFilePath = "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/000";
  InputData = "/ilc/prod/clic/500gev/yyveyx_o/ILD/SIM/00006325/000/yyveyx_o_sim_6325_17.slcio";
]
    """


    ### jdl with single outputdata
    self.jdl1 = """
[
  LogTargetPath = "/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/LOG/00006301_10256.tar";
  Executable = "$DIRACROOT/scripts/dirac-jobexec";
  TaskID = 10256;
  SoftwareDistModule = "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation";
  JobName = "00006301_00010256";
  Priority = 1;
  Platform = "x86_64-slc5-gcc43-opt";
  JobRequirements =
  [
    OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
    VirtualOrganization = "ilc";
    Setup = "ILC-Production";
    CPUTime = 300000;
    OwnerGroup = "ilc_prod";
    Platforms =
    {
      "x86_64-slc6-gcc44-opt",
      "x86_64-slc5-gcc43-opt",
      "slc5_amd64_gcc43",
      "Linux_x86_64_glibc-2.12",
      "Linux_x86_64_glibc-2.5"
    };
    UserPriority = 1;
    Sites =
    {
      "LCG.LAPP.fr",
      "LCG.Freiburg.de",
      "LCG.Tau.il",
      "LCG.Weizmann.il",
      "LCG.IFCA-LCG2.es",
      "LCG.Bristol.uk",
      "LCG.CERN.ch",
      "LCG.Oxford.uk",
      "LCG.Birmingham.uk",
      "LCG.UKI-SOUTHGRID-CAM-HEP.uk",
      "LCG.Bonn.de",
      "LCG.USC-LCG2.es",
      "LCG.Brunel.uk",
      "LCG.NIPNE.ro",
      "LCG.UKI-NORTHGRID-LIV-HEP.uk",
      "LCG.DESY-HH.de",
      "LCG.UKI-LT2-RHUL.uk",
      "LCG.GRIF.fr",
      "LCG.IN2P3-CC.fr",
      "LCG.QMUL.uk",
      "LCG.Manchester.uk",
      "LCG.TECHNION.il",
      "TEST.CERN.ch",
      "LCG.SCOTGRIDDURHAM.uk",
      "LCG.Glasgow.uk",
      "LCG.UKI-LT2-IC-HEP.uk",
      "LCG.RAL-LCG2.uk",
      "LCG.Cracow.pl",
      "LCG.IN2P3-IPNL.fr",
      "LCG.DESYZN.de",
      "LCG.UKI-SOUTHGRID-RALPP.uk"
    };
    BannedSites =
    {
      "OSG.BNL.us",
      "OSG.PNNL.us",
      "OSG.Arlington.us",
      "OSG.FNAL_FERMIGRID.us",
      "OSG.TTU.us",
      "LCG.KEK.jp",
      "OSG.GridUNESP_CENTRAL.br",
      "OSG.CIT.us",
      "OSG.UCSDT2.us",
      "OSG.UConn.us",
      "OSG.MIT.us",
      "OSG.SPRACE.br"
    };
    SubmitPools = "gLite";
    JobTypes = "MCSimulation";
  ];
  Arguments = "jobDescription.xml -o LogLevel=verbose";
  SoftwarePackages = "slic.v2r9p8";
  DebugLFNs = "";
  Status = "Created";
  InputDataModule = "DIRAC.WorkloadManagementSystem.Client.InputDataResolution";
  BannedSites =
  {
    "OSG.BNL.us",
    "OSG.PNNL.us",
    "OSG.Arlington.us",
    "OSG.FNAL_FERMIGRID.us",
    "OSG.TTU.us",
    "LCG.KEK.jp",
    "OSG.GridUNESP_CENTRAL.br",
    "OSG.CIT.us",
    "OSG.UCSDT2.us",
    "OSG.UConn.us",
    "OSG.MIT.us",
    "OSG.SPRACE.br"
  };
  LogLevel = "verbose";
  InputSandbox =
  {
    "jobDescription.xml",
    "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/042/d64/042d64cb0fe73720cbd114a73506c582.tar.bz2"
  };
  SubmitPools = "gLite";
  OwnerName = "sailer";
  StdOutput = "std.out";
  JobType = "MCSimulation";
  GridEnv = "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example";
  TransformationID = 6301;
  DIRACSetup = "ILC-Production";
  StdError = "std.err";
  IS_PROD = "True";
  CPUTime = 300000;
  OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
  JobGroup = 00006301;
  OutputSandbox =
  {
    "std.err",
    "std.out"
  };
  JobID = 15756456;
  Origin = "DIRAC";
  VirtualOrganization = "ilc";
  ProductionOutputData = "/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/010/e1e1_o_sim_6301_10256.slcio";
  Site = "ANY";
  OwnerGroup = "ilc_prod";
  Owner = "sailer";
  MaxCPUTime = 300000;
  LogFilePath = "/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/LOG/010";
  InputData = "/ilc/prod/clic/3tev/e1e1_o/gen/00006300/004/e1e1_o_gen_6300_4077.stdhep";
]
  """

    self.jdlNoInput = """
[
  LogTargetPath = "/ilc/prod/clic/1.4tev/ea_qqqqnu/gen/00006498/LOG/00006498_1307.tar";
  Executable = "$DIRACROOT/scripts/dirac-jobexec";
  TaskID = 1307;
  SoftwareDistModule = "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation";
  JobName = "00006498_00001307";
  Priority = 1;
  Platform = "x86_64-slc5-gcc43-opt";
  JobRequirements =
  [
    OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
    VirtualOrganization = "ilc";
    Setup = "ILC-Production";
    CPUTime = 300000;
    OwnerGroup = "ilc_prod";
    Platforms =
    {
      "x86_64-slc6-gcc44-opt",
      "x86_64-slc5-gcc43-opt",
      "slc5_amd64_gcc43",
      "Linux_x86_64_glibc-2.12",
      "Linux_x86_64_glibc-2.5"
    };
    UserPriority = 1;
    BannedSites = "LCG.KEK.jp";
    SubmitPools = "gLite";
    JobTypes = "MCGeneration";
  ];
  Arguments = "jobDescription.xml -o LogLevel=verbose";
  SoftwarePackages = "whizard.SM_V57";
  DebugLFNs = "";
  Status = "Created";
  InputDataModule = "DIRAC.WorkloadManagementSystem.Client.InputDataResolution";
  BannedSites = "LCG.KEK.jp";
  LogLevel = "verbose";
  InputSandbox =
  {
    "jobDescription.xml",
    "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/b2a/d98/b2ad98c3e240361a4253c4bb277be478.tar.bz2"
  };
  SubmitPools = "gLite";
  OwnerName = "sailer";
  StdOutput = "std.out";
  JobType = "MCGeneration";
  GridEnv = "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example";
  TransformationID = 6498;
  DIRACSetup = "ILC-Production";
  StdError = "std.err";
  IS_PROD = "True";
  CPUTime = 300000;
  OwnerDN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer";
  JobGroup = 00006498;
  OutputSandbox =
  {
    "std.err",
    "std.out"
  };
  JobID = 15762268;
  Origin = "DIRAC";
  VirtualOrganization = "ilc";
  ProductionOutputData = "/ilc/prod/clic/1.4tev/ea_qqqqnu/gen/00006498/001/ea_qqqqnu_gen_6498_1307.stdhep";
  Site = "ANY";
  OwnerGroup = "ilc_prod";
  Owner = "sailer";
  MaxCPUTime = 300000;
  LogFilePath = "/ilc/prod/clic/1.4tev/ea_qqqqnu/gen/00006498/LOG/001";
  InputData = "";
]
  """

  def tearDown( self ):
    pass


  def test_Init( self ):
    """ILCTransformation.Utilities.JobInfo init ...................................................."""
    self.assertIsNone( self.jbi.outputFiles )
    self.assertFalse( self.jbi.pendingRequest )

  def test_allFilesExist( self ):
    """ILCTransformation.Utilities.JobInfo.allFilesExist............................................"""
    self.jbi.outputFileStatus = ["Exists", "Exists"]
    self.assertTrue( self.jbi.allFilesExist() )
    self.jbi.outputFileStatus = ["Exists", "Missing"]
    self.assertFalse( self.jbi.allFilesExist() )
    self.jbi.outputFileStatus = ["Missing", "Exists"]
    self.assertFalse( self.jbi.allFilesExist() )
    self.jbi.outputFileStatus = ["Missing", "Missing"]
    self.assertFalse( self.jbi.allFilesExist() )
    self.jbi.outputFileStatus = []
    self.assertTrue( self.jbi.allFilesExist() )

  def test_allFilesMissing( self ):
    """ILCTransformation.Utilities.JobInfo.allFilesMissing.........................................."""
    self.jbi.outputFileStatus = ["Exists", "Exists"]
    self.assertFalse( self.jbi.allFilesMissing() )
    self.jbi.outputFileStatus = ["Exists", "Missing"]
    self.assertFalse( self.jbi.allFilesMissing() )
    self.jbi.outputFileStatus = ["Missing", "Exists"]
    self.assertFalse( self.jbi.allFilesMissing() )
    self.jbi.outputFileStatus = ["Missing", "Missing"]
    self.assertTrue( self.jbi.allFilesMissing() )
    self.jbi.outputFileStatus = []
    self.assertFalse( self.jbi.allFilesMissing() )

  def test_someFilesMissing( self ):
    """ILCTransformation.Utilities.JobInfo.someFilesMissing........................................."""
    self.jbi.outputFileStatus = ["Exists", "Exists"]
    self.assertFalse( self.jbi.someFilesMissing() )
    self.jbi.outputFileStatus = ["Exists", "Missing"]
    self.assertTrue( self.jbi.someFilesMissing() )
    self.jbi.outputFileStatus = ["Missing", "Exists"]
    self.assertTrue( self.jbi.someFilesMissing() )
    self.jbi.outputFileStatus = ["Missing", "Missing"]
    self.assertFalse( self.jbi.someFilesMissing() )
    self.jbi.outputFileStatus = []
    self.assertFalse( self.jbi.someFilesMissing() )

  def test_getJDL( self ):
    """ILCTransformation.Utilities.JobInfo.getJDL..................................................."""

    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdl1 )
    jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    self.assertIsInstance( jdlList, list )

    self.jobMonMock.getJobJDL.return_value = S_ERROR( "no mon" )
    with self.assertRaises( RuntimeError ) as contextManagedException:
      jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    self.assertIn( "Failed to get jobJDL" , str(contextManagedException.exception) )

  def test_getTaskInfo( self ):
    """ILCTransformation.Utilities.JobInfo.getTaskInfo.............................................."""
    ## task is only one
    self.jbi.taskID = 1234
    self.jbi.inputFile = "lfn"
    tasksDict = { 1234:  dict( FileID=123456, LFN="lfn", Status="Assigned" ) }
    lfnTaskDict = {}
    self.jbi.getTaskInfo( tasksDict, lfnTaskDict )
    self.assertEqual( self.jbi.fileStatus, "Assigned" )
    self.assertEqual( self.jbi.taskFileID, 123456 )
    self.assertIsNone( self.jbi.otherTasks )

    ## there are other tasks
    self.jbi.taskID = 1234
    self.jbi.inputFile = "lfn"
    tasksDict = { 12:  dict( FileID=123456, LFN="lfn", Status="Processed" ) }
    lfnTaskDict = { "lfn": 12 }
    self.jbi.getTaskInfo( tasksDict, lfnTaskDict )
    self.assertEqual( self.jbi.fileStatus, "Processed" )
    self.assertEqual( self.jbi.taskFileID, 123456 )
    self.assertEqual( self.jbi.otherTasks, 12 )

    ## raise
    self.jbi.taskID = 1234
    self.jbi.inputFile = ""
    tasksDict = { 1234:  dict( FileID=123456, LFN="lfn", Status="Processed" ) }
    lfnTaskDict = {}
    with self.assertRaisesRegexp( TaskInfoException, "InputFiles do not agree" ):
      self.jbi.getTaskInfo( tasksDict, lfnTaskDict )

  def test_getJobInformation( self ):
    """ILCTransformation.Utilities.JobInfo.getJobInformation........................................"""
    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdl1 )
    self.jbi.getJobInformation( self.jobMonMock )
    self.assertEqual( self.jbi.outputFiles , ["/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/010/e1e1_o_sim_6301_10256.slcio"] )
    self.assertEqual( 10256, self.jbi.taskID )
    self.assertEqual( self.jbi.inputFile , "/ilc/prod/clic/3tev/e1e1_o/gen/00006300/004/e1e1_o_gen_6300_4077.stdhep" )

    ##empty jdl
    self.setUp()
    self.jobMonMock.getJobJDL.return_value = S_OK( "" )
    self.jbi.getJobInformation( self.jobMonMock )
    self.assertEqual( self.jbi.outputFiles , [] )
    self.assertIsNone( self.jbi.taskID )
    self.assertIsNone( self.jbi.inputFile )


  def test_getOutputFiles( self ):
    """ILCTransformation.Utilities.JobInfo.getOutputFiles..........................................."""
    ## singleLFN
    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdl1 )
    jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    self.jbi._JobInfo__getOutputFiles( jdlList )
    self.assertEqual( self.jbi.outputFiles , ["/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/010/e1e1_o_sim_6301_10256.slcio"] )

    ## two LFNs
    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdl2 )
    jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    self.jbi._JobInfo__getOutputFiles( jdlList )
    self.assertEqual( self.jbi.outputFiles , [ "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/000/yyveyx_o_rec_6326_15.slcio",
                                               "/ilc/prod/clic/500gev/yyveyx_o/ILD/DST/00006326/000/yyveyx_o_dst_6326_15.slcio"
                                             ]
                    )

  def test_getTaskID( self ):
    """ILCTransformation.Utilities.JobInfo.getTaskID................................................"""
    ## singleLFN
    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdl1 )
    jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    self.jbi._JobInfo__getTaskID( jdlList )
    self.assertEqual( 10256, self.jbi.taskID )

    ## broken jdl
    out = StringIO()
    sys.stdout = out
    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdlBrokenContent )
    jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    with self.assertRaises( ValueError ):
      self.jbi._JobInfo__getTaskID( jdlList )
    self.assertIn( "*"*80 , out.getvalue() )
    self.assertIn( "ERROR" , out.getvalue() )

  def test_getInputFile( self ):
    """ILCTransformation.Utilities.JobInfo.getTaskID................................................"""

    ## singleLFN
    self.jobMonMock.getJobJDL.return_value = S_OK( self.jdl1 )
    jdlList = self.jbi._JobInfo__getJDL( self.jobMonMock )
    self.jbi._JobInfo__getInputFile( jdlList )
    self.assertEqual( self.jbi.inputFile , "/ilc/prod/clic/3tev/e1e1_o/gen/00006300/004/e1e1_o_gen_6300_4077.stdhep" )

    ret = self.jbi._JobInfo__getSingleLFN( [] )
    self.assertEqual( ret, [] )

  def test_checkRequests( self ):
    """ILCTransformation.Utilities.JobInfo.checkRequests............................................"""

    ## no request
    reqMock = Mock()
    reqMock.Status = "Done"
    reqClient = Mock( name="ReqMock" )
    reqClient.readRequestsForJobs.return_value = S_OK( {"Successful":{ } } )
    self.jbi.jobID = 1234
    self.jbi.checkRequests( reqClient )
    self.assertFalse( self.jbi.pendingRequest )

    ## no pending request
    reqMock = Mock()
    reqMock.Status = "Done"
    reqClient = Mock( name="ReqMock" )
    reqClient.readRequestsForJobs.return_value = S_OK( {"Successful":{1234: reqMock } } )
    self.jbi.jobID = 1234
    self.jbi.checkRequests( reqClient )
    self.assertFalse( self.jbi.pendingRequest )

    ## pending request
    reqMock = Mock()
    reqMock.Status = "Waiting"
    reqClient = Mock( name="ReqMock" )
    reqClient.readRequestsForJobs.return_value = S_OK( {"Successful":{1234: reqMock } } )
    self.jbi.jobID = 1234
    self.jbi.checkRequests( reqClient )
    self.assertTrue( self.jbi.pendingRequest )

    ## Failed to get Request
    reqMock = Mock()
    reqMock.Status = "Waiting"
    reqClient = Mock( name="ReqMock" )
    reqClient.readRequestsForJobs.return_value = S_ERROR( "Request Denied" )
    with self.assertRaises( RuntimeError) as cme:
      self.jbi.checkRequests( reqClient )
    self.assertIn( "Failed to check Requests" , str(cme.exception) )

  def test_checkFileExistance( self ):
    """ILCTransformation.Utilities.JobInfo.checkFileExistance......................................."""

    ## input and output files
    repStatus = { "Successful": { "inputFile": True, "outputFile1": False, "outputFile2": True } }
    self.jbi.inputFile = "inputFile"
    self.jbi.outputFiles = ["outputFile1", "outputFile2", "unknownFile"]
    fcMock = Mock()
    fcMock.exists.return_value = S_OK( repStatus )
    self.jbi.checkFileExistance( fcMock )
    self.assertTrue( self.jbi.inputFileExists )
    self.assertEqual( self.jbi.outputFileStatus, ["Missing", "Exists", "Unknown"] )

    ## just output files
    self.setUp()
    repStatus = { "Successful": { "inputFile": True, "outputFile1": False, "outputFile2": True } }
    self.jbi.inputFile = ""
    self.jbi.outputFiles = ["outputFile1", "outputFile2", "unknownFile"]
    fcMock = Mock()
    fcMock.exists.return_value = S_OK( repStatus )
    self.jbi.checkFileExistance( fcMock )
    self.assertEqual( self.jbi.outputFileStatus, ["Missing", "Exists", "Unknown"] )

    ## fcClient Error
    self.setUp()
    repStatus = { "Successful": { "inputFile": True, "outputFile1": False, "outputFile2": True } }
    self.jbi.inputFile = ""
    self.jbi.outputFiles = ["outputFile1", "outputFile2", "unknownFile"]
    fcMock = Mock()
    fcMock.exists.return_value = S_ERROR( "No FC" )
    with self.assertRaises( RuntimeError ) as cme:
      self.jbi.checkFileExistance( fcMock )
    self.assertIn( "Failed to check existance: No FC", str(cme.exception) )


  def test__str__( self ):
    """ILCTransformation.Utilities.JobInfo.__str__.................................................."""
    jbi = JobInfo( jobID=123, status="Failed", tID=1234, tType = "MCReconstruction" )
    jbi.tID = 1234
    jbi.taskID = 5678
    jbi.fileStatus = "Assigned"
    jbi.otherTasks = True
    jbi.inputFile = "inputFile"
    jbi.outputFiles = ["outputFile"]
    info = str(jbi)
    self.assertIn( "123: Failed MCReconstruction Transformation: 1234 -- 5678 TaskStatus: Assigned", info )

    ## other tasks exist, no tID or taskID
    jbi = JobInfo( jobID=123, status="Failed", tID=1234, tType = "MCReconstruction" )
    jbi.fileStatus = "Assigned"
    jbi.otherTasks = True
    jbi.inputFile = "inputFile"
    jbi.outputFiles = ["outputFile"]
    info = str(jbi)
    self.assertIn( "123: FailedTaskStatus: Assigned (Last task 1)", info )

    ## no otherTasks
    jbi = JobInfo( jobID=123, status="Failed", tID=1234, tType = "MCReconstruction" )
    jbi.fileStatus = "Assigned"
    jbi.otherTasks = False
    jbi.inputFile = "inputFile"
    jbi.outputFiles = ["outputFile"]
    info = str(jbi)
    self.assertIn( "123: FailedTaskStatus: Assigned \n---> inputFile: inputFile (False)\n", info )

    ## pending Requests
    jbi = JobInfo( jobID=123, status="Failed", tID=1234, tType = "MCReconstruction" )
    jbi.pendingRequest = True
    info = str(jbi)
    self.assertIn( "PENDING REQUEST IGNORE THIS JOB", info )

    ## pending Requests
    jbi = JobInfo( jobID=123, status="Failed", tID=1234, tType = "MCReconstruction" )
    jbi.pendingRequest = False
    info = str(jbi)
    self.assertIn( " No Pending Requests", info )

  def test_TaskInfoException( self ):
    """ILCTransformation.Utilities.JobInfo.TaskInfoException........................................"""
    tie = TaskInfoException( "notTasked" )
    self.assertIsInstance( tie, Exception )
    self.assertIn( "notTasked", str(tie) )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestJI )
  TESTRESULT = unittest.TextTestRunner( verbosity = 3 ).run( SUITE )
