Complete Example Submission Scripts
===================================


Running Marlin
--------------

.. code:: python

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin

  dIlc = DiracILC()

  job = UserJob()
  job.setOutputSandbox(["*.log", "*.sh", "*.py"])
  job.setJobGroup( "myMarlinRun1" )
  job.setName( "MyMarlinJob1" )

  marl = Marlin ()
  marl.setVersion("ILCSoft-01-17-09")

  marl.setInputFile(["./tempOut.slcio", "./tempOut2.slcio"])
  marl.setSteeringFile("clic_ild_cdr_steering.xml")
  marl.setGearFile("clic_ild_cdr.gear")
  marl.setNumberOfEvents(3)

  job.append(marl)
  job.submit(dIlc)
    
Running DDSim
-------------

.. code:: python

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import  UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim

  dIlc = DiracILC()

  job = UserJob()
  job.setJobGroup( "DDsimTest" )
  job.setOutputSandbox(["*.log", "*.sh", "*.py"])
  job.setCPUTime(100 * 4 * 60)
  job.setName( "DDTest_12" )

  outputFilename = "output_12.slcio"
  job.setOutputData(outputFilename, OutputPath="testJobs/Run3" , OutputSE="CERN-DST-EOS")

  D = DDSim()
  D.setOutputFile( outputFilename )
  D.setVersion("ILCSoft-01-17-09_HEAD160315_2")
  D.setDetectorModel("CLIC_o2_v04")
  D.setNumberOfEvents( 10 )
  D.setExtraCLIArguments( "--enableGun --gun.particle mu-" )

  job.append(D)
  job.submit(dIlc)


Running Marlin and then DDSim
-----------------------------

.. code:: python

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  
  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import DDSim, Marlin
  
  dIlc = DiracILC( False )
  
  myJob = UserJob()
  myJob.setILDConfig( "v01-17-09_lcgeo" )
  recoFile = "reco.slcio"
  myJob.setOutputData( recoFile, OutputPath="testRepl", OutputSE="CERN-DST-EOS" )

  ddsim = DDSim()
  ddsim.setVersion("ILCSoft-2016-06-22_gcc48")
  ddsim.setDetectorModel("ILD_o1_v05")
  ddsim.setInputFile("LFN:/ilc/prod/clic/500gev/Z_uds/gen/0/00.stdhep")
  ddsim.setNumberOfEvents(1)
  ddsim.setSteeringFile( "ddsim_steer.py" )
  ddsim.setOutputFile( "ddsimout.slcio" )

  myJob.append(ddsim)

  marlin = Marlin()
  marlin.setVersion( "ILCSoft-2016-06-22_gcc48" )
  marlin.getInputFromApp( ddsim )
  marlin.setSteeringFile( "bbudsc_3evt_stdreco_dd4hep.xml" )
  marlin.setDetectorModel( "ILD_o1_v05")
  marlin.setOutputFile( recoFile )

  myJob.append( marlin )
  myJob.submit( dIlc )
