.. _examplejobs:


Complete Example Submission Scripts
===================================

For description of the functions please see the
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.UserJob` class and the
:mod:`~ILCDIRAC.Interfaces.API.NewInterface.Applications` modules and finally at
the :mod:`~ILCDIRAC.Interfaces.API.DiracILC` class. There are also some more
instructions about :ref:`submittingjobs`.

.. Note ::

  Please use the
  :func:`~ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData` function
  to store any output data except for log files.

.. contents:: Table of Contents


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
  job.setOutputSandbox(['*.log', '*.sh', '*.py', '*.xml'])
  job.setOutputData( ['myOutputFile.slcio', 'myRootFile.root'] )
  job.setJobGroup( "myMarlinRun1" )
  job.setName( "MyMarlinJob1" )
  job.setInputData( '/ilc/user/u/username/slcio/input.slcio' )
  marl = Marlin ()
  marl.setVersion("ILCSoft-01-17-09")

  marl.setInputFile( ['input.slcio'] )
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
  job.setOutputSandbox(['*.log', '*.sh', '*.py'])
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


Running DDSim and then Marlin
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
  myJob.submit(dIlc)


Running Overlay and Marlin
--------------------------

.. code:: python

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin, OverlayInput

  dIlc = DiracILC()

  job = UserJob()
  job.setInputData( "/ilc/prod/clic/350gev/h_nunu/ILD/SIM/00006524/000/h_nunu_sim_6524_1.slcio" )
  job.setOutputSandbox( "*.log" )
  job.setOutputData( "myReco_1.slcio" )

  over = OverlayInput()
  over.setBXOverlay( 300 )
  over.setGGToHadInt( 0.0464 )
  over.setNumberOfSignalEventsPerJob( 100 )
  over.setBackgroundType( "gghad" )
  over.setDetectorModel( "CLIC_ILD_CDR500" )
  over.setEnergy( "350" )
  over.setMachine( "clic_cdr" )

  marlin = Marlin()
  marlin.setVersion( "v0111Prod" )
  marlin.setInputFile( "h_nunu_sim_6524_1.slcio" )
  marlin.setOutputFile( "myReco_1.slcio" )
  marlin.setSteeringFileVersion( "V22" )
  marlin.setSteeringFile( "clic_ild_cdr500_steering_overlay_350.0.xml" )
  marlin.setGearFile( "clic_ild_cdr500.gear" )
  marlin.setNumberOfEvents( 10 )

  res = job.append( over )
  if not res['OK']:
    print res['Message']
    exit( 1 )
  job.append( marlin )


  job.submit(dIlc)


Running Overlay and Marlin with CLIC_o3_v12
-------------------------------------------

.. code:: python

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin, OverlayInput

  dIlc = DiracILC()

  job = UserJob()
  job.setInputData( "/ilc/prod/clic/3tev/qqqq/CLIC_o3_v12/SIM/00008298/000/qqqq_sim_8298_1.slcio" )
  job.setOutputSandbox( "*.log" )
  job.setOutputData( "myReco_1.slcio" )
  job.setCLICConfig( "ILCSoft-2017-07-27" )

  over = OverlayInput()
  over.setBXOverlay( 30 )
  over.setGGToHadInt( 3.2 )
  over.setNumberOfSignalEventsPerJob( 100 )
  over.setBackgroundType( "gghad" )
  over.setDetectorModel( "CLIC_o3_v12" )
  over.setEnergy( "3000" )
  over.setMachine( "clic_opt" )
  over.setProcessorName( "Overlay3TeV" )

  marlin = Marlin()
  marlin.setVersion( "ILCSoft-2017-07-27_gcc62" )
  marlin.setInputFile( "qqqq_sim_8298_1.slcio" )
  marlin.setOutputFile( "myReco_1.slcio" )
  marlin.setSteeringFile( "clicReconstruction.xml" )
  marlin.setExtraCLIOptions( " --Config.Overlay=3TeV " )
  marlin.setNumberOfEvents( 100 )

  res = job.append( over )
  if not res['OK']:
    print res['Message']
    exit( 1 )
  job.append( marlin )


  job.submit(dIlc)


Automatic Job Splitting
-----------------------

This example shows how the automatic job splitting can be used to quickly create
a larger number of jobs that each simulate a given number of events. The random
seed for each job is based on the iLCDIRAC jobID, the output filenames are
injected with the job index, 0 to 9 in this example.

There is also the option to automatically split jobs over inputfiles, see
:func:`~ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setSplitInputData`.

.. code:: python

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  from ILCDIRAC.Interfaces.API.DiracILC import DiracILC
  from ILCDIRAC.Interfaces.API.NewInterface.UserJob import UserJob
  from ILCDIRAC.Interfaces.API.NewInterface.Applications import Marlin, OverlayInput

  dIlc = DiracILC()

  job = UserJob()
  job.setOutputSandbox( "*.log" )
  ## output data name is automatically changed to, e.g., ddsimout_5.slcio
  job.setOutputData( "ddsimout.slcio", outputPath="sim1" )
  job.setCLICConfig( "ILCSoft-2017-07-27" )
  ## creates 10 jobs with 100 events each
  job.setSplitEvents( eventsPerJob=100, numberOfJobs=10 )

  ddsim = DDSim()
  ddsim.setVersion("ILCSoft-2017-07-27_gcc62")
  ddsim.setDetectorModel("CLIC_o3_v13")
  ddsim.setExtraCLIArguments( " --enableGun --gun.particle=mu- " )
  ddsim.setNumberOfEvents( 100 )
  ddsim.setSteeringFile( "clic_steer.py" )
  ddsim.setOutputFile( "ddsimout.slcio" )
  myJob.append(ddsim)
  myJob.submit(dIlc)
