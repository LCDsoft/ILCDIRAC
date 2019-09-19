
.. _calibration:

Running Pandora Calorimeter Calibration
=======================================

This page provides a brief introduction of the iterative calorimeter calibration procedure with PandoraPFA reconstruction and its implementation as the Calibration service. The goal of the calibration procedure is to calculate:

- ECal, HCal and Muon system digitization constants
- Pandora calibration constants
- Photon likelihood

Calibration procedure consists of 3 different stages which run successively:

- Stage 1. Calculation of digitization and Pandora calibration constants with simplified photon reconstruction procedure. These constants are used during photon likelihood training. 
- Stage 2. Photon likelihood training.
- Stage 3. Calculation of final digitization and Pandora calibration constants with full photon reconstruction (using photon likelihood trained in the previous stage).

Each stage consist of set of phases. Each phase is designed to calculate some concrete constants by making an iterative reconstruction of the same input data, updating constants at each iteration, until reaching the requested precision:

- Phase 0. Calculate digitization constant for ECAL (CalibrationPhase.ECalDigi).
- Phase 1. Calculate digitization constant for HCAL (CalibrationPhase.HCalDigi).
- Phase 2. Calculate digitization constant for Muon system and HCALOther (CalibrationPhase.MuonAndHCalOtherDigi).
- Phase 3. Calculate Pandora electro-magnetic energy calibration constants (CalibrationPhase.ElectroMagEnergy).
- Phase 4. Calculate Pandora hadronic energy calibration constants (CalibrationPhase.HadronicEnergy).
- Phase 5. Perform photon likelihood training (CalibrationPhase.PhotonTraining).

During stages 1 and 3 Phases 0-4 are run while at the stage 2 only Phase 5 is run.
User can configure which stages and phases has to be run during the calibration by setting up **startStage**, **stopStage**, **startPhase** and **stopPhase** parameters. By default the complete calibration chain is run.

More information about the calibration procedure can be found in this `talk at the CLIC collaboration meeting. <https://indico.cern.ch/event/792656/contributions/3536472/attachments/1898302/3132559/clic_collabMeeting_PandoraCaloCalibration.pdf>`_

User input required for the calibration procedure
-------------------------------------------------
To start calibration user have to call :func:`createCalibration() <ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.CalibrationHandler.export_createCalibration>` function of the Calibration client :class:`ILCDIRAC.CalibrationSystem.Client.CalibrationClient` with three input arguments:

- **inputFiles** - dictionary which contains list of input file LFNs. Should contains keys ``zuds``, ``kaon``, ``muon``, ``gamma``, which correspond to the list of files of according type.
- **numberOfEventsPerFile** - number of events per file of each type. Should contains the same keys as inputFiles dictionary.
- **calibSettingsDict** - dictionary with calibration settings. Default settings can be initialized with createCalibrationSettings function of module ILCDIRAC.CalibrationSystem.Service.DetectorSettings.

The calibration uses simulated single particle data at various energies to set the ECal, HCal and Muon system digitization constants as well as the Pandora calibration constants. The procedure, as performed currently for CLICdet and CLD detector models, needs the following simulated single particle data (typically 1000-10000 events per point, distributed uniformly in the detector) in lcio format: 

- 10 GeV Photons (``gamma`` sample)
- 10 GeV muons (``muon`` sample) 
- 50 GeV Neutral Kaons (``kaon`` sample)

In addition to the single particle samples, user have to provide light flavour :math:`Z \to q \bar{q}` sample for photon likelihood training (``zuds`` sample).

Calibration settings
--------------------
A short description of calibration settings needed to be provided by user:

- **DDCaloDigiName** - the name of the DDCaloDigi processor which is used during reconstruction (some steering files may contain a few processors with different configurations).
- **DDPandoraPFANewProcessorName** - name of the DDPandoraPFANewProcessor processor which is used during reconstruction.
- **steeringFile** - path to the steering file. TODO: local path or LFN?
- **detectorModel** - detector model to use.
- **digitisationAccuracy** - required accuracy on the digitization constants (the total energy deposited in the calorimeter wrt. truth energy of particle). Default value is 0.05.
- **pandoraPFAAccuracy** - required accuracy on the Pandora calibration constants (the reconstructed energy of the Pandora cluster wrt. truth energy of the particle). Default value is 0.025.
- **disableSoftwareCompensation** - default value is True.
- **numberOfJobs** - number of worker nodes to be used for callibration. Default value is 100.
- **outputSE** - storage element where calibration service will copy all output files to. Default value is None.
- **outputPath** - LFN where copy all output files to. Default value is None.
- **marlinVersion** - version of the iLCSoft release to use. Default value is ILCSoft-2019-04-01_gcc62.
- **ecalBarrelCosThetaRange** - cos(theta) range within which all energy contributions of a single photon will end up in the barrel part of ECAL. Expected value is a list consisting two floats. Default value is None.
- **ecalEndcapCosThetaRange** - cos(theta) range within which all energy contributions of a single photon will end up in the endcap part of ECAL. Expected value is a list consisting two floats. Default value is None.
- **hcalBarrelCosThetaRange** - cos(theta) range within which all energy contributions of a single K0L will end up in the barrel part of HCAL. Expected value is a list consisting two floats. Default value is None.
- **hcalEndcapCosThetaRange** - cos(theta) range within which all energy contributions of a single K0L will end up in the endcap part of HCAL. Expected value is a list consisting two floats. Default value is None.
- **fractionOfFinishedJobsNeededToStartNextStep** - fraction of worker jobs are required to finish current step in order for service to execute the next step. Default value is 0.9.
- **nHcalLayers** - number of HCAL layers in the provided detector model. This parameter will be resolved automatically in the future releases. Default value is None.
- **nEcalThinLayers** - number of thin ECAL layers. Default value is 40. ???
- **nEcalThickLayers** - number of thick ECAL layers. Default value is 0. ???
- **ecalResponseCorrectionForThickLayers** - ??? Default value is 1.0.
- **startStage** - stage to start calibration from. Default value is 1.
- **startPhase** - phase to start calibration from. Default value is 0.
- **stopStage** - stop stage. Default value is 3.
- **stopPhase** - stop phase. Default value is 4.
- **startCalibrationFinished** - set calibrationFinished to True for current instance of CalibrationRun. This is a debug parameter. It will be removed in future releases.


An example of running calibration
---------------------------------
An example of the running calibration of the CLD detector model::

   from DIRAC.Core.Base import Script  # dirac enviroment
   Script.parseCommandLine(ignoreErrors=False)  # dirac enviroment
   
   from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import CalibrationClient
   client = CalibrationClient()

   from ILCDIRAC.CalibrationSystem.Service.DetectorSettings import createCalibrationSettings
   calibSettings = createCalibrationSettings('CLD')  # get default settings for CLD detector model
   calibSettingsDict = calibSettings.settingsDict

   calibSettingsDict['steeringFile'] = 'LFN:/ilc/user/o/oviazlo/fccee_caloCalib/fcceeReconstruction.xml'
   calibSettingsDict['outputPath'] = '/ilc/user/o/oviazlo/fccee_caloCalib/output/'
   calibSettingsDict['outputSE'] = 'CERN-DST-EOS' 

   inputFiles = {'zuds': ["LFN:/ilc/user/o/oviazlo/zudsFile1.slcio", "LFN:/ilc/user/o/oviazlo/zudsFile2.slcio"],
                 'gamma': ["LFN:/ilc/user/o/oviazlo/gammaFile1.slcio", "LFN:/ilc/user/o/oviazlo/gammaFile2.slcio"],
                 'kaon': ["LFN:/ilc/user/o/oviazlo/kaonFile1.slcio", "LFN:/ilc/user/o/oviazlo/kaonFile2.slcio"],
                 'muon': ["LFN:/ilc/user/o/oviazlo/muonFile1.slcio", "LFN:/ilc/user/o/oviazlo/muonFile2.slcio"]}

   numberOfEventsPerFile = {'zuds': 100, 'gamma': 20, 'kaon': 20, 'muon': 20}

   res = client.createCalibration(inputFiles, numberOfEventsPerFile, calibSettingsDict)


.. Input arguments:
..
.. **inputFiles**::
..
..    {'zuds': ["zudsFile1.slcio", "zudsFile2.slcio"], 'gamma': ["gammaFile1.slcio", "gammaFile2.slcio"], 'kaon': ["kaonFile1.slcio", "kaonFile2.slcio"], 'muon': ["muonFile1.slcio", "muonFile2.slcio"]}
..
.. **numberOfEventsPerFile**::
..
..    {'zuds': 100, 'gamma': 20, 'kaon': 20, 'muon': 20}

An example of settings for CLD detector calibration **calibSettingsDict**:
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   Parameter name                                        |      Parameter value                                                           |
   +=========================================================+================================================================================+
   |   DDCaloDigiName                                        |      MyDDCaloDigi_10ns                                                         |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   DDPandoraPFANewProcessorName                          |      MyDDMarlinPandora_10ns                                                    |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   detectorModel                                         |      LFN:/ilc/user/o/oviazlo/fccee_caloCalib/FCCee_o1_v04_ecal20_10.tgz        |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   digitisationAccuracy                                  |      0.02                                                                      |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   disableSoftwareCompensation                           |      True                                                                      |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   ecalBarrelCosThetaRange                               |      [0.0, 0.643]                                                              |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   ecalEndcapCosThetaRange                               |      [0.766, 0.94]                                                             |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   ecalResponseCorrectionForThickLayers                  |      1.9                                                                       |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   fractionOfFinishedJobsNeededToStartNextStep           |      0.9                                                                       |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   hcalBarrelCosThetaRange                               |      [0.15, 0.485]                                                             |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   hcalEndcapCosThetaRange                               |      [0.72, 0.94]                                                              |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   marlinVersion                                         |      ILCSoft-2019-07-09_gcc62                                                  |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   nEcalThickLayers                                      |      10                                                                        |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   nEcalThinLayers                                       |      20                                                                        |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   nHcalLayers                                           |      44                                                                        |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   numberOfJobs                                          |      200                                                                       |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   outputPath                                            |      /ilc/user/o/oviazlo/fccee_caloCalib/output/                               |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   outputSE                                              |      CERN-DST-EOS                                                              |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   pandoraPFAAccuracy                                    |      0.005                                                                     |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   startCalibrationFinished                              |      False                                                                     |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   steeringFile                                          |      LFN:/ilc/user/o/oviazlo/fccee_caloCalib/fcceeReconstruction.xml           |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   startStage                                            |      1                                                                         |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   startPhase                                            |      0                                                                         |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   stopStage                                             |      3                                                                         |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+
   |   stopPhase                                             |      4                                                                         |
   +---------------------------------------------------------+--------------------------------------------------------------------------------+


Calibration results
-------------------
Results from the calibration are copied to the directory provided by the user as "outputPath" settings. List of copied files:

- Marlin steering file with new set of the calibration constants. It has the same name as input steering file provided by user.
- Original copy of the steering file which ends with "_INPUT" postfix.
- **newPandoraLikelihoodData.xml** - file with new photon likelihood. This file has to be used together with the new calibration constants.
- **Calibration.txt** - files which contains intermediate results and fit values from the calibration. Can be used for debugging.
- set of .png and .C pictures which shows fits done during the calibration procedure. These plots can be used to verify correctness of the procedure.


Monitoring and controlling tools
--------------------------------
Calibration service have a list of functions which allows to monitor and control ongoing calibrations:

- :func:`getUserCalibrationStatuses() <ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.CalibrationHandler.export_getUserCalibrationStatuses>` - get statuses of all active calibrations.
- :func:`killCalibrations(calibIdsToKill) <ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.CalibrationHandler.export_killCalibrations>` - kill calibrations.
- :func:`cleanCalibrations(calibIdsToClean) <ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.CalibrationHandler.export_cleanCalibrations>` - remove calibration results from the server (calibration results are stored on the server by some time after calibration has been finished or killed).
- :func:`changeDirectoryToCopyTo(calibId, newPath, newSE) <ILCDIRAC.CalibrationSystem.Service.CalibrationHandler.CalibrationHandler.export_changeDirectoryToCopyTo>` - set new output path and storage element for the calibration 

Also, status of the calibration jobs can be monitored with Job Monitor with iLCDirac web-interface. Status of the running jobs which belongs to the calibration contain information about the current stage, phase and step of the calibration.


References
----------
- `Presentation at the CLIC collaboration meeting <https://indico.cern.ch/event/792656/contributions/3536472/attachments/1898302/3132559/clic_collabMeeting_PandoraCaloCalibration.pdf>`_
