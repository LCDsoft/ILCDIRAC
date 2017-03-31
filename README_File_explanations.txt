Files:
======
* *processlist.whiz, some_job_repository.rep*: DIRAC/... files
* *Testfiles/testfile.txt*: File used in the SE tests
* *Tests/Utilities/update_code.sh*: To be used on voilcdiractest07, switches to the /opt/dirac/... installation and pulls the new code after fetching
* *dev_create_events.py*: Creates 1000 photons, muons and kaons for the Calibration. (particles and energy are easily changed)
* *.gitlab-ci.yml3*: gitlab-ci yml used when Marko and I tried to move everything to CVMFS (I diffed it against the currently used one, actually almost no differences. can probably just be deleted)
* *CalibrationSystem/Client/CalibrationDistributedScript.py*: Executes the calibration on the worker nodes.
* *CalibrationSystem/Client/run_calibration.sh*: Prepares the environment and calls the CalibrationDistributedScript. Thus the executable passed to the job
* *CalibrationSystem/user_guide.rst*: The documentation file
* *ILCTransformationSystem/Tests/Test_TarTheLogsAgent.py*, Interfaces/API/NewInterface/Tests/Test_DDsimInterface.py*: 2 unfinished tests


Directories:
============
* *Tests/playground_globalmocking/*: Small example/test how to use the globalMocker


Tips for developing CalibrationSystem:
======================================
* If Marlin complains about loading duplicate shared libraries, init_ilcsoft.sh was sourced twice. (in my opinion this is a big in init_ilcsoft.sh and should be fixed)
* The file Xml_Generation/CLIC_PfoAnalysis_AAAA_SN_BBBB.xml as it is in the SVN is corrupted -- there are 'nested comments' which are forbidden according to XML specification (and most parsers will refuse them). This will mean the input files created from this one will be corrupt to, crashing Marlin! (something about pdf histograms)
* The original calibrate script from the SVN contains several errors - the PandoraSettingsDefault.xml and PandoraLikelihoodData9EBin.xml must be copied to the working directory before executing the script.
