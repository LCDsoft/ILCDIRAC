----------------
Package ILCDIRAC
----------------

Version v1r7p0
--------------
:CHANGE:
 Core
  - Reshuffle CombinedSoftwareInstallation so that we use the SharedArea
  - TARSoft: don't redownload the applications if they are already there. Had to do some tricks to manage slic folder name TODO: what about LCSIM
  - in TARSoft, use ReplicaManager if url does not start with http://
  - better check in SQLWrapper that TMP dir is properly created. Also do proper remove of TMP dir, whatever happened to the socket.
  - better handling of SQLWrapper errors
  - Add modules needed by UserJobFinalization
  - adapt ProdutionData to ILC needs, basically removing everything
  - To be able to use InputData, need to import InputDataResolution.
  - dirac-ilc-add-sofware.py: now add to TarBallURL location the tar ball
  - update detectOS after discussion with Hubert, comment out slc4 binary support
 Interfaces
  - In presubmissionchecks, check that outputpath, if used, does not contain /../, /./, or //, and does not end with /.
  - All applications now call the UserJobFinalization module, and setOutputData is ILC specific.
  - Check that outputdata and outputsandbox do not contain the same things and output data does not allow wildcard FIXME: checks where not done properly, all things were not checked FIXME: add TotalSteps in setROOT
  - allow to use LFNs for steering and xml files for Mokka and Marlin
 Workflow
  - handle return value of SQLWrapper in MokkaWrapper
  - check if input slcio is present for Marlin before running
  - add UserJobFinalization module, taken from LHCb
  - prepare for using InputData: find out where the files are on the fly and pass the full path to PrepareOptionsfiles

