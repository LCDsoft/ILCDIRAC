----------------
Package ILCDIRAC
----------------

Version v5r0p5
--------------

CHANGE
::::::

 Workflow
  - slic Remove checks for XERCES

Version v5r0p4
--------------

CHANGE
::::::

 Core
  - slic Make sure the xerces directory is there before dealing with it
 Workflow
  - slic Make sure the xerces directory is there before dealing with it

Version v5r0p3
--------------

BUGFIX
::::::

 OverlaySystem
  - wrong patch number

Version v5r0p2
--------------

CHANGE
::::::

 Workflow
  - SLICPandora will also look for the settings file under ./Settings. Add lib to LD_LIBRARY_PATH

Version v5r0p1
--------------

CHANGE
::::::

 Workflow
  - Added MALLOC_CHECK_=0 env variable to prevent SLIC to fail.

Version v4r2p7
--------------

NEW
:::

 Core
  - Don't change the InoutFiel if specified by the user

Version v4r2p6
--------------

NEW
:::

 Core
  - WhizardOptions and GeneratorModels now linked to each other for parameter resolution
 Interfaces
  - Support for model parameters in Whizard
 Workflow
  - Support for model parameters in WhizardAnalysis

Version v4r2p5
--------------

BUGFIX
::::::

 Core
  - DownloadInputData from DIRAC being buggy, need to import it here

Version v4r2p4
--------------

CHANGE
::::::

 Core
  - Allow for setting mcRunNumber
 Workflow
  - mcRunNumber added

Version v4r2p3
--------------

CHANGE
::::::

 Core
  - Added missing file in resolveIFpath S_ERROR message

BUGFIX
::::::

 Workflow
  - Make sure to treat only non-zero length inputfiles, and remove trailing ;

Version v4r2p2
--------------

Version v4r2p1
--------------

NEW
:::

 Workflow
  - Support for parametric parameters in ApplicationScript

Version v4r2p0
--------------

NEW
:::

 ProcessProductionSystem
  - Reimport the ProcessProduction in the release mechanism

Version v4r1p9
--------------

BUGFIX
::::::

 Workflow
  - Mokka reaches en-of-file error has status code 9, not 10

Version v4r1p8
--------------

BUGFIX
::::::

 Workflow
  - RandomSeed was not valid

Version v4r1p7
--------------

CHANGE
::::::

 Core
  - Add type checking in whizardOptions

Version v4r1p6
--------------

BUGFIX
::::::

 Workflow
  - Key for seed was not right.

Version v4r1p5
--------------

CHANGE
::::::

 Workflow
  - Check return value when updating the WhizardOptions

Version v4r1p4
--------------

CHANGE
::::::

 Workflow
  - added messages

Version v4r1p3
--------------

CHANGE
::::::

 Core
  - Added message in resolveIFpath, needed to debug

Version v4r1p2
--------------

NEW
:::

 Interfaces
  - Better support for WHIZARD new option style: handling of multiple process in particular

CHANGE
::::::

 Core
  - Increase default number of calls in whizard options

BUGFIX
::::::

 Workflow
  - Whizard's seed was not set to desired value

Version v4r1p1
--------------

NEW
:::

 Interfaces
  - Better support for WHIZARD new option style

BUGFIX
::::::

 Workflow
  - Whizard should not fail anymore

Version v4r1p0
--------------

NEW
:::

 Core
  - Utility that holds the available steering files
 Interfaces
  - Support for full options from WHIZARD

CHANGE
::::::

 Workflow
  - Add support for Whizard options and fix small InputData glitch

Version v4r0p0
--------------

NEW
:::

 OverlaySystem
  - No Change, needed version to move towards v6

Version v3r4p1
--------------

Version v3r4p0
--------------

NEW
:::

 Interfaces
  - Set detectormodel zip file in LCSIM to avoid downloading it from the web every job
 Workflow
  - Allow for handling of detector model in LCSIM passed in ISB

Version v3r3p4
--------------

BUGFIX
::::::

 Workflow
  - One break and continue statement

Version v3r3p3
--------------

BUGFIX
::::::

 Workflow
  - Detector.zip was not unzipped if passed

Version v3r3p2
--------------

CHANGE
::::::

 Workflow
  - Handling of NbOfEvts in file registration fixed

Version v3r3p1
--------------

CHANGE
::::::

 Core
  - GeneratorModels: getFile returns S_ERROR when no file is attached to a given model (e.g. sm)
 Workflow
  - Adapt to new GeneratorModels way of doing things.

Version v3r3p0
--------------

NEW
:::

 Core
  - Handling of available models is done through the CS
 Interfaces
  - Adapt to new handling of models

BUGFIX
::::::

 Workflow
  - Proper handling of Model

Version v3r2p1
--------------

BUGFIX
::::::

 Workflow
  - Proper handling of LesHouches file

Version v3r2p0
--------------

NEW
:::

 Core
  - Not needed to specify input if one passes it as dcap directly in xml

BUGFIX
::::::

 Interfaces
  - couple of things, better handling of nb evts.
 Workflow
  - Account for new model definition

Version v3r1p1
--------------

NEW
:::

 Core
  - Install software also in OSG_APP if defined

CHANGE
::::::

 Interfaces
  - Improved interfaces, several bug fixes
 Workflow
  - Adapt for new interface of slicPandora, several fixes, in particular for LCIOConcatenate (works in prod context)

Version v3r1p0
--------------

NEW
:::

 Core
  - Install software also in OSG_APP if defined

CHANGE
::::::

 Interface
  - Improved interfaces, several bug fixes
 Workflow
  - Adapt for new interface of slicPandora, several fixes, in particular for LCIOConcatenate (works in prod context)

Version v3r0p0
--------------

CHANGE
::::::

 Core
  - Many small things
 Interfaces
  - Added new Interface, for testing purposes
 Workflow
  - Many updates on all modules to fit the new Interface

Version v2r5p5
--------------

BUGFIX
::::::

 Workflow
  - soft links do not work, one needs to copy the things for whizard.

Version v2r5p4
--------------

BUGFIX
::::::

 Core
  - remove the incompatible libs during install, but also in lib folder

Version v2r5p3
--------------

BUGFIX
::::::

 Core
  - remove the incompatible libs during install

Version v2r5p2
--------------

BUGFIX
::::::

 Core
  - removing lib when no rights fails.

Version v2r5p1
--------------

BUGFIX
::::::

 Workflow
  - Path to steering files was not properly set

Version v2r5p0
--------------

CHANGE
::::::

 Core
  - Remove software module also removes the DB slice if found, fixed glitch in Mokka steering file
 Workflow
  - MokkaAnalysis: now the DB slice is checked before usage, as it's now  supposed to be in the Mokka directory

Version v2r4p0
--------------

NEW
:::

 Core
  - WasteCPU utility, that does what its name suggests. Added the dragon in SQLWrapper

CHANGE
::::::

 OverlaySystem
  - Using Client instead of RPCClient call
 Workflow
  - OverlayInput now uses WasteCPU utility

Version v2r3p0
--------------

NEW
:::

 OverlaySystem
  - OverlaySystem now comes with Agent to reset the job counters once per hours

Version v2r2p0
--------------

CHANGE
::::::

 Core
  - Added run number to Mokka and SLIC files, same as randomseed for the moment

Version v2r1p10
---------------

CHANGE
::::::

 Workflow
  - OverlayInput reports standby number once every 10 miutes

Version v2r1p9
--------------

CHANGE
::::::

 Workflow
  - add a count to 50000000 between each file in Overlay to prevent sites from thinking the job is stalled

Version v2r1p8
--------------

CHANGE
::::::

 Workflow
  - add a count to 1000000 between each file in Overlay to prevent sites from thinking the job is stalled

Version v2r1p7
--------------

BUGFIX
::::::

 Core
  - chdir was missing

Version v2r1p6
--------------

BUGFIX
::::::

 Workflow
  - Do not check for stalled when getting the fiels with rm.getFile()

Version v2r1p5
--------------

BUGFIX
::::::

 Workflow
  - In Marlin, location of gear file was not correctly specified

Version v2r1p4
--------------

BUGFIX
::::::

 Core
  - mysql4grid directory sent back to LocalArea, only if it's not there

Version v2r1p3
--------------

BUGFIX
::::::

 Core
  - mysql4grid directory sent back to LocalArea

Version v2r1p2
--------------

BUGFIX
::::::

 Core
  - Changing directory was done too late, the app was never found

Version v2r1p1
--------------

BUGFIX
::::::

 Core
  - checking that one is allowed to write in the area was done before checking that the application was there.

Version v2r1p0
--------------

CHANGE
::::::

 Core
  - added message in case of success of soft removal
 Workflow
  - Workflow modeules now look into the software dir for default location of steering files if they are not in the cur dir

Version v2r0p0
--------------

NEW
:::

 Core
  - Allow installation in SharedArea by default. Fall back to LocalArea when not possible to use.
  - RemoveApp module to remove applications
 Interfaces
  - Added MCReconstruction_Overlay as valid production type, Added interface to remove applications: experts ONLY!

Version v1r19p0
---------------

NEW
:::

 OverlaySystem
  - Service to handle properly the overlay
 Workflow
  - OverlayInput uses OverlaySystem

Version v1r18p16
----------------

BUGFIX
::::::

 Workflow
  - OverlayInput at RAL is failing (again)

Version v1r18p15
----------------

BUGFIX
::::::

 Core
  - Number of events per job was not properly treated.
 Workflow
  - OverlayInput at RAL is failing

Version v1r18p14
----------------

BUGFIX
::::::

 Workflow
  - In OverlayInput, dccp command for Imperial site had wrong argument

Version v1r18p13
----------------

CHANGE
::::::

 Workflow
  - In OverlayInput, Control the number of concurrent download per site, even for CERN, CC and Imperial

Version v1r18p12
----------------

CHANGE
::::::

 Workflow
  - In OverlayInput, use dcap protocol in imperial

Version v1r18p11
----------------

CHANGE
::::::

 Workflow
  - In OverlayInput, allow direct access in IMPERIAL

Version v1r18p10
----------------

CHANGE
::::::

 Workflow
  - In OverlayInput, don't use FC to get number of events per file, use CS parameter instead. Also count failures, and if too many (CS parameter =20), return error

Version v1r18p9
---------------

CHANGE
::::::

 Workflow
  - In OverlayInput, if running at CERN, use also rfcp if xrdcp fails. Add IN2P3-CC as a site that can use xrdcp

Version v1r18p8
---------------

BUGFIX
::::::

 Workflow
  - Default number of events to process in SLICPandora must be -1.

Version v1r18p7
---------------

BUGFIX
::::::

 Workflow
  - use of lower in name matching killed matching (Again).

Version v1r18p6
---------------

BUGFIX
::::::

 Workflow
  - use of lower in name matching killed matching.

Version v1r18p5
---------------

CHANGE
::::::

 Workflow
  - Naming convention in UploadOutputData, for easier maintenance

Version v1r18p4
---------------

BUGFIX
::::::

 Workflow
  - Fixed Pythia Module outputFile name in Prod context

Version v1r18p3
---------------

BUGFIX
::::::

 Workflow
  - Fixed Pythia Module outputFile name in Prod context

Version v1r18p2
---------------

BUGFIX
::::::

 Interfaces
  - Fixed Production.py
 Workflow
  - Fixed Pythia Module outputFile name

Version v1r18p1
---------------

BUGFIX
::::::

 Interfaces
  - Fixed Production.py
 Workflow
  - Fixed Pythia Module outputFile name

Version v1r18p0
---------------

NEW
:::

 Interfaces
  - Added Pythia Step

Version v1r17p10
----------------

CHANGE
::::::

 Workflow
  - Added printout of files obtained in overlay

Version v1r17p9
---------------

BUGFIX
::::::

 Workflow
  - don't account for the dirac_directory things when nsls

Version v1r17p8
---------------

BUGFIX
::::::

 Workflow
  - don't account for the dirac_directory things when nsls

Version v1r17p7
---------------

NEW
:::

 Workflow
  - OverlayInput: when running at CERN, get the file list from CASTOR

Version v1r17p6
---------------

BUGFIX
::::::

 Workflow
  - OverlayInput failed to find metadata because specified prodID was not correct

Version v1r17p5
---------------

CHANGE
::::::

 Workflow
  - if overlayInput runs at CERN, it will get the files with xrdcp

Version v1r17p4
---------------

CHANGE
::::::

 Workflow
  - OverlayInput will wait no longer than 300 minutes, else declare as failed.

BUGFIX
::::::

 Workflow
  - whizard was throwing an uncaught exception when the lumi was not found

Version v1r17p3
---------------

BUGFIX
::::::

 Workflow
  - Overlayinput was downloading all files twice!

Version v1r17p2
---------------

NEW
:::

 Interfaces
  - LCSIM now has a new parameter, extraparams, that can be used to pass command line parameters
  - GetSRMFile now limits the number of parallel downloads to 100 by default (CS parameter) to avoid time outs from disk server
  - More messages during overlay input module

Version v1r17p1
---------------

CHANGE
::::::

 Interfaces
  - Default Log file name now includes step number, so one can run 2 times or more the same application, and the log file does not get erased
  - Missing process list message is now a warning.

BUGFIX
::::::

 Core
  - OutputREC files and OutputDST were not set properly in LCSIM

Version v1r17p0
---------------

NEW
:::

 Workflow
  - Overlay now allows only 200 parallel file downloads, CS parameter

CHANGE
::::::

 Core
  - add-software script puts the file at IN2P3 and the replication request is to CERN
 Interfaces
  - Parameters are now properly placed in the CS
 Workflow
  - Added proper SVN keywords

Version v1r16p17
----------------

BUGFIX
::::::

 Workflow
  - Again the tag name is wrong...

Version v1r16p16
----------------

BUGFIX
::::::

 Workflow
  - Fix logic bug in OverlayInput as it used to download as many files as there are signal events.

Version v1r16p15
----------------

BUGFIX
::::::

 Workflow
  - level of message warning does not exists, but warn does

Version v1r16p14
----------------

BUGFIX
::::::

 Core
  - USER_spectrum_mode was not set properly in whizard

Version v1r16p13
----------------

NEW
:::

 Interfaces
  - : Support for user spectrum in whizard.

CHANGE
::::::

 Core
  - Also look at the Number of bunch train to overlay before looking at the files.

Version v1r16p12
----------------

CHANGE
::::::

 Workflow
  - Disable CPU check while getting the overlay files as there is a risk it takes too much time

Version v1r16p11
----------------

CHANGE
::::::

 Workflo
  - Disable CPU check while getting the overlay files as there is a risk it takes too much time

Version v1r16p10
----------------

BUGFIX
::::::

 Workflow
  - tag number was wrong

Version v1r16p9
---------------

NEW
:::

 Core
  - dirac-ilc-add-software and add-whizard now create a replication request for new tar balls.
 Interfaces
  - Module to print out the Workflow parameters only
 Workflow
  - For next major dirac release, ParametricInputSandbox will be possible with Marlin

CHANGE
::::::

 Workflow
  - Now when getting the overlay fioles, wait for 3 minutes on average (gauss distributed, sigma=0.1)
  - Use common method between application modules (not for Mokka though) to report the final status

Version v1r16p8
---------------

NEW
:::

 Interfaces
  - Script to obtain the productions summaries

CHANGE
::::::

 Interfaces
  - Production API now get the directory metadata to pass to daughters
 Workflow
  - Catch message in whizard log to declare the job as successful

Version v1r16p7
---------------

CHANGE
::::::

 Core
  - Get the directorymetadata of the InputData files to get the number of events.

Version v1r16p6
---------------

CHANGE
::::::

 Core
  - Look for overlay files only if needed

Version v1r16p5
---------------

NEW
:::

 Core
  - Allow setting of event by event parameter ProcessID. Can be set by users' jobs and automatically resolved for production jobs

Version v1r16p4
---------------

NEW
:::

 Core
  - Handle the particle.tbl file for Mokka

Version v1r16p3
---------------

NEW
:::

 Workflow
  - Catch the luminosity generated by whizard for a job, and pass it to the workflow_commons definition

Version v1r16p2
---------------

BUGFIX
::::::

 Core
  - dirac-ilc-add-software

Version v1r16p1
---------------

NEW
:::

 Core
  - PrepareTomatoSalad: prepare the xml file for running tomato

CHANGE
::::::

 Workflow
  - MarlinAnalysis can be subclassed easily: TomatoAnalysis is a subclass

Version v1r15p7
---------------

NEW
:::

 Core
  - CheckXMLValidity utility to check at submission time the validity of the xml steering files

CHANGE
::::::

 Interfaces
  - Use new CheckXMLValidity utility for Marlin and LCSIM

Version v1r15p6
---------------

NEW
:::

 Interfaces
  - Switch to ignore application errors, use setIgnoreApplicationErrors() method of ILCJob to enable
  - validate input xml files during submission, catches most typos.

CHANGE
::::::

 Workflow
  - allow for user defined LesHouches file if whizard.

Version v1r15p5
---------------

CHANGE
::::::

 Core
  - Processlist is now passed as inputsandbox, so if downloading fails the first time, the job gets rescheduled

BUGFIX
::::::

 Interfaces
  - Production API: do not look for detector model if the data type is gen
 Workflow
  - SLICAnalysis: outputslcio -> outputFile

Version v1r15p4
---------------

NEW
:::

 Workflow
  - Registration of production files ancestors

Version v1r15p3
---------------

NEW
:::

 Interfaces
  - Add MCGeneration as a possible Production type

CHANGE
::::::

 Workflow
  - Added memory requirement for java in LCSIM

BUGFIX
::::::

 Core
  - With new Script interface, our scripts would not work. Made ilc-proxy-init deprecated, use proxy-init instead
  - Overlay input for LCSIM did not work (created exception)

Version v1r15p2
---------------

BUGFIX
::::::

 Workflow
  - bad workflow tag

Version v1r15p1
---------------

BUGFIX
::::::

 Workflow
  - bad workflow tag

Version v1r15p0
---------------

CHANGE: move to DIRAC v5r12p7



NEW
:::

 Core
  - Utility to obtain a prod proxy if needed, useful in prod submission scripts
 Interfaces
  - support for Tomato, check collections, lcio concat: currently in test phase
 Workflow
  - Support for overlay in LCSIM

CHANGE
::::::

 Interfaces
  - Modified scripts for sid jobs
 Workflow
  - Moved many parameters from many sub classes to mother class (ModuleBase): easier maintenance

Version v1r14p0
---------------

NEW
:::

 Interfaces
  - SID production submission scripts
  - SID chain job submission scripts, and directory containing necessary files

CHANGE
::::::

 Core
  - software addition uses Request object for replication.

BUGFIX
::::::

 Core
  - now remove system libs from all application on site. In the future, should remove them at tar ball creation time
 Workflow
  - Pass basename of xml file in LCSIM instead of parameter value

Version v1r13p3
---------------

BUGFIX
::::::

 Core
  - Gear file can also be a text in the xml parameters, not only a value

Version v1r13p2
---------------

NEW
:::

 Core
  - Added utilities for overlay input
 Interfaces
  - interface for overlay
 Workflow
  - Module for Overlay Input

BUGFIX
::::::

 Workflow
  - fix import location in LCSIMAnalysis

Version v1r13p1
---------------

BUGFIX
::::::

 Workflow
  - fix LD_LIBRARY_PATH for whizard

Version v1r13p0
---------------

NEW
:::

 Core
  - Utility to remove the libc provided in the software packages
 Interfaces
  - Script to submit productions in slic context

CHANGE
::::::

 Workflow
  - All worflow modules check that log file is present

Version v1r12p1
---------------

BUGFIX
::::::

 Workflow
  - bug fix in MokkaAnalysis

Version v1r12p0
---------------

NEW
:::

 Core
  - Now Mokka uses random seed for every job. Users can set their own seed.

Version v1r11p2
---------------

BUGFIX
::::::

 Workflow
  - take new interface of writestdhep into account

Version v1r11p1
---------------

BUGFIX
::::::

 Core
  - Bug in CombimedSoftware installation
 Interfaces
  - Several errors remained in PostGenSel module

Version v1r11p0
---------------

NEW
:::

 Core
  - added script to obtain list of available software: no need to use web page
 Interfaces
  - added PostGenSel step to allow "generator level" cuts

Version v1r10p7
---------------

CHANGE
::::::

 Core
  - All applications are also replicated to IN2P3-SRM
 Interfaces
  - jobindex in whizard can be anything
 Workflow
  - in whizard, when PYSTOP was called, application was still OK, now not anymore

BUGFIX
::::::

 Interfaces
  - XML file for LCSIM is now a parameter in the Production API

Version v1r10p6
---------------

BUGFIX
::::::

 Core
  - TARSoft was failing installation of lcio

Version v1r10p5
---------------

NEW
:::

 Core
  - LCIO specific install: environment vars are set

CHANGE
::::::

 Interfaces
  - Allowed models in Whizard for susy are slsqhh and chne

Version v1r10p4
---------------

NEW
:::

 Interfaces
  - allow choice of SUSY model in whizard

Version v1r10p3
---------------

CHANGE
::::::

 Core
  - added beam_ercoil and keep_initials as parameters

Version v1r10p2
---------------

BUGFIX
::::::

 Workflow
  - Registration of file in FC failed because FC changed

Version v1r10p1
---------------

BUGFIX
::::::

 Core
  - PrepareOptionsFile had a bug in Preparation of whizard.in

Version v1r10p0
---------------

NEW
:::

 Interfaces
  - Whizard step in DIRAC
  - SLIC Pandora step is in ProductionAPI
 Workflow
  - WhizardAnalysis module
  - FailoverRequest module: publish requests and update file status in transformation system

CHANGE
::::::

 Core
  - Whizard default .in file is now whizard.template.in, and is templated
  - Propagate the number of events and luminosity through productions
 Interfaces
  - Production and user job API takes parameters for whizard, to fill in the template
  - complete LCSIM step in production API: input and output are treated properly
  - Production details are available from web interface
 Workflow
  - UserLFN now uses current credentials to guess the VO: suitable for ILC and CALICE run

Version v1r9p0
--------------

NEW
:::

 Core
  - add resolveOFnames to change output files in production context
  - script/dirac-ilc-add-whizard: define in DIRAC a new whizard version
 Interfaces
  - Add possibility to get a file using its SRM path FIXME: startFrom in mokka is 0 by default instead of 1.
  - SLICPandora step definition
 Workflow
  - GetSRMFile module: used to get a file given its SRM path. Useful to get a file that is not registered in the DIRAC FC.
  - RegisterOutputData: set the metadata flags for production data
  - SLICPandora Module

CHANGE
::::::

 Core
  - check that application software is not empty after untarring
 Interfaces
  - allow arguments in ApplicationScript. To be used for pyroot scripts
  - add IS_PROD to workflow parameters, for Production API only
 Workflow
  - handle production context properly: input and output file names depend on prod ID and job ID
  - check that applications are actually there before running, and if not return an error.

Version v1r7p1
--------------

CHANGE
::::::

 Core
  - add comments in created steering and xml TODO: idem for SLIC and LCIM FIXME: replace rstrip by replace in TARSoft.py
 Interfaces
  - Marlin does not need to be specified the inputslcio list, as it is taken from inputdata if mokka step is not run before
  - overload setBannedSites

Version v1r7p0
--------------

CHANGE
::::::

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

Version v1r6p2
--------------

Version v1r6p1
--------------

Version v1r6p0
--------------

NEW
:::

 Core
  - dirac-ilc-add-software, utility to add software in CS

CHANGE
::::::

 Interfaces
  - use elif statements
 Workflow
  - handle end of file reached in Mokka, avoid job declared as failed.
  - in Marlin if nb of events to process is not specified, use -1 i.e. all events.

Version v1r5p0
--------------

CHANGE
::::::

 Core
  - Take into account dependencies in installation phase.
  - Set convention that folder containing application is same as tar ball name minus .tar.gz and .tgz
 Workflow
  - Get base folder  name based on CS content, allows for multiple version of the same software to run FIXME: Running marlin: duplicated processors were not properly removed from MARLIN_DLL.

Version v1r4p0
--------------

NEW
:::

 Interfaces
  - add DiracILC with specification of preSubmissionChecks
 DataManagementSystem
  - add DataManagementSystem, for dirac-dms-gridify-castor-file script

CHANGE
::::::

 Core
  - add in PrepareOptionsFiles the relevant methods for SLIC and LCSIM FIXME: fixes to the methods for Mokka and Marlin.
 Interfaces
  - add the relevant bits of code for the definition of SLIC and LCSIM jobs
  - add the possibility to run on mac files in mokka
 Workflow
  - add relevant workflow for SLIC and LCSIM

Version v1r3p0
--------------

CHANGE
::::::

 Core
  - add ilc-install.sh script FIXME: Fix PrepareOptions such that the parsing of options is done properly
 Interfaces
  - in ILCJob, possibility to run Mokka and Marlin in one job

Version v1r2p0
--------------

CHANGE
::::::

 Core
  - rewrite of SQLwrapper

Version v1r1p0
--------------

CHANGE
::::::

 Core
  - start working on InputDataResolution
 ConfigurationSystem
  - adapt UsersAndGroups to LCD : comment references to LFC
 Interfaces
  - finish dev of LCDJob

BUGFIX
::::::

 Workflow
  - Fix several bugs

Version v1r0p0
--------------

NEW: first release



NEW
:::

 Core
  - first import
 ConfigurationSystem
  - first import
 Interfaces
  - first import
 Workflow
  - first import

