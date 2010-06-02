----------------
Package ILCDIRAC
----------------

Version v1r6p2
--------------

Version v1r6p1
--------------

Version v1r6p0
--------------
:NEW:
 Core
  - dirac-ilc-add-software, utility to add software in CS
:CHANGE:
 Interfaces
  - use elif statements
 Workflow
  - handle end of file reached in Mokka, avoid job declared as failed.
  - in Marlin if nb of events to process is not specified, use -1 i.e. all events.

Version v1r5p0
--------------
:CHANGE:
 Core
  - Take into account dependencies in installation phase.
  - Set convention that folder containing application is same as tar ball name minus .tar.gz and .tgz
 Workflow
  - Get base folder  name based on CS content, allows for multiple version of the same software to run FIXME: Running marlin: duplicated processors were not properly removed from MARLIN_DLL.

Version v1r4p0
--------------
:NEW:
 Interfaces
  - add DiracILC with specification of preSubmissionChecks
 DataManagementSystem
  - add DataManagementSystem, for dirac-dms-gridify-castor-file script
:CHANGE:
 Core
  - add in PrepareOptionsFiles the relevant methods for SLIC and LCSIM FIXME: fixes to the methods for Mokka and Marlin.
 Interfaces
  - add the relevant bits of code for the definition of SLIC and LCSIM jobs
  - add the possibility to run on mac files in mokka
 Workflow
  - add relevant workflow for SLIC and LCSIM

Version v1r3p0
--------------
:CHANGE:
 Core
  - add ilc-install.sh script FIXME: Fix PrepareOptions such that the parsing of options is done properly
 Interfaces
  - in ILCJob, possibility to run Mokka and Marlin in one job

Version v1r2p0
--------------
:CHANGE:
 Core
  - rewrite of SQLwrapper

Version v1r1p0
--------------
:CHANGE:
 Core
  - start working on InputDataResolution
 ConfigurationSystem
  - adapt UsersAndGroups to LCD : comment references to LFC
 Interfaces
  - finish dev of LCDJob
:BUGFIX:
 Workflow
  - Fix several bugs

Version v1r0p0
--------------

NEW: first release


:NEW:
 Core
  - first import
 ConfigurationSystem
  - first import
 Interfaces
  - first import
 Workflow
  - first import

