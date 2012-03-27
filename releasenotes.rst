----------------
Package ILCDIRAC
----------------

Version v9r0p0
--------------

CHANGE
::::::

 Core
  - Add verbose message in InputFilesUtilities, support change of numberofevents in LCSIM file
 Interfaces
  - Remove requirement on number of event per job in Marlin.
 Workflow
  - NumberOfEvents now changed also in LCSIM, don't change the NbEvts of workflow_commons except if it's >0

