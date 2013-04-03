----------------
Package ILCDIRAC
----------------

Version v18r0p0
---------------

NEW
:::

 Interfaces
  - Script to get the info about a file or a prod
 Workflow
  - Finalize ILDRegisterOutputData
 ILCTransformationSystem
  - changed name

CHANGE
::::::

 Core
  - Use GenProcessID as ProcessID in Mokka
 Interfaces
  - Add support for the ProcessID meta data
 Workflow
  - Some reshuffling: promote number of event resolution to ModuleBase
  - Register all the meta data to the daughters
 ProcessProductionSystem
  - Drop the DataRecoveryAgent as it's in ILCTransformationSystem

BUGFIX
::::::

 Core
  - prevent deleting things that are not in the dict
 Interfaces
  - prevent deleting things that are not in the dict

