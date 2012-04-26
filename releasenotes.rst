----------------
Package ILCDIRAC
----------------

Version v10r0p0
---------------

NEW
:::

 Core
  - WhizardOptions: add method to get the options as python dict
 ProcessProductionSystem
  - LesHouchesFileManager: service that provides the content of the LesHouches files on request, does not require them to be installed on the grid.
 Workflow
  - Store the cross section and its error for every job and every process in WhizardAnalysis, stored in workflow_commons['Info']

CHANGE
::::::

 Workflow
  - Add registration of additional info (workflow_commons['Info']) for every file BUgFIX: Handle the 350GeV case in OverlayInput

BUGFIX
::::::

 Interfaces
  - Handling of 350GeV in ProductionJob

