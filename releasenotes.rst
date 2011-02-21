----------------
Package ILCDIRAC
----------------

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

