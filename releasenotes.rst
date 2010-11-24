----------------
Package ILCDIRAC
----------------

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

