----------------
Package ILCDIRAC
----------------

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

