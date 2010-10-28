----------------
Package ILCDIRAC
----------------

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

