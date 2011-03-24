----------------
Package ILCDIRAC
----------------

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

