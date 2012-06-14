----------------
Package ILCDIRAC
----------------

Version v10r1p0
---------------

CHANGE
::::::

 Interfaces
  - Allow for different background in OverlayInput, make sure the specified background exists.
 Workflow
  - Allow for multiple bkg in OverlayInput. Provides function to check valid backgrounds. TODO: Check against CS for valid background types (consistency with energy)

BUGFIX
::::::

 Core
  - Support for multiple overlay driver in LCSIM, each with different background. Default is gghad

