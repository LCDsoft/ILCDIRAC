----------------
Package ILCDIRAC
----------------

Version v16r9p0
---------------

CHANGE
::::::

 Core
  - always put the lib directory (if available) in the LD_LIBRARY_PATH
  - Don't bother with shared area if the local platform is incompatible with all supported platforms
  - add support for md5sum of software tar balls, both at creation and installation
  - Add limit in validity of lock file during installation
 ProcessProductionSystem
  - Remove software only from the shared area

