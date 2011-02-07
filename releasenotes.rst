----------------
Package ILCDIRAC
----------------

Version v1r14p0
---------------

NEW
:::

 Interfaces
  - SID production submission scripts
  - SID chain job submission scripts, and directory containing necessary files

CHANGE
::::::

 Core
  - software addition uses Request object for replication.

BUGFIX
::::::

 Core
  - now remove system libs from all application on site. In the future, should remove them at tar ball creation time
 Workflow
  - Pass basename of xml file in LCSIM instead of parameter value

