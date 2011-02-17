----------------
Package ILCDIRAC
----------------

Version v1r15p3
---------------

NEW
:::

 Interfaces
  - Add MCGeneration as a possible Production type

CHANGE
::::::

 Workflow
  - Added memory requirement for java in LCSIM

BUGFIX
::::::

 Core
  - With new Script interface, our scripts would not work. Made ilc-proxy-init deprecated, use proxy-init instead
  - Overlay input for LCSIM did not work (created exception)

