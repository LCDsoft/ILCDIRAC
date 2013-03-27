----------------
Package ILCDIRAC
----------------

Version v17r3p6
---------------

NEW
:::

 TransformationSystem
  - Transformation class to implement missing checkLimitedPlugin method

CHANGE
::::::

 Interfaces
  - Use our Transformation class
  - Promote setILDConfig to ProductionJob
 TransformationSystem
  - If max_tasks is <0, process all
 Workflow
  - Fix the metadata registration to have less burden on the service
  - simplify the logic in every module, promote stuff to ModuleBase

BUGFIX
::::::

 Interfaces
  - fix this ILDConfig handling

