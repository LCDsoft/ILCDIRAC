iLCDirac for Production Managers
================================

Documentation about the TransformationSystem
--------------------------------------------

There is a guide about what the TransformationSystem does: :doc:`AdministratorGuide/Systems/Transformation/index`

Configuration in the ConfigurationSystem
----------------------------------------

Some options for productions can be steered via the `configuration system <https://voilcdiracwebapp.cern.ch/DIRAC/?view=tabs&theme=Grey&url_state=1|*DIRAC.ConfigurationManager.classes.ConfigurationManager:,>`_

Which log files to store
````````````````````````

Operations->Defaults->LogFiles -> [Experiment] -> Extension

Set the patterns to match for logfiles to store, e.g.: ".log"

Which SE to use for failover
````````````````````````````

Operation->Defaults->Production-> [Experiment] -> FailOverSE

The the StorageElement to use as failover, do not set to the same SE as your main output SE.
The FailoverSE is only temporally used to store files, when the main SE is not available.


Base path for production output
```````````````````````````````

Operation->Defaults->Production-> [Experiment] -> BasePath

The LFN path where files for given experiment are stored.
Please note: If this path is changed there might be changes needed in the code!


File Catalogs to register production data in
````````````````````````````````````````````

Operation->Defaults->Production-> [Experiment] -> Catalogs
Only the Dirac FileCatalog is used
