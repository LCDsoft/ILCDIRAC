CLIC Production Manager Guide
=============================



Creating New GG Hadron Simulation Files
---------------------------------------

1. Simulate gg->hadron files via the usual production script
   `dirac-clic-make-productions` using this configuration for different energies::
   
      ## background simulation
      prodGroup = GGhadronSimulation_%(detectorModel)s
      ProdTypes = Sim
      energies =                     350,    1400,     3000
      processes =                  gghad,   gghad,    gghad
      eventsPerJobs =               1000,    1000,      100
      prodids =                        1,       2,        1
      NumberOfEventsInBaseFiles =  
      
2. When the productions are created add the *DetectorModel* meta tag to the
   folder of the DetectorModel. By itself on the *DetectorType* is set for
   backward compatibility reasons. With the
   :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-filecatalog-cli` ::

     meta set /ilc/prod/clic/<energy>/gghad/<detectorModel>/ DetectorModel <DetectorModel>
