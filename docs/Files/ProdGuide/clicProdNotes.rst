CLIC Production Manager Guide
=============================



Creating New GG Hadron Simulation Files
---------------------------------------

1. Simulate gg->hadron files via the usual production script
   `dirac-clic-make-productions` using this configuration for different energies:

   .. code-block:: ini
   
      ## background simulation
      prodGroup = GGhadronSimulation_%(detectorModel)s
      ProdTypes = Sim
      energies =                     350,    1400,     3000
      processes =                  gghad,   gghad,    gghad
      eventsPerJobs =               1000,    1000,      100
      prodids =                        1,       2,        1
      
2. When the productions are created add the *DetectorModel* meta tag to the
   folder of the DetectorModel. By itself on the *DetectorType* is set for
   backward compatibility reasons. With the
   :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-filecatalog-cli` ::

     meta set /ilc/prod/clic/<energy>/gghad/<detectorModel>/ DetectorModel <DetectorModel>

3. If overlay events at a certain energy are to be used for jobs with a
   different energy, add them to the configuration system with a name like
   *gghad3tev* in the
   ``/Operations/Defaults/Overlay/clic_opt/500gev/CLIC_o3_v12/gghad3tev`` section:

   .. code-block:: ini

     prodGroup = DiJets_%(detectorModel)s
     ProdTypes = Split, Sim, RecOver
     energies =                       91,    100,    200,    380,    500,    750,   1000,  1500,    2000,   3000,
     processes =                   Z_uds,  Z_uds,  Z_uds,  Z_uds,  Z_uds,  Z_uds,  Z_uds, Z_uds,   Z_uds,  Z_uds,
     eventsPerJobs =                  50,     50,     50,     50,     50,     50,     50,    50,      50,     50,
     prodids =                    600001, 600002, 600003, 600004, 600005, 600006, 600007, 600008, 600009, 600010,
     eventsInSplitFiles        =    1000,   1000,   1000,   1000,   2500,   1000,   1000,   2500,   1000,   1000,
     MoveTypes = Gen, Sim, Rec
     move = True
     overlayEvents = 3TeV
