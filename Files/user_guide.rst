Pandora Calibration System User Guide
=====================================

This is the tutorial for end users of the CalibrationSystem component of iLCDirac.
It starts with a minimal hands-on guide to start your first calibration, afterwards the architecture of the system is explained (without being too technical; with a high abstraction) in case the user wants to further optimize their computation or for debugging.

Getting started
---------------

In principle, starting a calibration is very simple, you just need three (and a half) lines of code:

.. code-block:: python

    numberOfJobs = 100
    from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import setConfig, createCalibration
    result = createCalibration( 'steering_file.xml', 'software_version', [ '/afs/cern.ch/user/j/jebbing/particles/CLIC_o3_v08/gamma/10', '/afs/cern.ch/user/j/jebbing/particles/CLIC_o3_v08/mu-/10', <...> ], numberOfJobs )

Usually you will want to check that everything worked. In case of success, ``createCalibration`` returns you the ID of the newly created calibration in the DIRAC ``S_OK`` structure. This is needed to later fetch the result, so you usually want to change this to:

.. code-block:: python

    numberOfJobs = 100
    from ILCDIRAC.CalibrationSystem.Client.CalibrationClient import createCalibration
    result = createCalibration( 'steering_file.xml', 'software_version', [ '/afs/cern.ch/user/j/jebbing/particles/CLIC_o3_v08/gamma/10', '/afs/cern.ch/user/j/jebbing/particles/CLIC_o3_v08/mu-/10', <...> ], numberOfJobs )
    if not result[ 'OK ' ]:
      raise RuntimeError( 'Creating calibration failed! Something went wrong, fix this please' )
    calibrationID = result[ 'Value' ]

This will store the id in the variable ``calibrationID``.
Here's a quick rundown of the parameters passed to ``createCalibration``:

- SteeringFile: The steering file used
- SoftwareVersion: The version number
- InputFiles: The list of input files used by the calibration
- NumberOfJobs: The amount of jobs that are started initially. Please note that you should add a bit extra to your desired average number of workers running. You can expected 80-90% of this number to be used per step as a guarantee. This is because if a job fails or takes too long it is not resubmitted immediately/its result might not be counted. This is explained in more detail in the developer documentation, but the way resubmission roughly works is that we wait until too many jobs have failed and then resubmit as many as possible at once (since that is more efficient than resubmitting as soon as we detect a failure and puts less stress on the system). If a few workers take exceptionally long for a step, there is the possibility that the system already received enough results for this step and continues with the next without waiting for those slow workers, leading to less workers effectively being used than set here.

This will run your calibration jobs on the grid until they converge. Once the computation is complete, the result is stored in a database and can be accessed with:

.. code-block:: python

    from DIRAC.Core.DISET.RPCClient import RPCClient
    calibrationService = RPCClient( 'Calibration/Calibration' )
    result = calibrationService.getResult( calibrationID )

This assumes the ID of the calibration you started is still stored in ``calibrationID``.

Overview of the technical design
--------------------------------

The CalibrationSystem consists of three main parts - a Service, an Agent, and the code for the worker nodes. In essence, the CalibrationService is running all the time and waits for the user or the system to issue a command. Users can create a new Calibration by using the Client-provided method ``createCalibration``.
The worker nodes use the service to report back their interim results of each iteration of the calibration as well as asking ('polling') if there is a new parameter set available for their computation and if so for which step, as long as they've finished their computation and not yet received a new parameter set.
The Agent tells the service when to resubmit which jobs.
Without one of these commands (technical necessities have been omitted), the service will execute nothing - aside from initializing itself.

The Agent has a special method that it will execute every X seconds. The CalibrationSystem uses it to check the status of the system and thus to decide which calibration steps have been finished and which are still running. In short, the Agent will ask the DIRAC system for the status of all calibration jobs. Then it will check if any calibrations have too few jobs running and resubmit the ones necessary. Finally, it determines all calibrations which can advance to the next step in their computation and orders them to do so.

The Client runs a loop: Once the program is started on the worker node, it will ask the Service for its first set of parameters. Subsequently, it computes its result and reports this result to the service. After it has reported this result, it will continuously ask the Service every X seconds for a new set of parameters and at which part of the calibration the worker is. After the last step is computed, the program terminates on the worker node.
