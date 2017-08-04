User-Created Libraries with existing Applications
=================================================

Libraries have dedicated support. This mostly depends on the application, in
particular for ``Marlin``, Please check if the application you want to run for has
specificities. All applications come with their own dependencies, so a user does
not need to take care of those. He should only take care of those libraries that
are not part of the software stack.

Let's say you have an application that depends on ``libSomething.so`` that is
not a default library (in the Marlin case, one can replace existing processors,
so this is covered in the Marlin section, in the Marlin case on also needs a
special directory structure is mandatory! See here).

You will copy this ``libSomething.so`` into a ``lib`` directory. Now, 2 solutions are possible:

1.) Just one job:


  If and only if you have just 1 job: Simply input the lib directory like this::

     job.setInputSandbox("lib")

  If you have a tar ball, you may as well use it::

     job.setInputSandbox("lib.tar.gz")

  as any tar ball is automatically untarred in the job running directory.

  Only directly add files if you have one job and you are testing your
  libraries. Because every job submitted uploads its sandbox to the main iLCDirac
  servers, filling its hard drive, and slowing down your job submission. All files
  are uploaded except those that start with LFN:.

2.) More than one job:

  You are planning on submitting many jobs: I recommend you to put that lib
  directory on the grid and specify the LFN in the job definition. How to do
  that?::

    tar czf lib.tar.gz lib/
    dirac-dms-add-file /ilc/user/i/initial/some/path/lib.tar.gz lib.tar.gz CERN-SRM

  The ``/ilc/user/...`` is the LFN (Logical File Name). The ``i/initial/`` part
  is user specific: you need to use your own iLCDirac user name. You can find it
  in the :doc`UserGuide/CommandReference/Others/dirac-proxy-init` output, check
  for username. The ``some/path/`` part is free to you, you can use whatever you
  want. There are a few limitations: you can not have a total number of
  subsequent directories greater than 14, and the final file name (here
  ``lib.tar.gz``) cannot be longer than 128 chars. The last element, :ref:`CERN-SRM`
  indicates the logical name of the Storage Element on which you wish to upload
  you file.

  This LFN is registered in the DIRAC File Catalog, so it can now be used in the
  job definition.

  You now have a file on the GRID that you wish to input for a series of
  jobs. You would use the following::

    job.setInputSandbox("LFN:/ilc/user/i/initial/some/path/lib.tar.gz")

  Notice the LFN: part that is used by DIRAC to identify the files that must be
  downloaded from the grid. If you omit it, the job submission should fail
  because an input sandbox file will be missing. The fact that it's a tar ball
  does not matter, it will be untarred automatically.

  * Replicate your libraries:

    For better reliability and reducing load on individual storage elements you
    should also replicate your library file to a few storage elements by
    running::

      dirac-dms-replicate-lfn /ilc/user/i/initial/some/path/lib.tar.gz DESY-SRM
      dirac-dms-replicate-lfn /ilc/user/i/initial/some/path/lib.tar.gz RAL-SRM
      dirac-dms-replicate-lfn /ilc/user/i/initial/some/path/lib.tar.gz CERN-DIP-4
      dirac-dms-replicate-lfn /ilc/user/i/initial/some/path/lib.tar.gz CERN-DST-EOS
      dirac-dms-replicate-lfn /ilc/user/i/initial/some/path/lib.tar.gz PNNL-SRM

  * Replacing your files:

    If you wish to replace the file, you cannot overwrite the file, you need
    first to issue a dirac-dms-remove-files
    /ilc/user/i/initial/some/path/lib.tar.gz then re upload. The
    dirac-dms-remove-files command will remove the file from all storage
    elements.

    Moving a file cannot be done on the GRID: if really needed, you need to get
    the file (dirac-dms-get-file /ilc/...) then remove it from the GRID (same as
    above), then re upload it to the new location. Don't forget to replicate the
    files again.

  * Calice VO:

    Warning, important When running with the CALICE VO, the path has a different
    beginning: ``/calice/users/i/initial``. Notice the s at users. Also, CERN-SRM is
    not a valid storage element for CALICE users, so DESY-SRM or IN2P3-SRM must
    be preferred.


Custom Marlin Processors
------------------------

If you want to run with your own processors, the lib directory **must** have the
following structure because Marlin is sensitive to the difference between a
Processor library and a non processor library

* Libraries that your processors depend on must go under ``lib/lddlib/``. It is
  recommended to put the versioned libraries here as well, i.e., something like
  ``libUser.so``, as well as ``libUser.so.5.7``

* The processor libraries **must** be under ``lib/marlin_dll/``
  Any ``MARLIN_DLL`` file must end on ``.so`` (not ``.so.xyz``)
