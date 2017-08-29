Frequently Asked Questions
==========================

.. contents::



Error Messages
--------------


I keep getting Could not verify credential errors
`````````````````````````````````````````````````

You see errors like::

  [SE][GetSpaceTokens][] httpg://srm-public.cern.ch:8443/srm/managerv2: CGSI-gSOAP running on pclcd15.cern.ch reports Error initializing context
  GSS Major Status: Authentication Failed

  GSS Minor Status Error Chain:
  globus_gsi_gssapi: SSLv3 handshake problems
  globus_gsi_callback_module: Could not verify credential
  globus_gsi_callback_module: Could not verify credential
  globus_gsi_callback_module: Invalid CRL: The available CRL has expired

This means that the local copy of the certificate revocation list (CRL)
expired. Please run
:doc:`AdministratorGuide/CommandReference/dirac-admin-get-CAs` to update you
copy, or if you don't have the right to run the command yourself, ask your local
dirac administrator. See also :ref:`caAndCRLs`




DataManagement
--------------



How do upload/download/remove files to the Grid?
````````````````````````````````````````````````

Have a look at the :doc:`DataManagement Tutorial
<UserGuide/Tutorials/DataManagementBasic/index>` and the
:doc:`UserGuide/CommandReference/DataManagement/index`

* :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-add-file`
* :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-remove-files`
* :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-get-file`

How to check for replicas of a file
```````````````````````````````````

* :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-lfn-replicas`

How to replicate a file
```````````````````````

* :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-replicate-lfn`

Or use the replicate command in the :doc:`UserGuide/CommandReference/DataManagement/dirac-dms-filecatalog-cli`.

See the list of available :ref:`storageelements`


Jobs
----


My Job Submission is Very Slow
``````````````````````````````

I the job repository gets too large your job submission becomes very slow. Use
different job repository files, for example name the repository file after your
job group::

   jobGroup = "resonableDescription_take1"
   dirac = DiracILC(True, jobGroup+".rep")


How do I get the list of files that were uploaded on a SE by my jobs?
`````````````````````````````````````````````````````````````````````

See the command: `dirac-repo-create-lfn-list`

This will print on screen the list of files for each job, so you would probably
want to redirect the output to a text file.

That command might take some time, depending on how many jobs there are.


How do I retrieve the uploaded output data of my jobs?
``````````````````````````````````````````````````````

See the command: `dirac-repo-retrieve-jobs-output-data`

My jobs keep failing to upload output data
``````````````````````````````````````````

If an outputfile already exists on the grid, your job will not be able to
overwrite it. You have to either delete your outputfiles before submitting your
jobs again or use, for example, the jobgroup as a subfolder to differentiate
different job groups Use the jobgroup to separate outputfiles in subdirectories.::

  jobGroup = "jetReco_take1"
  ...
  job.setOutputData(["somefile1","somefile2"],"some/path/"+jobGroup,"CERN-SRM")

Change jobGroup whenever there is a new set of steering files, parameters or
whatever to avoid trying to overwrite your outputfiles

If you no longer need a set of output files, please remove them from the
storage.


I want to use my own Marlin processors
``````````````````````````````````````

I need to use my own processors
See also here: `userlibraries`

It's fully taken in account in dirac. For that, you'll need to compile them
against a version that dirac knows. And we defined a directory containing those
version on ``cvms`` under ``/cvmfs/clicdp.cern.ch/ilcsoft/builds`` or ``/cvmfs/ilc.desy.de/sw/``

So simply setup the env, use ``cmake`` including the ``ILCSoft.cmake`` in the
directory of your choice from the available ones, and put your
processor/libraries in the proper directories as mentioned elsewhere.
