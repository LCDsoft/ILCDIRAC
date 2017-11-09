
.. _submittingjobs:

Submitting Jobs
===============

There are :ref:`examplejobs` which can be adapted and submitted after sourcing
the :ref:`iLCDirac client <client>` and creating a user proxy::

  source /cvmfs/clicdp.cern.ch/DIRAC/bashrc
  dirac-proxy-init -g ilc_user
  python myJob.py


Running Jobs Locally
--------------------

To test jobs quickly a job can be run locally whiteout going through the iLCDirac
workload management system (WMS). This can be done by setting the ``mode``
parameter of the :func:`ILCDIRAC.Interfaces.API.DiracILC.DiracILC.submitJob` function to
``'local'``.

In addition the option for the ``LocalArea``, where the software is temporarily
installed, has to be set in ``${HOME}/.dirac.cfg`` file::

  LocalSite
  {
    LocalArea=/path/to/writableFolder
  }

If there is already a ``LocalSite`` section in your ``[.]dirac.cfg`` file just add the
variables to the section.
