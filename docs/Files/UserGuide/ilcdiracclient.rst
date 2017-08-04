Setting up an iLCDirac environment
==================================

.. warning::

   The DIRAC environment is usually incompatible with other environments, and
   therefore requires a dedicated terminal/shell. Do not add the iLCDirac bashrc
   file to your default shell environment. Only source the iLCDirac setup scrip
   when you need it


.. contents::


Using a pre existing installation (e.g. at CERN)
------------------------------------------------

If you rely on someone's installation (or your own) you have normally access to
the directory in which iLCDirac has been installed. In that directory there is a
setup file, ``bashrc`` or ``cshrc`` depending on your shell, that needs sourcing to get
the right environment. For example, for CERN users, you can run::


  # bash users
  source /afs/cern.ch/eng/clic/software/DIRAC/bashrc
  # (t)csh users
  source /afs/cern.ch/eng/clic/software/DIRAC/cshrc

Once this file has been sourced, you get access to all the DIRAC and iLCDirac
commands, as well as the python API. You can proceed to the `Job section <submittingjobs>`_.


Getting a proxy
```````````````

.. note ::

  See :ref:`convCert` for creating the certificate files DIRAC
  requires.


Once you have sourced the DIRAC environment file, you can run::

  dirac-proxy-init -g ilc_user

or::

  dirac-proxy-init -g calice_user

depending on the **VO** you want a proxy for. ``ilc_user`` and ``calice_user``
are the default user groups for the respective VOs. The two VOs have access to
different resources, it's sometimes needed to get the right proxy (in particular
for data access). Also, this has an effect on the grid sites on which one can
run, for instance, only the DESY-HH and IPNL sites are available to the CALICE
users (in ILCDIRAC, and for the moment).



Installing iLCDirac
-------------------

When not having access to a pre installed DIRAC, you need to install it
yourself. The procedure is as follows:

.. code-block:: bash

  wget -np -O dirac-install https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py --no-check-certificate
  chmod +x dirac-install
  ./dirac-install -V ILCDIRAC


This also creates the ``bashrc`` / ``cshrc`` file needed to get the DIRAC
environment that you obtain with::

  # bash users
  source bashrc
  # (t)csh users
  source cshrc

Then you need to configure DIRAC (make the client know where to get the services
from). This requires the presence of a valid grid proxy. First, if you don't
have the pem files, you can get them with::

  dirac-cert-convert.sh grid.p12

where ``grid.p12`` is the file you got from the web browser export. If you
already have the pem files, this can be skipped.

To get a valid proxy, run::

  dirac-proxy-init -x

.. warning::

   This is not a DIRAC proxy. A DIRAC proxy is obtained by omitting the ``-x``. Do
   not use ``-x`` for anything but installing DIRAC. See below how to obtain a valid
   proxy to use the system.

Then run the configuration::

  dirac-configure -S ILC-Production -C dips://voilcdirac01.cern.ch:9135/Configuration/Server \
  --SkipCAChecks

In principle, if this commands runs fine, you should be able to got to the next section.

Don't forget to source the ``bashrc`` / ``cshrc`` file whenever using DIRAC.


Updating iLCDirac
`````````````````

In the ``$DIRAC`` directory, run::

  dirac-install -V ILCDIRAC

.. _caAndCRLs:

Certification Authorities and Certificate Revocation Lists
``````````````````````````````````````````````````````````

If you are installing your own iLCDirac client, you have to keep the
Certification Authorities (CAs) and Certificate Revocation Lists (CRLs)
up-to-date. If your system installs and updates these files automatically you
don't have to do anything. See if the folder ``/etc/grid-security/certificates``
exists.

If you don't have this folder, you need to occasionally update the files
yourself. They will be located in the ``$DIRAC/etc/grid-security/certificates``
folder in this case.

Use the :doc:`AdministratorGuide/CommandReference/dirac-admin-get-CAs` command to update the files.

A clear sign of when to run the above command is an error message during the
call to :doc:`UserGuide/CommandReference/Others/dirac-proxy-init` about CRLs being out-of-date.

In this case, add the following line to ``$DIRAC/bashrc``::

  export X509_CERT_DIR=$DIRAC/etc/grid-security/certificates

Then source the ``bashrc`` file again.


Error Missing LCG-Bundles
`````````````````````````

If you notice an error message about "Cannot download DIRAC-lcg-20XX-YY-ZZ-<OS,
Architecture, libc version>-python27.tar.gz"

If you do not need to copy files to or from a grid StorageElement you can ignore
the missing LCG Bundles. If you do need to access files, read the next section.

Alternative to LCG-Bundles For some operating systems the lcg-bundles will not
be available (OSX, Ubuntu, ...). Job submission is not affected by this. To
access files you can configure dirac to use the StorageElementProxy service.

See here: `IlcDiracSEProxy`

Or even better install a docker container (get the image ilcdirac/slc6-base) and
install an iLCDirac container inside the container.
