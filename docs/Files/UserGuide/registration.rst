Registration
============

Obtaining a Grid Certificate and Registration in ILCDirac
---------------------------------------------------------

Obtain a Grid Certificate
`````````````````````````
Get a grid certificate from your preferred certification authority.

See your local grid expert (there is usually at least one GRID user in every lab
nowadays) for the name of the certification authority corresponding to your
institute.

At CERN (for staff and users) that is
`<https://ca.cern.ch/ca/?template=ee2user>`_. Go to `New Grid User Certificate
<https://ca.cern.ch/ca/user/Request.aspx?template=ee2user>`_


Register to the Virtual Organisation
````````````````````````````````````
Then, depending on your VO, either go to

   `<https://grid-voms.desy.de:8443/voms/ilc>`_

or

   `<https://grid-voms.desy.de:8443/voms/calice>`_

or both to register yourself as a VO member.

.. note ::

  Don't forget to send an email to the VO adminstrators if you are registering
  for the first time.


Registration in iLCDirac is automatically done once you are fully registered in
the VOMS server

Use your Certificate to Obtain a DIRAC proxy
--------------------------------------------

.. _convCert:

Convert Certificate for DIRAC
`````````````````````````````

Once registered in the VO and in DIRAC, you need to create the =pem= files. For
this, export your certificate from your browser in p12 format.

How to do this is
documented in the
[[http://lcd-data.web.cern.ch/lcd-data/doc/HeadFirstTalk.pdf][HeadFirstTalk]]
slides. This =p12= will need to be converted.

The DIRAC installation comes with
a handy script to convert the ``p12`` in ``pem`` format. To get this script, you
need either to see the `Installing DIRAC <ilcdiracclient>` section up to the ``dirac-proxy-init -x``
or ``source the bashrc`` file from your existing local DIRAC installation. Then
run ::

  dirac-cert-convert.sh cert.p12

where ``cert.p12`` is the file you obtained from the browser export.
