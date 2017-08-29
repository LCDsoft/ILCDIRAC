Using the StorageElementProxy
=============================

Instead of directly accessing the GRID StorageElements one can use the
StorageElement Proxy to access some files.

This should not be used to upload or download Gigabytes of files, because there
is not a lot of bandwidth for this service, but it can be used to transfer a few
files.

Add this to your ``$HOME/.dirac.cfg`` or ``$DIRAC/etc/dirac.cfg`` file.::

  LocalSite{
     StorageElements
    {
      ProxyProtocols=srm
    }
  } 
  
If there is already a LocalSite section in your dirac.cfg file just add the
StorageElements part to this section.

This will instruct copy commands to use the proxy service for the srm protocol,
which is used by most StorageElements.
