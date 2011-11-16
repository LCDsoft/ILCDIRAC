#!/bin/sh

version=$1
if [ -z "$version" ]; then
  version=v4r1p1
fi

extension=$2
if [ -z "$extension" ]; then
  extension=ILC
fi


wget http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/dirac-install
chmod +x dirac-install
./dirac-install -l ILCDIRAC -r $version

vo=ilc
setup=ILC-Production
csserver=dips://volcd01.cern.ch:9135/Configuration/Server

scripts/dirac-configure -V $vo -S $setup -C $csserver -d --SkipCAChecks
