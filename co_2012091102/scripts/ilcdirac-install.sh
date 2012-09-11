#!/bin/sh

version=$1
if [ -z "$version" ]; then
  version=v9r0p4
fi

wget http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/dirac-install
chmod +x dirac-install
./dirac-install -l ILCDIRAC -r $version -i 26

vo=ilc
setup=ILC-Production
csserver=dips://volcd01.cern.ch:9135/Configuration/Server

scripts/dirac-configure -V $vo -S $setup -C $csserver -d --SkipCAChecks
echo ""
echo "You might want to run :"
echo "./dirac-install -l ILCDIRAC -r $version -i 26 -g 2011-06-06"
echo "$version=v9r0p4 for e.g."
echo "to get the grid UI (-g option), if available for your platform"
echo ""
echo "To get the proper environment, run source bashrc"
echo ""
echo "You can now obtain a proxy by running"
echo "dirac-proxy-init"