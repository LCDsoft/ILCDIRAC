#!/bin/sh

wget -O dirac-install -np  https://raw.github.com/DIRACGrid/DIRAC/master/Core/scripts/dirac-install.py  --no-check-certificate
chmod +x dirac-install
./dirac-install -V ILCDIRAC

vo=ilc
setup=ILC-Production
csserver=dips://volcd01.cern.ch:9135/Configuration/Server

scripts/dirac-configure -V $vo -S $setup -C $csserver -d --SkipCAChecks
echo ""
echo "You might want to run :"
echo "./dirac-install -V ILCDIRAC -g 2012-02-20"
echo "to get the grid UI (-g option), if available for your platform"
echo ""
echo "To get the proper environment, run source bashrc"
echo ""
echo "You can now obtain a proxy by running"
echo "dirac-proxy-init"
