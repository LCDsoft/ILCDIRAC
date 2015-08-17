#!/bin/bash

### 
# This program will create a tar ball suitable for running the program on the grid with ILCDIRAC
# Needs the chrpath and readelf utilities
###

if [ $# -eq 0 ]; then
    echo " Please state the name of the Program! " 
    exit 1
fi

programname=$1
programpath=$(which ${programname})
echo "Getting libraries for $programpath"

TARBALLNAME=lib.tar.gz
LIBFOLDER=lib
mkdir -p $LIBFOLDER

if [ $(which ${programname}&>/dev/null) $? -eq 0 ]; then
    
    rsync -avzL $programpath $LIBFOLDER/
    string1=$(ldd $programpath | grep "=>" | sed 's/.*=>//g' | sed "s/(.*)//g")
    string=""
    for file in $string1; do
	string="$file $string"
    done
    rsync -avzL $string $LIBFOLDER
    echo -e "Creating Tarball, this might take some time"

    # for file in $( ls --color=never $LIBFOLDER/* ); do
    # 	chrpath -d $file
    # 	readelf -d $file | grep RPATH
    # 	if [ $? == 0 ]; then
    # 	    echo "FOUND RPATH Aborting!!"
    # 	    exit 1
    # 	fi
    # done

    for file in libc.so* libc-2.5.so* libm.so* libpthread.so* libdl.so* libstdc++.so* libgcc_s.so.1*; do
	rm $LIBFOLDER/$file 2> /dev/null
    done

    tar zcf $TARBALLNAME $LIBFOLDER/*
    exit 0
else
    echo -e "$programname not found, environment not set, Aborting!"
    exit 1
fi
