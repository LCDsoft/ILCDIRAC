#!/bin/bash

programname=Marlin
source /afs/cern.ch/eng/clic/software/DIRAC/bashrc
platform=`dirac-architecture`
platform='x86_64-slc5-gcc43-opt'

if [ $# -eq 0 ]; then
    echo "Please Specify the version of the tarball e.g. 0108 and comment e.g. \"added new processor\""
    exit 1
else

YESNO=""
while [[ "$YESNO" != "n" && "$YESNO" != "y" ]]; do
    echo "Is the Background flag in MarlinReco enabled?[y/n]"
    read YESNO
    if [ "$YESNO" == "n" ]; then
	exit 1
    fi
done
YESNO=""


    version=$1
    comment=$2
    if [ $(which Marlin &>/dev/null) $? -eq 0 ]; then
	marlinExe=$(which ${programname})
	directoryname=$programname$version
	mkdir -p $directoryname/Executable
	mkdir -p $directoryname/LDLibs
	mkdir -p $directoryname/MARLIN_DLL
	mkdir -p $directoryname/ROOT
	mkdir -p $directoryname/Settings

	rsync -avL $marlinExe $directoryname/Executable/${programname}

	if [ -n "$PANDORASETTINGS" ] && [ -e $PANDORASETTINGS ]; then
	    echo "PANDORASETTINGS Found $PANDORASETTINGS"
	    mkdir -p $directoryname/Settings
	    rsync -avL $PANDORASETTINGS $directoryname/Settings/PandoraSettings.xml
	else
	    echo "PandoraSettings.xml not found! Aborting"
	    exit 1
	fi

    ##Dealing with the libraries for Marlin
#	ldd $marlinExe | grep "=>" | sed 's/.*=>/rsync -avL /g' | sed 's/(.*)/$directoryname\/LDLibs/g'  > lddLog.sh
#	source lddLog.sh
	string1=$(ldd $marlinExe | grep "=>" | sed 's/.*=>//g' | sed "s/(.*)//g")
	string=""
	for file in $string1; do
	    string="$file $string"
	done
	rsync -avL $string $directoryname/LDLibs
    ## Now Dealing with the libraries for the existing Processors
	for marlinLib in $( echo $MARLIN_DLL | sed s/":"/"\n"/g ); do
	    echo $marlinLib
	    if [[ $marlinLib =~ .*\/.* ]]; then
		if [[ $(basename $marlinLib ) != libPandoraPFANew.so &&  $(basename $marlinLib ) != libPandoraMonitoring.so ]]; then
		    rsync -avL  $marlinLib  $directoryname/MARLIN_DLL
		fi #DO NOT USE PANDORAPFANEW.SO!!!
#		ldd  $marlinLib  | grep "=>" | sed 's/.*=>/rsync -avL /g' | sed 's/(.*)/$directoryname\/LDLibs/g' > lddLog.sh
#		source lddLog.sh

		string1=$(ldd $marlinLib | grep "=>" | sed 's/.*=>//g' | sed "s/(.*)//g")
		string=""
		for file in $string1; do
		    string="$file $string"
		done
#		echo "STRING $string"
		rsync -avL $string $directoryname/LDLibs
	    else
		if [[ $(basename $marlinLib ) != libPandoraPFANew.so &&  $(basename $marlinLib ) != libPandoraMonitoring.so ]]; then
		    rsync -avL $( find $( echo $LD_LIBRARY_PATH | sed s/":"/"\n"/g ) -name $marlinLib ) $directoryname/MARLIN_DLL
		fi #DO NOT USE PANDORAPFANEW.SO!!!

    ## Copying needed libraries for the processorfiles
#		ldd $( find $( echo $LD_LIBRARY_PATH | sed s/":"/"\n"/g )  -name $marlinLib ) | grep "=>" | sed 's/.*=>/rsync -avL /g' | sed 's/(.*)/$directoryname\/LDLibs/g' > lddLog.sh
#		source lddLog.sh

		string1=$( ldd $( find $( echo $LD_LIBRARY_PATH | sed s/":"/"\n"/g )  -name $marlinLib ) | grep "=>" | sed 's/.*=>//g' | sed "s/(.*)//g" )
		string=""
#		echo "STRING1 $string1"
		for file in $string1; do
		    string="$file $string"
		done

#		echo "STRING $string"
		rsync -avL $string $directoryname/LDLibs
	    fi
	done
	python $DIRAC/ILCDIRAC/Core/Utilities/PrepareLibs.py $directoryname/LDLibs
	rsync --exclude '.svn' -av ${ROOTSYS}/lib ${ROOTSYS}/etc ${ROOTSYS}/bin  $directoryname/ROOT


	##Drop rpath
	chrpath -d $directoryname/Executable/${programname}
	readelf -d $directoryname/Executable/${programname} | grep RPATH
	if [ $? == 0 ]; then
	    echo "FOUND RPATH Aborting!!"
	    exit 1
	fi

	for file in $( ls $directoryname/LDLibs/*.so ); do
	    chrpath -d $file
	    readelf -d $file | grep RPATH
	    if [ $? == 0 ]; then
		echo "FOUND RPATH Aborting!!"
		exit 1
	    fi
	done

	for file in $( ls $directoryname/MARLIN_DLL/*.so ); do
	    chrpath -d $file
	    readelf -d $file | grep RPATH
	    if [ $? == 0 ]; then
		echo "FOUND RPATH Aborting!!"
		exit 1
	    fi
	done



        #Now we replace all processor libraries in LD_LIBS with links to the Libraries in the MARLIN_DLL folder
	cd $directoryname/LDLibs
	for file in $(ls --color=never *.so.*); do ls ../MARLIN_DLL/${file%.so.*}.so &> /dev/null && ln -sf ../MARLIN_DLL/${file%.so.*}.so $file ; done
	cd ../..

	echo $ROOTSYS

	echo -e "**** Computing checksum, can be slow ****"
        cd $directoryname
        find . -type f -print0 | xargs -0 md5sum > md5_checksum.md5.tmp
	cat md5_checksum.md5.tmp | grep -v md5_checksum.md5 > md5_checksum.md5
        cd ..

	echo -e "**** Creating Tarball, this might take some time ****"

	tar czf $directoryname.tgz $directoryname/*

	#echo -e "\E[031mCopying the file to clic/data/software"
	#cp -i $directoryname.tgz /afs/cern.ch/eng/clic/data/software/$directoryname.tgz
	echo -e "Adding to DIRAC CS and copying to final location"
	dirac-proxy-init -g ilc_prod
	echo $platform $programname $version "$comment"
	dirac-ilc-add-software -P $platform -N $programname -V $version -C "$comment"
	exit 0
    else
	echo -e "***** Can't find Marlin, environment not set: Aborting *****"
	exit 1
    fi
fi
