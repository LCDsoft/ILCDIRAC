#!/bin/bash

function MokkaDatabaseDump() {
# Function based on the MokkaDumpScript by AdrianVogel
# This script creates a dump of the central Mokka geometry database. It can be regularly executed by cron.
# Adrian Vogel, DESY FLC, 2008-04-12, last change 2008-04-24
# set appropriate default file access permissions: allow write access for the group
    umask 0002
# host from which the dump should be fetched
#MYSQL_HOST="pollin1.in2p3.fr"
#    MYSQL_HOST="pccds03.cern.ch"
    MYSQL_HOST="polui01.in2p3.fr"
# username and password to access the host
    MYSQL_USER="consult"
    MYSQL_PASS="consult"
# common options together in one variable
    MYSQL_OPT="-h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS"
# additional options for mysqldump in one variable
    MYSQL_DUMP_OPT="--quote-names --lock-tables=false"
# the name of the dump file, containing the MySQL host, current date and time
#    MYSQL_DUMP="${MYSQL_HOST%%.*}_$(date +%Y-%m-%d_%H-%M-%S).sql"
### NAME OF THE OUTPUTFILE
    MYSQL_DUMP=CLICMokkaDB.sql
# fetch a list of all databases which should be dumped - we do not simply use "--all-databases" because we want to suppress `mysql` (access permissions), `test` (demo database from mysql_install_db), `information_schema` (pseudo-database in higher MySQL versions), and the Mokka temporary databases
    MYSQL_DBS="$(echo 'SHOW DATABASES;' | mysql $MYSQL_OPT | sed '1d;/^information_schema$/d;/^mysql$/d;/^test$/d;/^TMP_DB..$/d')"
# dump the selected databases and filter out some problematic properties (DEFAULT CHARSET, PACK_KEYS, ENGINE, TYPE) which are not needed here, reset the information about temporary databases
    mysqldump $MYSQL_OPT $MYSQL_DUMP_OPT --databases $MYSQL_DBS | \
	sed > $MYSQL_DUMP '
s/ DEFAULT CHARSET=[a-zA-Z0-9]\{1,\}//g
s/ PACK_KEYS=[a-zA-Z0-9]\{1,\}//g
s/ ENGINE=[a-zA-Z0-9]\{1,\}//g
s/ TYPE=[a-zA-Z0-9]\{1,\}//g
/^\/\*!.\{1,\}\*\/;$/d
/^INSERT INTO `tmp_databases` VALUES/s/\<[0-9]\{1,\}\>/0/g'
# delete the current dump if it contains no data and exit
    [[ -s $MYSQL_DUMP ]] || { rm $MYSQL_DUMP ; exit ; }
# delete the current dump if it contains error messages and exit
    grep -q '^Usage:' $MYSQL_DUMP && { rm $MYSQL_DUMP ; exit ; }
# set up clean temporary databases
    for ((i = 0; i < 50; i++)) ; do
	echo >> $MYSQL_DUMP "CREATE DATABASE \`TMP_DB$(printf "%02d" "$i")\`;"
    done
}
programname=Mokka

LD_LIBRARY_PATH_TEMP=$LD_LIBRARY_PATH
source /afs/cern.ch/eng/clic/software/DIRAC/bashrc
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_TEMP
platform=`dirac-architecture`

if [ $# -eq 0 ]; then
    echo "Please Specify the version of the tarball e.g. 0702ExeSL5, which will produce Mokka0702ExeSL5.tgz, and comment like \"fixed muon detector driver\"" 
    exit 1
elif [ $# -eq 2 ]; then 
    version=$1
    comment=$2
    tarballname=$programname$version.tgz
    echo Tarballname $tarballname
    if [ $(which Mokka&>/dev/null) $? -eq 0 ]; then
#	echo "Getting Newest DatabaseDump"
	echo "Getting DatabaseDump from polui"
	MokkaDatabaseDump
	
	mokkatarballfolder=$programname$version

	mkdir -p $mokkatarballfolder

	mokkadir=$(which ${programname})

	rsync -avzL $mokkadir $mokkatarballfolder/${programname}
	
    ##Dealing with the libraries for Mokka
#	ldd $mokkadir | grep "=>" | sed 's/.*=>/rsync -avzL /g' | sed "s/(.*)/$mokkatarballfolder/g"  > lddLog.sh
	chrpath -d $mokkatarballfolder/${programname}
	readelf -d $mokkatarballfolder/${programname} | grep RPATH
	if [ $? == 0 ]; then
	    echo "FOUND RPATH in Mokka Aborting!!"
	    exit 1
	fi
	mkdir -p $mokkatarballfolder/ConfigFiles
	rsync -avzlr $(dirname $(dirname $(dirname $mokkadir)))/particle.tbl  $mokkatarballfolder/ConfigFiles/particle.tbl
	string1=$(ldd $mokkadir | grep "=>" | sed 's/.*=>//g' | sed "s/(.*)//g")
	string=""
	for file in $string1; do
	    string="$file $string"
	done
##copy files locally first, then make a copy in the tarball folder
##this way we don't always have to copy them again, after we strip the rpath
	rsync -avzL $string temp/
	rsync -avzL $temp $mokkatarballfolder
	##Clean the libs that will fail the check sum validation
	python $DIRAC/ILCDIRAC/Core/Utilities/PrepareLibs.py $mokkatarballfolder

	for file in $( ls $mokkatarballfolder/*.so ); do
	    chrpath -d $file
	    readelf -d $file | grep RPATH
	    if [ $? == 0 ]; then
		echo "FOUND RPATH Aborting!!"
		exit 1
	    fi
	done

	if [ -n "$G4LEDATA" ] && [ -e $G4LEDATA ]; then
	    rsync -avzlr $G4LEDATA/* $mokkatarballfolder/G4LEDATA/
	else 
	    echo "Variable G4LEDATA not set $G4LEDATA"
	    exit 1;
	fi

	softwarefolder=/afs/cern.ch/eng/clic/software
	rsync -avzlr $softwarefolder/mysql4grid .
	#Move DB dump into Mokka folder
        rsync -av CLICMokkaDB.sql $mokkatarballfolder/

	echo "\E[034mComputing md5 checksum\E[030m"
	cd $mokkatarballfolder
        find . -type f -print0 | xargs -0 md5sum > md5_checksum.md5
	cd ..
	
	echo -e "\E[034mCreating Tarball, this might take some time\E[030m"
	tar zcf $tarballname $mokkatarballfolder/*  mysql4grid/*
	#echo -e "\E[031mCopying the file to clic/data/software"
	#cp -i $tarballname $softwarefolder/$tarballname
        echo -e "Adding to DIRAC CS and copying to final location"
        dirac-proxy-init -g ilc_prod
        echo "running dirac-ilc-add-software $platform $programname $version"
	dirac-ilc-add-software -P $platform -N $programname -V $version -C "$comment"
	echo -e "\E[030m"
	exit 0
    else
	echo -e "\E[031mMokka not found, environment not set, Aborting!\E[030m"
	exit 1
    fi
fi



## Command

#dirac-ilc-add-software --platform=`dirac-architecture` --name=Mokka --version=Mokka070705_G4943 --comment="Mokka version 070705 with Geant4 9.4.p03 and additional physics list"
