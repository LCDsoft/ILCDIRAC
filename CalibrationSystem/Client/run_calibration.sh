#!/bin/bash

source $1/init_ilcsoft.sh # Directory which to source
/usr/bin/krenew
export KRB5CCNAME=FILE:./credentials.krb5 
export AKLOG=/usr/bin/aklog
CLIENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # Gets the full path to this file
python $CLIENT_DIR/CalibrationDistributedScript.py $2 # StartFrom
