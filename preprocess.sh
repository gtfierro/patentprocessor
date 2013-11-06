#!/bin/bash

configfile=$1
numberofcores=$2

if [ -z $1 ]
  then
    echo "Please specify a config file as the first argument"
    exit
fi

if [ -z $2 ]
  then
    echo "Please specify a number of cores as the second argument"
    exit
fi

ipcluster stop

ipcluster start --n=$numberofcores --daemon

python start.py $configfile

ipcluster stop
