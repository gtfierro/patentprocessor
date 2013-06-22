#!/bin/bash

configfile=$1
numberofcores=$2

ipcluster start --n=$numberofcores &

python start.py $configfile

ipcluster stop
