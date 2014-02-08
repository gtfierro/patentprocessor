#!/bin/bash

rm disambiguator.csv
echo 'Running consolidation for disambiguator'
thisyear=`date "+%Y"`
for i in $(seq 1975 $thisyear) ; do
    python consolidate.py $1 $i
done
