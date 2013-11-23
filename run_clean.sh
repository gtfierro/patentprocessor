#!/bin/bash

echo 'Running assignee disambiguation'
for i in {a..z} ; do
    python lib/assignee_disambiguation.py $1 $i
done

if [ $1 = "grant" ]
    then
        echo 'Running lawyer disambiguation'
        for i in {a..z} ; do
            python lib/lawyer_disambiguation.py $i
        done
fi

echo 'Running geo disambiguation'
python lib/geoalchemy.py
