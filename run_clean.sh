#!/bin/bash

echo 'Running assignee disambiguation'
for i in {a..z} ; do
    python lib/assignee_disambiguation.py $i
done

echo 'Running lawyer disambiguation'
for i in {a..z} ; do
    python lib/lawyer_disambiguation.py $i
done

echo 'Running geo disambiguation'
python lib/geoalchemy.py
