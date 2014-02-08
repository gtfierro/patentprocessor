#!/bin/bash

echo 'Running assignee disambiguation'
python lib/assignee_disambiguation.py $1

if [ $1 == "grant" ] ; then
  echo 'Running lawyer disambiguation'
  python lib/lawyer_disambiguation.py $1
fi

echo 'Running geo disambiguation'
python lib/geoalchemy.py $1
