#!/bin/bash

# First crack at building integration test framework for
# ensuring the preprocessing works correctly as we move
# forward fixing bugs, etc.

cd ..

echo 'Testing test/fixtures/xml/ipg120327.one.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipg120327.one
./parse.py -p test/fixtures/xml/ -x ipg120327.one.xml -o .

for table in application citation ipcr mainclass otherreference patent rawassignee rawinventor rawlawyer rawlocation subclass uspc usreldoc
do
  echo $table 'diffs...'
  sqlite3 -csv alchemy.sqlite3 "select * from ${table}"  > tmp/integration/ipg120327.one/${table}.csv
  # remove UUIDs from database dump because these change each time
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipg120327.one/${table}.csv
  diff test/integration/parse/ipg120327.one/${table}.csv tmp/integration/ipg120327.one/${table}.csv
done

echo 'Testing test/fixtures/xml/ipg120327.two.xml'
make spotless > /dev/null
mkdir -p tmp/integration/ipg120327.two
./parse.py -p test/fixtures/xml/ -x ipg120327.two.xml -o .

for table in application citation ipcr mainclass otherreference patent rawassignee rawinventor rawlawyer rawlocation subclass uspc usreldoc
do
  echo $table 'diffs...'
  sqlite3 -csv alchemy.sqlite3 "select * from ${table}"  > tmp/integration/ipg120327.two/${table}.csv
  # remove UUIDs from database dump because these change each time
  perl -pi -e 's/^[a-z0-9]{8}-([a-z0-9]{4}-){3}[a-z0-9]{12},//' tmp/integration/ipg120327.two/${table}.csv
  diff test/integration/parse/ipg120327.two/${table}.csv tmp/integration/ipg120327.two/${table}.csv
done

make spotless > /dev/null
mkdir -p tmp/integration/ipg120327.18
./parse.py -p test/fixtures/xml/ -x ipg120327.18.xml -o .

for table in assignee citation class inventor lawyer patdesc patent sciref usreldoc
do
  sqlite3 -csv ${table}.sqlite3 "select * from ${table}"  > tmp/integration/ipg120327.18/${table}.csv
  diff test/integration/parse/ipg120327.18/${table}.csv tmp/integration/ipg120327.18/${table}.csv
done

## clean up after we're done

make spotless > /dev/null
