#!/bin/bash

# Integration testing for the consolidate.py script

cd ..

##### Two rows

make spotless > /dev/null
./parse.py -p test/fixtures/xml/ -x ipg120327.two.xml -o .
mkdir -p tmp/integration/ipg120327.two

echo Starting clean...
python clean.py

echo Starting consolidate...
python consolidate.py

diff test/integration/consolidate/ipg120327.two/disambiguator.csv disambiguator.csv

### 18 rows

make spotless > /dev/null
./parse.py -p test/fixtures/xml/ -x ipg120327.18.xml -o .
mkdir -p tmp/integration/ipg120327.18

echo Starting clean...
python clean.py

echo Starting consolidate...
python consolidate.py

diff test/integration/consolidate/ipg120327.two/disambiguator.csv disambiguator.csv

## clean up after we're done

make spotless > /dev/null
