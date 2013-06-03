#!/bin/bash

# dump the citations file to CSV so we can regex it
echo "exporting citations table..."
sqlite3 citation.sqlite3 < export_citations.sql
echo "stripping leading zero..."
perl -pi -e "s/^D0/D/g;" -e "s/^0//g;" citation_tmp.csv
echo "re-importing into citations table..."
sqlite3 citation_stripped.sqlite3 < import_citations.sql
