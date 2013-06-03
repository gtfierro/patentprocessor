-- removes the leading zeros for the Patent and Citation columns in the
-- citation.sqlite3 table. Run as the following:
-- $ sqlite3 /path/to/citation.sqlite3 < strip_zeros.sql

.mode csv
.separator ||
.output citation_tmp.csv
drop index if exists uqCit;
select * from citation;
