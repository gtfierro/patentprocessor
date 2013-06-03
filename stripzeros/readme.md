These files are meant for stripping the leading zeros from the patent numbers found in citation.sqlite3, e.g.
turning something like D0526785 into D526785. Make sure strip_zeros.sh, export_citation.sql and import_citation.sql
are all in the same directory as your citation.sqlite3 file, then run `bash strip_zeros.sh` to edit in-place the
citation.sqlite3 file.
