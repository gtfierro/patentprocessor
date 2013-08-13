
clean:
	rm -rf *~ *.pyc *.log

spotless: clean
	rm -rf *.sqlite3 tmp alchemy.db *-journal disambiguator.csv
