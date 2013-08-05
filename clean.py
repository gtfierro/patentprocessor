#!/usr/bin/env python

from lib import assignee_disambiguation
from lib import lawyer_disambiguation
from lib import geoalchemy
<<<<<<< HEAD
import datetime

t1 = datetime.datetime.now()

||||||| merged common ancestors
import datetime

t1 = datetime.datetime.now()

## run geocoding
#print "START: geocode", t1
#import lib.geocode
##print "   - Loc Merge", "\n   -", datetime.datetime.now()-t1
#print"DONE: geocode"
#print "   -", datetime.datetime.now()-t1

=======

>>>>>>> c740e03e1b1ddef82669973da6412b862b56aa88
# run assignee disambiguation and populate the Assignee table
assignee_disambiguation.run_disambiguation()

# run lawyer disambiguation
lawyer_disambiguation.run_disambiguation()

#Run new geocoding
geoalchemy.main()
