#!/usr/bin/env python

from lib import assignee_disambiguation
from lib import lawyer_disambiguation
from lib import geoalchemy

# run assignee disambiguation and populate the Assignee table
assignee_disambiguation.run_disambiguation()

# run lawyer disambiguation
lawyer_disambiguation.run_disambiguation()

#Run new geocoding
geoalchemy.main()
