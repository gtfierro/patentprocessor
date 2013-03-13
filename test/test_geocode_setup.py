#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

import unittest
import sys

sys.path.append( '.' )
sys.path.append( '../lib/' )

from make_test_databases import *
from geocode_setup import *


class TestGeocodeSetup(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        remove_existing_databases()
        self.conn = get_connection("hashTbl.sqlite3")
        self.c = get_cursor(self.conn)
        create_sql_helper_functions(self.conn)
        geocode_db_initialize(self.c)
        loc_create_table(self.c)

    def setUp(self):
        pass

    def test_fix_city_country(self):
        make_assignee_db()
        fix_city_country(self.c)
        # Inspect loc table in hashTbl, find something to assert.
        rows = self.c.execute('select * from loc order by city').fetchall()
        element = rows[0][1]
        self.assertTrue('BERLIN'==element,"{0} should be {1}".format(element,'BERLIN'))
        element = rows[0][3]
        self.assertTrue('DE' == element,"{0} should be {1}".format(element,'DE'))
        element = rows[2][1]
        self.assertTrue('FAIRHAVEN' == element,"{0} should be {1}".format(element,'FAIRHAVEN'))
        element = rows[2][2]
        self.assertTrue('MA' == element,"{0} should be {1}".format(element,'MA'))
        pass

    def test_fix_state_zip(self):
        make_inventor_db()
        fix_state_zip(self.c)
        # Inspect loc table in hashTbl, find something to assert.
        rows = self.c.execute('select * from loc order by city').fetchall()
        element = rows[3][1]
        self.assertTrue('FRISCO' == element,"{0} should be {1}".format(element,'FRISCO'))
        element = rows[0][5]
        self.assertTrue('BER' == element,"{0} should be {1}".format(element,'BER'))
        element = rows[6][1]
        self.assertTrue('LAGUNA HILLS' == element,"{0} should be {1}".format(element, 'LAGUNA HILLS'))
        pass

    def test_create_usloc_table(self):
        create_loc_indexes(self.conn)
        create_usloc_table(self.c)
        rows = self.c.execute('select * from usloc').fetchall()
        element = rows[0][0]
        self.assertTrue(92274 == element, "{0} should be {1}".format(element,92274))
        element = rows[3][3]
        self.assertTrue('115 FIRMS' == element,"{0} should be {1}".format(element, '115 FIRMS'))

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass

if __name__ == '__main__':
    unittest.main()
