#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

import unittest
import sys

sys.path.append( '.' )
sys.path.append( '../lib/' )

import sqlite3
import make_test_databases
import geocode_setup


class TestGeocodeSetup(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        make_test_databases.remove_existing_databases()
        self.conn = sqlite3.connect("hashTbl.sqlite3")
        self.c = self.conn.cursor()
        geocode_setup.create_sql_helper_functions(self.conn)
        geocode_setup.geocode_db_initialize(self.c)
        geocode_setup.loc_create_table(self.c)

    def setUp(self):
        pass

    def test_fix_city_country(self):
        make_test_databases.make_assignee_db()
        geocode_setup.fix_city_country(self.c)
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
        make_test_databases.make_inventor_db()
        geocode_setup.fix_state_zip(self.c)
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
        geocode_setup.create_loc_indexes(self.conn)
        geocode_setup.create_usloc_table(self.c)
        rows = self.c.execute('select * from usloc').fetchall()
        element = rows[0][0]
        self.assertTrue(0 == element, "{0} should be {1}".format(element,0))
        element = rows[3][3]
        self.assertTrue('ABEL' == element,"{0} should be {1}".format(element, 'ABEL'))

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(self):
        pass

if __name__ == '__main__':
    unittest.main()
