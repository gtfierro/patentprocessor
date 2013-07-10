#!/usr/bin/env python

import sys
import sqlite3
import unittest

import make_test_databases

sys.path.append('../')
import lib.geocode_setup as geocode_setup
import lib.geocode_replace_loc as geocode_replace_loc

class TestGeocodeReplaceLoc(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        make_test_databases.remove_existing_databases()
        make_test_databases.make_assignee_db()
        make_test_databases.make_inventor_db()
        self.conn = sqlite3.connect("hashTbl.sqlite3")
        self.cursor = self.conn.cursor()
        geocode_setup.create_sql_helper_functions(self.conn)
        geocode_setup.create_hashtbl(self.cursor, self.conn)

    def test_domestic_sql(self):
        query = geocode_replace_loc.domestic_sql() % (-1, -1)
        result = self.cursor.execute(query)
        rows = result.fetchall()
        element = rows[0][2]
        self.assertTrue('FAIRHAVEN'==element, "{0} should be {1}".format(element,'FAIRHAVEN'))
        query = geocode_replace_loc.domestic_sql() % (0, 0)
        result = self.cursor.execute(query)
        element = rows[3][2]
        self.assertTrue('MOUNT KISCO'==element, "{0} should be {1}".format(element,'MOUNT KISCO'))
        pass

    def test_domestic_block_remove_sql(self):
        query = geocode_replace_loc.domestic_block_remove_sql() % (-1, -1)
        result = self.cursor.execute(query)
        rows = result.fetchall()
        element = rows[4][2]
        self.assertTrue('BOCA RATON'==element, "{0} should be {1}".format(element,'BOCA RATON'))
        pass

    def test_domestic_first3_jaro_winkler_sql(self):
        query = geocode_replace_loc.domestic_first3_jaro_winkler_sql() % (-1, -1, geocode_setup.get_first3_jaro_required(), -1)
        result = self.cursor.execute(query)
        rows = result.fetchall()
        element = rows[6][6]
        self.assertTrue('LOGAN'==element,"{0} should be {1}".format(element,'LOGAN'))
        pass

    def test_domestic_last4_jaro_winkler_sql(self):
        query = geocode_replace_loc.domestic_last4_jaro_winkler_sql() % (-1, -1, geocode_setup.get_last4_jaro_required(), -1)
        result = self.cursor.execute(query)
        rows = result.fetchall()
        element = rows[8][4]
        self.assertTrue('US'==element,"{0} should be {1}".format(element,'US'))
        pass

    def test_foreign_full_name_1_sql(self):
        query = geocode_replace_loc.foreign_full_name_1_sql() % (-1, -1)
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1,num_rows, "{0} should be {1}".format(num_rows,-1))
        pass

    def test_foreign_full_name_2_sql(self):
        query = geocode_replace_loc.foreign_full_name_2_sql() % (-1, -1)
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1,num_rows, "{0} should be {1}".format(num_rows,-1))    
        pass

    def test_foreign_short_form_sql(self):
        query = geocode_replace_loc.foreign_short_form_sql() % (-1, -1)
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1, num_rows , "{0} should be {1}".format(num_rows,-1))
        pass

    def test_foreign_block_split_sql(self):
        query = geocode_replace_loc.foreign_block_split_sql() % (-1, -1)
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1, num_rows, "{0} should be {1}".format(num_rows,-1))
        pass

    def test_foreign_first3_jaro_winkler_sql(self):
        query = geocode_replace_loc.foreign_first3_jaro_winkler_sql() % (-1, -1, "20.92", -1)
        result = self.cursor.execute(query)
        num_rows= result.rowcount
        self.assertEquals(-1, num_rows, "{0} should be {1}".format(num_rows, -1))
        pass

    def test_foreign_last4_jaro_winkler_sql(self):
        query = geocode_replace_loc.foreign_last4_jaro_winkler_sql() % (-1, -1, "20.90", -1)
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1,num_rows,"{0} should be {1}".format(num_rows, -1))
        pass

    def test_domestic_2nd_layer_sql(self):
        query = geocode_replace_loc.domestic_2nd_layer_sql()
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1, num_rows,"{0} should be {1}".format(num_rows,-1))
        pass

    def test_domestic_first3_2nd_jaro_winkler_sql(self):
        query = geocode_replace_loc.domestic_first3_2nd_jaro_winkler_sql() % "14.95"
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1,num_rows,"{0} should be {1}".format(num_rows, -1))
        pass

    def test_foreign_full_name_2nd_layer_sql(self):
        query = geocode_replace_loc.foreign_full_name_2nd_layer_sql()
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1,num_rows,"{0} should be {1}".format(num_rows, -1))
        pass

    def test_foreign_full_nd_2nd_layer_sql(self):
        query = geocode_replace_loc.foreign_full_nd_2nd_layer_sql()
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1, num_rows,"{0} should be {1}".format(num_rows,-1))
        pass

    def test_foreign_no_space_2nd_layer_sql(self):
        query = geocode_replace_loc.foreign_no_space_2nd_layer_sql()
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1,num_rows,"{0} should be {1}".format(num_rows,-1))
        pass

    def test_foreign_first3_2nd_jaro_winkler_sql(self):
        query = geocode_replace_loc.foreign_first3_2nd_jaro_winkler_sql() % "24.95"
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1, num_rows,"{0} should be {1}".format(num_rows,-1))
        pass

    def test_domestic_zipcode_sql(self):
        query = geocode_replace_loc.domestic_zipcode_sql()
        result = self.cursor.execute(query)
        num_rows = result.rowcount
        self.assertEquals(-1, num_rows,"{0} should be {1}".format(num_rows,-1))
        pass


if __name__ == '__main__':
    unittest.main()
