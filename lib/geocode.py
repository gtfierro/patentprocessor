# Non_US http://earth-info.nga.mil/gns/html/gis_countryfiles.htm
# US     http://geonames.usgs.gov/domestic/download_data.htm

# NEED TO DO...
# CREATE INDEX IF NOT EXISTS idx_ctc0 ON gnsloc (SORT_NAME, CC1);

import datetime
# TODO: cover geocode setup functions with unit tests.
import sqlite3
import geocode_setup
# geocode_replace_loc consists of a series of functions,
# each with a SQL statement that is passed as a parameter
# to replace_loc. Uses temporary tables for handling
# intermediate relations.
import geocode_replace_loc

conn = sqlite3.connect("hashTbl.sqlite3")
c = conn.cursor()
geocode_setup.create_sql_helper_functions(conn)

print "Start setup for geocoding: ", datetime.datetime.now()
geocode_setup.create_hashtbl(c, conn)
print "Finish setup for geocoding: ", datetime.datetime.now()

# End of setup.
# Exiting here gets the initial hashTbl.sqlite3 file when
# executed as `python lib/geocode.py`
#exit()



# TODO: Unit test extensively.
def table_temp1_has_rows(cursor):
    return cursor.execute("SELECT count(*) FROM temp1").fetchone()[0] > 0

def replace_loc(script):

    c.execute("DROP TABLE IF EXISTS temp1")
    c.execute("CREATE TEMPORARY TABLE temp1 (jaro_match_value FLOAT, count INTEGER, \
        cityA TEXT, stateA TEXT, countryA TEXT, ncity TEXT, nstate TEXT, ncountry TEXT, nlat FLOAT, nlong FLOAT);")
    c.execute("INSERT OR REPLACE INTO temp1 %s" % script)

    #print_table_info(c)

    # TODO: Which tables will pass this conditional?
    if table_temp1_has_rows(c):
        geocode_replace_loc.create_loc_and_locmerge_tables(c)
        geocode_replace_loc.print_loc_and_merge(c)

    conn.commit()


# Prefixed tablename (loc) with with dbname (also loc)
print "Loc =", c.execute("select count(*) from loctbl.loc").fetchone()[0]

# TODO: Refactor the range call into it's own function, unit test
# that function extensively.

for separator_count in range(-1, c.execute("select max(separator_count(city)) from loctbl.loc").fetchone()[0]+1):

    sep = separator_count
    print "------", separator_count, "------"
    replace_loc(geocode_replace_loc.domestic_sql()                     % (sep, separator_count))
    replace_loc(geocode_replace_loc.domestic_block_remove_sql()        % (sep, separator_count))
    replace_loc(geocode_replace_loc.domestic_first3_jaro_winkler_sql() % (sep, sep, geocode_setup.get_jaro_required('domestic_first3'), separator_count))
    replace_loc(geocode_replace_loc.domestic_last4_jaro_winkler_sql()  % (sep, sep, geocode_setup.get_jaro_required('domestic_last4'), separator_count))
    replace_loc(geocode_replace_loc.foreign_full_name_1_sql()          % (sep, separator_count))
    replace_loc(geocode_replace_loc.foreign_full_name_2_sql()          % (sep, separator_count))
    replace_loc(geocode_replace_loc.foreign_short_form_sql()           % (sep, separator_count))
    replace_loc(geocode_replace_loc.foreign_block_split_sql()          % (sep, separator_count))
    replace_loc(geocode_replace_loc.foreign_first3_jaro_winkler_sql()  % (sep, sep, geocode_setup.get_jaro_required('foreign_first3'), separator_count))
    replace_loc(geocode_replace_loc.foreign_last4_jaro_winkler_sql()   % (sep, sep, geocode_setup.get_jaro_required('foreign_last4'), separator_count))

### End of for loop

print "------ F ------"

# TODO: Put these calls into a function.
replace_loc(geocode_replace_loc.domestic_2nd_layer_sql())
replace_loc(geocode_replace_loc.domestic_first3_2nd_jaro_winkler_sql() % (geocode_setup.get_jaro_required('domestic_first3_2nd')))
replace_loc(geocode_replace_loc.foreign_full_name_2nd_layer_sql())
replace_loc(geocode_replace_loc.foreign_full_nd_2nd_layer_sql())
replace_loc(geocode_replace_loc.foreign_no_space_2nd_layer_sql())
replace_loc(geocode_replace_loc.foreign_first3_2nd_jaro_winkler_sql()  % (geocode_setup.get_jaro_required('foreign_first3_2nd')))

conn.commit()
c.close()
conn.close()
