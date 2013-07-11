
# Create SQL statements for passing in to function replace_loc
# in geocode.py. These statements were originally in geocode.py
# and have been put here both for debugging purposes and to
# modularize the process.

import datetime
import sys

# TODO: Unit test this so that it and the unit test can be
# eliminated in a future redesign. Also, ensure that this
# is the correct name for this function, and adjust accordingly.

def print_table_info(c):
    field = ["[%s]" % x[1] for x in c.execute("PRAGMA TABLE_INFO(temp1)")][2:6]
    var_f = ",".join(field)
    print "var_f: ", var_f


def print_loc_and_merge(cursor):
    VarX = cursor.execute("select count(*) from loc").fetchone()[0]
    VarY = cursor.execute("select count(*) from locMerge").fetchone()[0]
    print " - Loc =", VarX, " OkM =", VarY, " Total =", VarX+VarY, "  ", datetime.datetime.now()


def create_table_temp2(cursor):
    cursor.executescript("""
        CREATE TEMPORARY TABLE temp2 AS
            SELECT  CityA,
                    StateA,
                    CountryA,
                    count(*) AS cnt
              FROM  temp1
          GROUP BY  CityA,
                    StateA,
                    CountryA;

        CREATE INDEX IF NOT EXISTS t2_idx ON temp2 (CityA, StateA, CountryA);
          """)


def update_table_locmerge(cursor):
    cursor.executescript("""
        CREATE INDEX IF NOT EXISTS t1_idx ON temp1 (CityA, StateA, CountryA);

        INSERT OR REPLACE INTO locMerge
            SELECT  b.cnt,
                    a.jaro_match_value,
                    a.count,
                    a.cityA,
                    a.stateA,
                    a.countryA,
                    '',
                    a.ncity,
                    a.nstate,
                    a.ncountry,
                    '',
                    a.nlat,
                    a.nlong,
                    SUBSTR(a.cityA,1,3)

              FROM  temp1 AS a
        INNER JOIN  temp2 AS b
                ON  a.CityA = b.CityA
               AND  a.StateA = b.StateA
               AND  a.CountryA = b.CountryA;
          """)


def create_table_temp3(cursor):
    cursor.executescript("""
        CREATE TEMPORARY TABLE temp3 AS
            SELECT  a.*
              FROM  loc      AS a
         LEFT JOIN  locMerge AS b
                ON  a.City    = b.City
               AND  a.State   = b.State
               AND  a.Country = b.Country;
          """)


# TODO: Find a way to unit test this set of queries
def create_loc_and_locmerge_tables(cursor):

    create_table_temp2(cursor)
    update_table_locmerge(cursor)
    create_table_temp3(cursor)

    # cleanup, we'll leave that in here.
    cursor.executescript("""
        DROP TABLE IF EXISTS loc;

        CREATE TABLE loc AS SELECT * FROM temp3;

        CREATE INDEX IF NOT EXISTS loc_idxCC ON loc (City, Country);
        CREATE INDEX IF NOT EXISTS loc_idx   ON loc (City, State, Country);
        CREATE INDEX IF NOT EXISTS loc_idxCS ON loc (City, State);

        DROP TABLE IF EXISTS temp2;
        DROP TABLE IF EXISTS temp3;
          """)

def domestic_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  11,
               a.cnt as cnt,
               a.city as CityA,
               a.state as StateA,
               a.country as CountryA,
               b.city,
               b.state,
               'US',
               b.latitude,
               b.longitude
         FROM  loc AS a
   INNER JOIN  usloc AS b
           ON  GET_ENTRY_FROM_ROW(CityA, %d) = b.city
          AND  StateA = b.state
          AND  CountryA = 'US'
        WHERE  separator_count(CityA) >= %d
          AND  CityA != '' """
    return stmt;


def domestic_block_remove_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  11,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.city,
                b.state,
                'US',
                b.latitude,
                b.longitude
          FROM  loc AS a
    INNER JOIN  usloc AS b
            ON  remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)) = b.blkcity
           AND  a.state = b.state
           AND  a.country = 'US'
         WHERE  separator_count(a.City) >= %d
           AND  a.City != '' """
    return stmt;


def domestic_first3_jaro_winkler_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  (jarow(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),
                b.BlkCity)) AS Jaro,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.city,
                b.state,
                'US',
                b.latitude,
                b.longitude
          FROM  loc AS a
    INNER JOIN  usloc AS b
            ON  SUBSTR(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),1,3) = b.City3
           AND  a.state = b.state
           AND  a.country = 'US'
         WHERE  jaro > %s
           AND  separator_count(a.City) >= %d
           AND  a.City != ''
      ORDER BY  a.City, a.State, jaro"""
    return stmt;


def domestic_last4_jaro_winkler_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  (jarow(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),
                b.BlkCity)) AS Jaro,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.city,
                b.state,
                'US',
                b.latitude,
                b.longitude
          FROM  loc AS a
    INNER JOIN  usloc AS b
            ON  UPPER(SUBSTR(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),-4)) = b.City4R
           AND  a.state = b.state
           AND  a.country = 'US'
         WHERE  jaro > %s
           AND  separator_count(a.City) >= %d
           AND  a.City != ''
      ORDER BY  a.City, a.State, jaro"""
    return stmt;


# JR Code started taking longer to run at this statement
def foreign_full_name_1_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  21,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                "",
                b.cc1,
                b.lat,
                b.long
          FROM  loc AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  GET_ENTRY_FROM_ROW(a.City, %d) = b.full_name_ro
           AND  a.country = b.cc1
         WHERE  separator_count(a.City) >= %d
           AND  a.City!="" """
    return stmt;


def foreign_full_name_2_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  21,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                "",
                b.cc1,
                b.lat,
                b.long
          FROM  loc AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  GET_ENTRY_FROM_ROW(a.City, %d) = b.full_name_nd_ro
           AND  a.country = b.cc1
         WHERE  separator_count(a.City) >= %d
           AND  a.City != "" """
    return stmt;


def foreign_short_form_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  21,
                a.cnt     AS cnt,
                a.city    AS CityA,
                a.state   AS StateA,
                a.country AS CountryA,
                b.full_name_nd_ro,
                "",
                b.cc1,
                b.lat,
                b.long
          FROM  loc           AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  GET_ENTRY_FROM_ROW(a.City, %d) = b.short_form
           AND  a.country = b.cc1
         WHERE  separator_count(a.City) >= %d
           AND  a.City != "" """
    return stmt;


def foreign_block_split_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  21,
                a.cnt     AS cnt,
                a.city    AS CityA,
                a.state   AS StateA,
                a.country AS CountryA,
                b.full_name_nd_ro,
                "",
                b.cc1,
                b.lat,
                b.long
          FROM  loc AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)) = b.sort_name_ro
           AND  a.country = b.cc1
         WHERE  separator_count(a.City) >= %d
           AND  a.City != "" """
    return stmt;


def foreign_first3_jaro_winkler_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  (jarow(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),
                b.sort_name_ro)) AS Jaro,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                "",
                b.cc1,
                b.lat,
                b.long
          FROM  loc AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  SUBSTR(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),1,3) = b.sort_name_ro
           AND  a.country = b.cc1
         WHERE  jaro > %s
           AND  separator_count(a.City) >= %d
           AND  a.City != ""
      ORDER BY  a.City, a.Country, jaro"""
    return stmt;



def foreign_last4_jaro_winkler_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  (jarow(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),
                b.sort_name_ro)) AS Jaro,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                "",
                b.cc1,
                b.lat,
                b.long
          FROM  loc AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  UPPER(SUBSTR(remove_spaces(GET_ENTRY_FROM_ROW(a.City, %d)),-4)) = b.sort_name_ro
           AND  a.country = b.cc1
         WHERE  jaro > %s
           AND  separator_count(a.City) >= %d
           AND  a.City != ""
      ORDER BY  a.City, a.Country, jaro"""
    #""" % (sep, sep, "20.90", scnt))
    return stmt;


def domestic_2nd_layer_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  15,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.city,
                b.state,
                'US',
                b.latitude,
                b.longitude
          FROM  (SELECT  * FROM  loc WHERE  NCity IS NOT NULL) AS a
    INNER JOIN  usloc AS b
            ON  a.NCity = b.city
           AND  a.NState = b.state
           AND  a.NCountry = 'US'"""
    return stmt;


def domestic_first3_2nd_jaro_winkler_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  jarow(remove_spaces(a.NCity),
                b.BlkCity) AS Jaro,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.city,
                b.state,
                'US',
                b.latitude,
                b.longitude
          FROM  (SELECT  * FROM  loc WHERE  NCity IS NOT NULL) AS a
    INNER JOIN  usloc AS b
            ON  SUBSTR(remove_spaces(a.NCity),1,3) = b.City3
           AND  a.Nstate = b.state
           AND  a.Ncountry ='US'
         WHERE  jaro > %s
      ORDER BY  a.NCity, a.NState, jaro"""
    return stmt;


def foreign_full_name_2nd_layer_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  25,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                '' as state,
                b.cc1,
                b.lat,
                b.long
          FROM  (SELECT  * FROM  loc WHERE  NCity IS NOT NULL) AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  a.NCity = b.full_name_ro
           AND  a.NCountry = b.cc1"""
    return stmt;


def foreign_full_nd_2nd_layer_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  25,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                '' as state,
                b.cc1,
                b.lat,
                b.long
          FROM  (SELECT  * FROM  loc WHERE  NCity IS NOT NULL) AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  a.NCity = b.full_name_nd_ro
           AND  a.NCountry = b.cc1"""
    return stmt;


def foreign_no_space_2nd_layer_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  25,
                a.cnt as cnt,
                a.city as CityA,
                a.state as StateA,
                a.country as CountryA,
                b.full_name_nd_ro,
                '' as state,
                b.cc1,
                b.lat,
                b.long
          FROM  (SELECT  * FROM  loc WHERE  NCity IS NOT NULL) AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  remove_spaces(a.NCity) = b.sort_name_ro
           AND  a.NCountry = b.cc1"""
    return stmt;


def foreign_first3_2nd_jaro_winkler_sql():

    print sys._getframe().f_code.co_name

    stmt = """SELECT  jarow(remove_spaces(a.NCity),
                b.sort_name_ro) AS Jaro,
                a.cnt     AS cnt,
                a.city    AS CityA,
                a.state   AS StateA,
                a.country AS CountryA,
                b.full_name_nd_ro,
                ''        AS state,
                b.cc1,
                b.lat,
                b.long
          FROM  (SELECT  * FROM  loc WHERE  NCity IS NOT NULL) AS a
    INNER JOIN  loctbl.gnsloc AS b
            ON  SUBSTR(remove_spaces(a.NCity),1,3) = b.sort_name_ro
           AND  a.Ncountry = b.cc1
         WHERE  jaro > %s
      ORDER BY  a.NCity, a.NCountry, jaro"""
    return stmt;


# def domestic_zipcode_sql():
# 
#     print sys._getframe().f_code.co_name
# 
#     stmt = """SELECT  31,
#                 a.cnt     AS cnt,
#                 a.city    AS CityA,
#                 a.state   AS StateA,
#                 a.country AS CountryA,
#                 a.zipcode AS ZipcodeA,
#                 b.City,
#                 b.State,
#                 'US',
#                 b.zipcode,
#                 b.latitude,
#                 b.longitude
#           FROM  (SELECT  *,
#                          (GET_ENTRY_FROM_ROW(zipcode,0)+0) as Zip2
#                    FROM  loc
#                   WHERE  Zipcode != ''
#                     AND  Country = 'US') AS a
#     INNER JOIN  usloc AS b
#             ON  a.Zip2 = b.Zipcode"""
#     return stmt;


# TODO: Add this block to its own function, add a commented out call to
# to that function here.
####    ##DOMESTIC (State miscode to Country)
####    replace_loc("""
####        SELECT  31,
####                a.cnt, a.city, a.state, a.country, a.zipcode,
####                b.city, b.state, 'US', b.zipcode, b.lat, b.long
####          FROM  loc AS a INNER JOIN usloc AS b
####            ON  GET_ENTRY_FROM_ROW(a.City, %d)=b.city AND a.country=b.state
####         WHERE  separator_count(a.City)>=%d AND a.City!="";
####        """ % (sep, scnt))


##MISSING JARO (FIRST 3)
#replace_loc("""
#    SELECT  30+jarow(a.City, b.City) AS Jaro,
#            a.cnt, a.city, a.state, a.country, a.zipcode,
#            b.ncity, b.nstate, b.ncountry, b.nzipcode, b.nlat, b.nlong
#      FROM  loc AS a INNER JOIN locMerge AS b
#        ON  a.City3=b.City3 AND a.state=b.state AND a.country=b.country
#     WHERE  jaro>%s AND a.City!=""
#  ORDER BY  a.City, a.State, a.Country, jaro;
#    """ % ("30.95"))
