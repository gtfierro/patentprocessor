# sets up the geocoding databases

import re
import fwork

FIRST3_JARO_REQUIRED="0.92"
LAST4_JARO_REQUIRED="10.90"

def get_first3_jaro_required():
    return FIRST3_JARO_REQUIRED

def get_last4_jaro_required():
    return LAST4_JARO_REQUIRED

#return the ith entry in a row separated by ',' or '|'. Return an empty string if there are fewer than i segments in the row.
def get_entry_from_row(row, i):
    if i==-1:
        return row
    else:
        #Match the , and | separators and any spaces on either side
        pattern = re.compile(" *?[,|] *")
        row_split = pattern.split(row)
        if len(row_split)> i:
            return row_split[i]
        else:
            return ""

# TODO: Consider replacing the lambdas with functions which can be tested.
def create_sql_helper_functions(conn):
    conn.create_function("remove_spaces", 1, lambda x: re.sub(" ", "", x))
    conn.create_function("separator_count",   1, lambda x: len(re.findall("[,|]", x)))
    conn.create_function("jarow",     2, fwork.jarow)
    conn.create_function("cityctry",  3, fwork.cityctry)
    conn.create_function("get_entry_from_row", 2, get_entry_from_row)
    #conn.create_function("rev_wrd",   2, lambda x,y:x.upper()[::-1][:y])


def geocode_db_initialize(cursor):
    cursor.executescript("""
        PRAGMA CACHE_SIZE=20000;
        ATTACH DATABASE 'loctbl'   AS loctbl;
        """)


# TODO: Ensure this is the correct schema, that is, the
# schema below needs to match the schema in the existing
# loc table. If it doesn't match, we need to find out
# why, and figure out what to do about that.
def loc_create_table(cursor):
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS loc (
            Cnt      INTEGER,
            City     VARCHAR(10),
            State    VARCHAR(2),
            Country  VARCHAR(2),
            Zipcode  VARCHAR(5),
            City3    VARCHAR,
            NCity    VARCHAR(10),
            NState   VARCHAR(2),
            NCountry VARCHAR(2),
            UNIQUE(City,State,Country,Zipcode));

        DROP INDEX IF EXISTS loc_idxCC;
        DROP INDEX IF EXISTS loc_idx;
        DROP INDEX IF EXISTS loc_idxCS;
        DROP INDEX IF EXISTS loc_ixnCC;
        DROP INDEX IF EXISTS loc_ixn;
        DROP INDEX IF EXISTS loc_ixnCS;
        DROP INDEX IF EXISTS loc3_idxCC;
        """)


def create_hashtbl(cursor, connection):
    geocode_db_initialize(cursor)
    loc_create_table(cursor)
    if not(fwork.tblExist(cursor, "locMerge")):
        fix_city_country(cursor)
        fix_state_zip(cursor)
        create_loc_indexes(connection)
    create_usloc_table(cursor)
    create_locMerge_table(cursor)


# This feels a little overboard, but this DRYs up
# code in a couple of methods where the line count
# is already too long.
def drop_temp_tables(cursor):
    cursor.executescript("""
        DROP TABLE temp;
        DROP TABLE temp2;
        """)


def update_table_loc_from_typos(cursor):
    cursor.executescript("""
        INSERT OR REPLACE INTO loc
            SELECT  a.*,
                    SUBSTR(CityY,1,3),
                    b.NewCity,
                    b.NewState,
                    b.NewCountry
              FROM  temp2 AS a
         LEFT JOIN  loctbl.typos AS b
                ON  a.CityY  = b.City
               AND  a.StateY = b.State
               AND  a.CtryY  = b.Country;
        """)


def create_table_temp_assignees(cursor):
    cursor.executescript("""
       CREATE TEMPORARY TABLE temp AS
            SELECT  UPPER(City)    AS CityX,
                    UPPER(State)   AS StateX,
                    UPPER(Country) AS CountryX,
                    count(*)       AS Cnt
              FROM  assigneesdb.assignee
             WHERE  City != ""
          GROUP BY  CityX, StateX, CountryX;
        """)


def create_table_temp_inventors(cursor):
    cursor.executescript("""
       CREATE TEMPORARY TABLE temp AS
            SELECT  UPPER(City)    AS CityX,
                    UPPER(State)   AS StateX,
                    UPPER(Country) AS CountryX,
                    Zipcode,
                    count(*)       AS Cnt
              FROM  inventorsdb.inventor
             WHERE  City != ""
                OR (City = "" AND Zipcode != "")
          GROUP BY  CityX, StateX, CountryX, Zipcode;
        """)


def create_table_temp_assignees_db(cursor):
    cursor.execute("ATTACH DATABASE 'assignee.sqlite3' AS assigneesdb;")
    create_table_temp_assignees(cursor)
    cursor.execute("DETACH DATABASE assigneesdb;")


def create_table_temp_inventors_db(cursor):
    cursor.execute("ATTACH DATABASE 'inventor.sqlite3' AS inventorsdb;")
    create_table_temp_inventors(cursor)
    cursor.execute("DETACH DATABASE inventorsdb;")


def create_table_temp2_assignees(cursor):
    cursor.executescript("""
        CREATE TEMPORARY TABLE temp2 AS
            SELECT  sum(Cnt)                          AS Cnt,
                    cityctry(CityX, CountryX, 'city') AS CityY,
                    StateX                            AS StateY,
                    cityctry(CityX, CountryX, 'ctry') AS CtryY,
                    ''                                AS ZipcodeY
              FROM  temp
             WHERE  CityY != ""
          GROUP BY  CityY, StateY, CtryY;
        """)


def create_table_temp2_inventors(cursor):
    cursor.executescript("""
        CREATE TEMPORARY TABLE temp2 AS
            SELECT  sum(Cnt)                          AS Cnt,
                    cityctry(CityX, CountryX, 'city') AS CityY,
                    StateX                            AS StateY,
                    cityctry(CityX, CountryX, 'ctry') AS CtryY,
                    Zipcode                           AS ZipcodeY
              FROM  temp
             WHERE  CityY != ""
          GROUP BY  CityY, StateY, CtryY, ZipcodeY;
        """)


# TODO: Find a way to unit test fix_city_country
def fix_city_country(cursor):
    create_table_temp_assignees_db(cursor)
    create_table_temp2_assignees(cursor)
    update_table_loc_from_typos(cursor)
    drop_temp_tables(cursor)


# TODO: Find a way to unit test fix_state_zip
def fix_state_zip(cursor):
    create_table_temp_inventors_db(cursor)
    create_table_temp2_inventors(cursor)
    update_table_loc_from_typos(cursor)
    drop_temp_tables(cursor)


# TODO: Find a way to ensure that the correct indexes are created as
# the schemas change.
def create_loc_indexes(cursor):
    cursor.executescript("""
        CREATE INDEX IF NOT EXISTS loc_idCC3 ON loc (City3,State,Country);
        CREATE INDEX IF NOT EXISTS loc_idxCC ON loc (City,Country);
        CREATE INDEX IF NOT EXISTS loc_idx   ON loc (City,State,Country,Zipcode);
        CREATE INDEX IF NOT EXISTS loc_idxCS ON loc (City,State);
        CREATE INDEX IF NOT EXISTS loc_ixnCC ON loc (NCity,NCountry);
        CREATE INDEX IF NOT EXISTS loc_ixn   ON loc (NCity,NState,NCountry);
        CREATE INDEX IF NOT EXISTS loc_ixnCS ON loc (NCity,NState);
        """)


# TODO: unit test
def create_usloc_table(cursor):
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS usloc AS
            SELECT  0 as Zipcode,
                    Latitude,
                    Longitude,
                    UPPER(City)                        AS City,
                    remove_spaces(Upper(City))             AS BlkCity,
                    SUBSTR(UPPER(remove_spaces(City)),1,3) AS City3,
                    SUBSTR(remove_spaces(City), -4)        AS City4R,
                    UPPER(State)                       AS State,
                    "US"                               AS Country
              FROM  loctbl.us_cities;

        CREATE INDEX If NOT EXISTS usloc_idxZ  on usloc (Zipcode);
        CREATE INDEX If NOT EXISTS usloc_idxCS on usloc (City, State);
        CREATE INDEX If NOT EXISTS usloc_idBCS on usloc (BlkCity, State);
        CREATE INDEX If NOT EXISTS usloc_idC3S on usloc (City3, State);
        CREATE INDEX If NOT EXISTS usloc_idC4R on usloc (City4R, State);


        /*
        DETACH DATABASE loc;
        CREATE TEMPORARY TABLE gnsloc AS
            SELECT  '' AS zipcode, lat, long,
                    UPPER(full_name_nd) AS city, "" AS State, cc1 AS country
              FROM  loctbl.gnsloc;
        CREATE INDEX gnsloc_idxCC on gnsloc (City, Country)
        */;
        """)


def create_locMerge_table(cursor):
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS locMerge (
            Mtch     INTEGER,
            Val      FLOAT,
            Cnt      INTEGER,
            City     VARCHAR,
            State    VARCHAR,
            Country  VARCHAR,
            Zipcode  VARCHAR,
            NCity    VARCHAR,
            NState   VARCHAR,
            NCountry VARCHAR,
            NZipcode VARCHAR,
            NLat     FLOAT,
            NLong    FLOAT,
            City3    VARCHAR,
            UNIQUE(City, State, Country, Zipcode));

        CREATE INDEX IF NOT EXISTS okM_idxCC ON locMerge (City,Country);
        CREATE INDEX IF NOT EXISTS okM_idx   ON locMerge (City,State,Country,Zipcode);
        CREATE INDEX IF NOT EXISTS okM_idxCS ON locMerge (City,State);
        CREATE INDEX IF NOT EXISTS okM_idx3  ON locMerge (City3,State,Country);
        """)
