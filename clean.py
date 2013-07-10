import datetime

import lib.SQLite as SQLite


# ascit was refactored from senAdd in favor of ascit in fwork.
# They differ by 1 line. Leave this comment until covered
# by unit test.
import lib.senAdd as senAdd
import lib.fwork as fwork
import lib.locFunc as locFunc
import lib.orgClean as orgClean
from lib.handlers.xml_util import normalize_document_identifier

debug = False
#debug = True

t1 = datetime.datetime.now()
#print "Start", t1

# TODO: Move the location handling code into a different script,
# or call it from the driver file.
##### Run B2_LocationMatch.py
#import B2_LocationMatch
 # Geocode needs to run by itself.
print "START: geocode", t1
import lib.geocode
#print "   - Loc Merge", "\n   -", datetime.datetime.now()-t1
print"DONE: geocode"
print "   -", datetime.datetime.now()-t1


# TODO: Refactor assignee statements
### Create copy of assignee table, add column for assigneeAsc
s = SQLite.SQLite(db = 'assignee.sqlite3', tbl = 'assignee_1')
s.conn.create_function("ascit", 1, fwork.ascit)
s.conn.create_function("clean_assignee", 1, fwork.clean_assignee)
s.conn.create_function("cc", 3, locFunc.cityctry)

def normalize_doc_numbers():
    citation_table = SQLite.SQLite('citation.sqlite3')
    citation_table.conn.create_function('normalize_document_identifier', 1, normalize_document_identifier)
    citation_table.attach('citation.sqlite3')
    citation_table.conn.execute('update citation set Citation=normalize_document_identifier(Citation);')
    citation_table.commit()
    citation_table.close()

normalize_doc_numbers()

#TODO: read up on nber matchdoc.pdf
def handle_assignee():

    #s.attach(database='NBER_asg',name='NBER')
    s.attach(db='NBER_asg',name='NBER')

    s.c.execute("DROP TABLE IF EXISTS assignee_1")
    s.replicate(tableTo = 'assignee_1', table = 'assignee')
    #s.addSQL(data='assignee', insert="IGNORE")
    s.c.execute("INSERT INTO assignee_1 SELECT * FROM assignee %s" % (debug and "LIMIT 2500" or ""))
    s.add('assigneeAsc', 'VARCHAR(30)')
    s.c.execute("UPDATE assignee_1 SET assigneeAsc = clean_assignee(assignee);")
    s.commit()
    #print "DONE: assignee_1 table created in assignee.sqlite3 with new column assigneeAsc", "\n   -", datetime.datetime.now()-t1

    #s.merge(key=[['AsgNum', 'pdpass']], on=[['assigneeAsc', 'assignee']], keyType=['INTEGER'], tableFrom='main', db='db')
    #s.attach(database = 'NBER_asg')
    #print "Tables call from script ", s.tables()

    s.merge(key=[['AsgNum', 'pdpass']], on=[['assigneeAsc', 'assignee']],
            keyType=['INTEGER'], tableFrom='assignee', db='NBER')
    #s.merge(key=[['AsgNum', 'pdpass']], on=['assigneeAsc', 'assignee'], keyType=['INTEGER'], tableFrom='assignee', db='NBER')

    s.c.execute("UPDATE assignee_1 SET AsgNum=NULL WHERE AsgNum<0")
    print"DONE: NBER pdpass added to assignee_1 in column AsgNum", "\n   -", datetime.datetime.now()-t1
    s.commit()

handle_assignee()

# TODO: Refactor to function
### Run orgClean.py and generate grp
# TODO: get rid of in refactor
def run_org_clean():
    org = orgClean.orgClean(db = 'assignee.sqlite3', fld = 'assigneeAsc', table = 'assignee_1', other = "")
    org.disambig()
    print"DONE: orgClean"
    #print "   -", datetime.datetime.now()-t1

    # Copy assignee num from grp to assignee table
    s.merge(key=[['AsgNum', 'AsgNum2']], on=['AssigneeAsc'], tableFrom='grp')
    print "DONE: Replaced Asgnum!", "\n   -", datetime.datetime.now()-t1
    s.c.execute("""update assignee_1 set City = cc(city, country, 'city'), Country = cc(city, country, 'ctry');""")
    s.attach('hashTbl.sqlite3')
    s.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'],
            on=['City', 'State', 'Country'],
            tableFrom='locMerge', db='db')
    s.commit()
    print "DONE: Asg Locationize!", "\n   -", datetime.datetime.now()-t1


run_org_clean()
s.close()


 ###########################
###                       ###
##     I N V E N T O R     ##
###                       ###
 ###########################

# adds geocoding to inventors

def handle_inventor():

    ## Clean inventor: ascit(Firstname, Lastname, Street)
    ## Create new table inventor_1 to hold prepped data

    i = SQLite.SQLite(db = 'inventor.sqlite3', tbl = 'inventor_1')
    i.conn.create_function("ascit", 1, fwork.ascit)
    i.conn.create_function("cc",    3, locFunc.cityctry)
    i.c.execute('drop table if exists inventor_1')
    i.replicate(tableTo = 'inventor_1', table = 'inventor')
    i.c.execute('insert or ignore into inventor_1 select * from inventor  %s' % (debug and "LIMIT 2500" or ""))

    i.c.execute("""
            UPDATE  inventor_1
               SET  firstname = ascit(firstname),
                    lastname  = ascit(lastname),
                    street    = ascit(street),
                    City      = cc(city, country, 'city'),
                    Country   = cc(city, country, 'ctry');
                """)

    i.commit()

    i.attach('hashTbl.sqlite3')
    i.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'],
            on=['City', 'State', 'Country'],
            tableFrom='locMerge',
            db='db')

    i.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'],
            on=['City', 'State', 'Country', 'Zipcode'],
            tableFrom='locMerge',
            db='db')

    i.commit()
    i.close()
    print "DONE: Inv Locationize!", "\n   -", datetime.datetime.now()-t1

handle_inventor()


 ###########################
###                       ###
##        C L A S S        ##
###                       ###
 ###########################

# Clean up classes
# see CleanDataSet.py --> classes()
# FIXME: Module importing not allowed in function.
# TODO: get rid of in refactor
import lib.CleanDataset as CleanDataset
CleanDataset.classes()
print "DONE: Classes!", "\n   -", datetime.datetime.now()-t1



 ###########################
###                       ###
##       P A T E N T       ##
###                       ###
 ###########################

# normalizes the application date and grant date
def handle_patent():
    p = SQLite.SQLite(db = 'patent.sqlite3', tbl = 'patent')
    p.conn.create_function('dVert', 1, senAdd.dateVert)
    p.c.execute("""update patent set AppDate=dVert(AppDate), GDate=dVert(GDate);""")
    p.commit()
    p.close()
    print "DONE: Patent Date!", "\n   -", datetime.datetime.now()-t1

handle_patent()
