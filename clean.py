#!/usr/bin/env Python
from lib import alchemy
from lib import assignee_disambiguation
import datetime

t1 = datetime.datetime.now()

# run geocoding
print "START: geocode", t1
import lib.geocode
#print "   - Loc Merge", "\n   -", datetime.datetime.now()-t1
print"DONE: geocode"
print "   -", datetime.datetime.now()-t1

# run assignee disambiguation and populate the Assignee table
assignee_disambiguation.run_disambiguation()

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
    print 'inventor_1 created'

    i.attach('hashTbl.sqlite3')
    i.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'],
            on=['City', 'State', 'Country'],
            tableFrom='locMerge',
            db='db')

#     i.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'],
#             on=['City', 'State', 'Country', 'Zipcode'],
#             tableFrom='locMerge',
#             db='db')

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
