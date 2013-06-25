#!/usr/bin/env python
import sqlite3
import alchemy

"""
The plan is to build up a queue of commands to be executed
on each sql table. This queue should be able to be constructed
concurrently among many processes. After all patents have been
added to the queue, we can just hit "commit" on each db 
connection.
"""

class SQLTableBuilder(object):
    def __init__(self):
        raise NotImplementedError("Create database file")
    def commit(self):
        raise NotImplementedError("Create commit method")

class AssigneeSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("assignee.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS assignee (
                Patent VARCHAR(8),      AsgType INTEGER,        Assignee VARCHAR(30),
                City VARCHAR(10),       State VARCHAR(2),       Country VARCHAR(2),
                Nationality VARCHAR(2), Residence VARCHAR(2),   AsgSeq INTEGER);
            CREATE UNIQUE INDEX IF NOT EXISTS uqAsg ON assignee (Patent, AsgSeq);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("assignee.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO assignee VALUES \
            (?, ?, ?, ?, ?, ?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class CitationSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("citation.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS citation (
                Patent VARCHAR(8),      Cit_Date INTEGER,       Cit_Name VARCHAR(10),
                Cit_Kind VARCHAR(1),    Cit_Country VARCHAR(2), Citation VARCHAR(8),
                Category VARCHAR(15),   CitSeq INTEGER);
            CREATE UNIQUE INDEX IF NOT EXISTS uqCit ON citation (Patent, CitSeq);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("citation.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO citation VALUES \
            (?, ?, ?, ?, ?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class ClassSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("class.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS class (
                Patent VARCHAR(8),      Prim INTEGER,
                Class VARCHAR(3),       SubClass VARCHAR(3));
            CREATE UNIQUE INDEX IF NOT EXISTS uqClass ON class (Patent, Class, SubClass);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("class.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO class VALUES \
            (?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class InventorSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("inventor.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS inventor (
                Patent VARCHAR(8),      Firstname VARCHAR(15),  Lastname VARCHAR(15),
                Street VARCHAR(15),     City VARCHAR(10),
                State VARCHAR(2),       Country VARCHAR(12),
                Zipcode VARCHAR(5),     Nationality VARCHAR(2), InvSeq INTEGER);
            CREATE UNIQUE INDEX IF NOT EXISTS uqInv ON inventor (Patent, InvSeq);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("inventor.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO inventor VALUES \
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class PatentSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("patent.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS patent (
                Patent VARCHAR(8),      Kind VARCHAR(3),        Claims INTEGER,
                AppType INTEGER,        AppNum VARCHAR(8),
                GDate INTEGER,          GYear INTEGER,
                AppDate INTEGER,        AppYear INTEGER, PatType VARCHAR(15) );
            CREATE UNIQUE INDEX IF NOT EXISTS uqPat on patent (Patent);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("patent.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO patent VALUES \
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class PatdescSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("patdesc.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS patdesc (
                Patent VARCHAR(8),
                Abstract VARCHAR(50),   Title VARCHAR(20));
            CREATE UNIQUE INDEX IF NOT EXISTS uqPatDesc ON patdesc (Patent);
            """)
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("patdesc.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO patdesc VALUES \
            (?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class LawyerSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("lawyer.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS lawyer (
                Patent VARCHAR(8),      Firstname VARCHAR(15),  Lastname VARCHAR(15),
                LawCountry VARCHAR(2),  OrgName VARCHAR(20),    LawSeq INTEGER);
            CREATE UNIQUE INDEX IF NOT EXISTS uqLawyer ON lawyer (Patent, LawSeq);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("lawyer.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO lawyer VALUES \
            (?, ?, ?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class ScirefSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("sciref.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS sciref (
                Patent VARCHAR(8),      Descrip VARCHAR(20),    CitSeq INTEGER);
            CREATE UNIQUE INDEX IF NOT EXISTS uqSciref ON sciref (Patent, CitSeq);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("sciref.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO sciref VALUES \
            (?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

class UsreldocSQL(SQLTableBuilder):
    def __init__(self):
        self.conn = sqlite3.connect("usreldoc.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executescript("""
            CREATE TABLE IF NOT EXISTS usreldoc (
                Patent VARCHAR(8),      DocType VARCHAR(10),    OrderSeq INTEGER,
                Country VARCHAR(2),     RelPatent VARCHAR(8),   Kind VARCHAR(2),
                RelDate INTEGER,        Status VARCHAR(10));
            CREATE UNIQUE INDEX IF NOT EXISTS uqUSRelDoc ON usreldoc (Patent, OrderSeq);
            """)
        self.conn.close()
        self.inserts = []

    def commit(self, inserts):
        inserts = [[unicode(x) for x in inner] for inner in inserts]
        self.conn = sqlite3.connect("usreldoc.sqlite3")
        self.cursor = self.conn.cursor()
        self.cursor.executemany("""INSERT OR IGNORE INTO usreldoc VALUES \
            (?, ?, ?, ?, ?, ?, ?, ?)""", inserts)
        self.conn.commit()
        self.conn.close()

assignee_table = AssigneeSQL()
citation_table = CitationSQL()
class_table = ClassSQL()
inventor_table = InventorSQL()
patent_table = PatentSQL()
patdesc_table = PatdescSQL()
lawyer_table = LawyerSQL()
sciref_table = ScirefSQL()
usreldoc_table = UsreldocSQL()

class XMLPatentBase(object):

    def __init__(self, patentgrant):
        self.patent = patentgrant

class AssigneeXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate(self.patent.asg_list):
            if not y[0]:
                ack.extend([[self.patent.patent, y[2], y[1], y[4], y[5], y[6], y[7], y[8], i]])
            else:
                ack.extend([[self.patent.patent, "00", "%s, %s" % (y[2], y[1]), y[4], y[5], y[6], y[7], y[8], i]])
        return ack

    def insert_table(self):
        assignee_table.inserts.extend(self.build_table())

class CitationXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate([x for x in self.patent.cit_list if x[1] != ""]):
            ack.extend([[self.patent.patent, y[3], y[5], y[4], y[1], y[2], y[0], i]])
        return ack

    def insert_table(self):
        citation_table.inserts.extend(self.build_table())

class ClassXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate(self.patent.classes):
            ack.extend([[self.patent.patent, (i==0)*1, y[0], y[1]]])
        return ack

    def insert_table(self):
        class_table.inserts.extend(self.build_table())

class InventorXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate(self.patent.inv_list):
            ack.extend([[self.patent.patent, y[1], y[0], y[2], y[3], y[4], y[5], y[6], y[8], i]])
        return ack

    def insert_table(self):
        inventor_table.inserts.extend(self.build_table())

class PatentXML(XMLPatentBase):
    def build_table(self):
        return [[self.patent.patent, self.patent.kind, self.patent.clm_num, self.patent.code_app, self.patent.patent_app, self.patent.date_grant, self.patent.date_grant[:4], self.patent.date_app, self.patent.date_app[:4], self.patent.pat_type]]

    def insert_table(self):
        patent_table.inserts.extend(self.build_table())


class PatdescXML(XMLPatentBase):
    def build_table(self):
        return [[self.patent.patent, self.patent.abstract, self.patent.invention_title]]

    def insert_table(self):
        patdesc_table.inserts.extend(self.build_table())


class LawyerXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate(self.patent.law_list):
            ack.extend([[self.patent.patent, y[1], y[0], y[2], y[3], i]])
        return ack

    def insert_table(self):
        lawyer_table.inserts.extend(self.build_table())


class ScirefXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate([y for y in self.patent.cit_list if y[1] == ""]):
            ack.extend([[self.patent.patent, y[-1], i]])
        return ack

    def insert_table(self):
        sciref_table.inserts.extend(self.build_table())


class UsreldocXML(XMLPatentBase):
    def build_table(self):
        ack = []
        for i,y in enumerate(self.patent.rel_list):
            if y[1] == 1:
                ack.extend([[self.patent.patent, y[0], y[1], y[3], y[2], y[4], y[5], y[6]]])
            else:
                ack.extend([[self.patent.patent, y[0], y[1], y[3], y[2], y[4], "", ""]])
        return ack

    def insert_table(self):
        usreldoc_table.inserts.extend(self.build_table())
