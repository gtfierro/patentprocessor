import unittest
import os
import sys
sys.path.append('../lib/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import alchemy
from alchemy.schema import *


class TestAlchemy(unittest.TestCase):

    def setUp(self):
        # this basically resets our testing database
        os.system("cp {0}/alchemy.raw {0}/test.db".format(config.get("sqlite").get("path")))
        session = alchemy.fetch_session()
        pass

    def tearDown(self):
        # we keep this to tidy up our database if it fails
        session.close()
        pass

    def test_raw_clean(self):
        # add a Clean record to mark something against
        asg0 = session.query(RawAssignee).limit(10).all()
        asg1 = session.query(RawAssignee).limit(10).offset(10).all()

        alchemy.match(asg0)
        alchemy.match(asg1)
        alchemy.match([asg0[0], asg1[0].assignee])

    def test_set_default(self):
        # create two items
        loc = session.query(RawLocation).all()
        alchemy.match(loc)

        alchemy.match(loc[0], {"city": u"Frisco", "state": u"Cali", "country": u"US", "longitude": 10.0, "latitude": 10.0})
        self.assertEqual("Frisco, Cali, US", loc[0].location.address)

        alchemy.match(loc[0], keepdefault=True)
        self.assertEqual("Frisco, Cali, US", loc[0].location.address)
        self.assertEqual(10.0, loc[0].location.latitude)
        self.assertEqual(10.0, loc[0].location.longitude)

        alchemy.match(loc[0])
        self.assertEqual("Hong Kong, OH, US", loc[0].location.address)
        self.assertEqual(10.0, loc[0].location.latitude)
        self.assertEqual(10.0, loc[0].location.longitude)

        alchemy.match(loc[0], {"city": u"Frisco"}, keepdefault=True)
        self.assertEqual("Frisco, OH, US", loc[0].location.address)
        self.assertEqual(10.0, loc[0].location.latitude)
        self.assertEqual(10.0, loc[0].location.longitude)

    def test_assigneematch(self):
        # blindly assume first 10 are the same
        asg0 = session.query(RawAssignee).limit(10).all()
        asg1 = session.query(RawAssignee).limit(10).offset(10).all()
        asgs = session.query(Assignee)

        alchemy.match(asg0)
        alchemy.match(asg1)

        # create two items
        self.assertEqual(10, len(asg0[0].assignee.rawassignees))
        self.assertEqual(10, len(asg1[0].assignee.rawassignees))
        self.assertEqual(10, len(asg0[0].assignee.patents))
        self.assertEqual(2, asgs.count())
        self.assertEqual("CAFEPRESS.COM", asg0[0].assignee.organization)

        # merge the assignees together
        alchemy.match([asg0[0], asg1[0]])
        self.assertEqual(20, len(asg0[0].assignee.rawassignees))
        self.assertEqual(20, len(asg1[0].assignee.rawassignees))
        self.assertEqual(20, len(asg0[0].assignee.patents))
        self.assertEqual(1, asgs.count())

        # override the default values provided
        alchemy.match(asg0[0], {"organization": u"Kevin"})
        self.assertEqual("Kevin", asg0[0].assignee.organization)

        # determine the most common organization name
        alchemy.match(session.query(RawAssignee).limit(40).all())
        self.assertEqual(40, len(asg1[0].assignee.rawassignees))
        self.assertEqual("The Procter & Gamble Company", asg0[0].assignee.organization)

    def test_inventormatch(self):
        # blindly assume first 10 are the same
        inv0 = session.query(RawInventor).limit(10).all()
        inv1 = session.query(RawInventor).limit(10).offset(10).all()
        invs = session.query(Inventor)

        alchemy.match(inv0)
        alchemy.match(inv1)

        # create two items
        self.assertEqual(10, len(inv0[0].inventor.rawinventors))
        self.assertEqual(10, len(inv1[0].inventor.rawinventors))
        self.assertEqual(2, invs.count())
        self.assertEqual(6, len(inv0[0].inventor.patents))
        self.assertEqual(5, len(inv1[0].inventor.patents))
        self.assertEqual("David C. Mattison", inv0[0].inventor.name_full)

        # merge the assignees together
        alchemy.match([inv0[0], inv1[0]])
        self.assertEqual(20, len(inv0[0].inventor.rawinventors))
        self.assertEqual(20, len(inv1[0].inventor.rawinventors))
        self.assertEqual(11, len(inv0[0].inventor.patents))
        self.assertEqual(1, invs.count())

        # override the default values provided
        alchemy.match(inv0[0], {"name_first": u"Kevin", "name_last": u"Yu"})
        self.assertEqual("Kevin Yu", inv0[0].inventor.name_full)

        # determine the most common organization name
        alchemy.match(session.query(RawInventor).all())
        self.assertEqual(137, len(inv1[0].inventor.rawinventors))
        self.assertEqual("Robert Wang", inv0[0].inventor.name_full)

    def test_lawyermatch(self):
        # blindly assume first 10 are the same
        law0 = session.query(RawLawyer).limit(10).all()
        law1 = session.query(RawLawyer).limit(10).offset(10).all()
        laws = session.query(Lawyer)

        alchemy.match(law0)
        alchemy.match(law1)

        # create two items
        self.assertEqual(10, len(law0[0].lawyer.rawlawyers))
        self.assertEqual(10, len(law1[0].lawyer.rawlawyers))
        self.assertEqual(2, laws.count())
        self.assertEqual(7, len(law0[0].lawyer.patents))
        self.assertEqual(9, len(law1[0].lawyer.patents))
        self.assertEqual("Warner Norcross & Judd LLP", law0[0].lawyer.organization)

        # merge the assignees together
        alchemy.match([law0[0], law1[0]])
        self.assertEqual(20, len(law0[0].lawyer.rawlawyers))
        self.assertEqual(20, len(law1[0].lawyer.rawlawyers))
        self.assertEqual(15, len(law0[0].lawyer.patents))

        self.assertEqual(1, laws.count())

        # override the default values provided
        alchemy.match(law0[0], {"name_first": u"Devin", "name_last": u"Ko"})
        self.assertEqual("Devin Ko", law0[0].lawyer.name_full)

        # determine the most common organization name
        alchemy.match(session.query(RawLawyer).all())

        self.assertEqual(57, len(law1[0].lawyer.rawlawyers))
        self.assertEqual("Robert Robert Chuey", law0[0].lawyer.name_full)

    def test_locationmatch(self):
        # blindly assume first 10 are the same
        loc0 = session.query(RawLocation).limit(10).all()
        loc1 = session.query(RawLocation).limit(10).offset(10).all()
        locs = session.query(Location)

        alchemy.match(loc0)
        alchemy.match(loc1)

        # create two items
        self.assertEqual(10, len(loc0[0].location.rawlocations))
        self.assertEqual(10, len(loc1[0].location.rawlocations))
        self.assertEqual(0, len(loc0[0].location.assignees))
        self.assertEqual(0, len(loc0[0].location.inventors))
        self.assertEqual(2, locs.count())
        self.assertEqual("Hong Kong, MN, NL", loc0[0].location.address)

        # merge the assignees together
        alchemy.match([loc0[0], loc1[0]])
        self.assertEqual(20, len(loc0[0].location.rawlocations))
        self.assertEqual(20, len(loc1[0].location.rawlocations))
        self.assertEqual(0, len(loc0[0].location.assignees))
        self.assertEqual(0, len(loc0[0].location.inventors))
        self.assertEqual(1, locs.count())
        self.assertEqual("Hong Kong, MN, US", loc0[0].location.address)
        self.assertEqual(None, loc0[0].location.latitude)
        self.assertEqual(None, loc0[0].location.longitude)

        # override the default values provided
        alchemy.match(loc0[0], {"city": u"Frisco", "state": u"Cali", "country": u"US", "longitude": 10.0, "latitude": 10.0})
        self.assertEqual("Frisco, Cali, US", loc0[0].location.address)
        self.assertEqual(10.0, loc0[0].location.latitude)
        self.assertEqual(10.0, loc0[0].location.longitude)

    def test_assignee_location(self):
        # insert an assignee first.
        # then location. make sure links ok
        asg = session.query(RawAssignee).limit(20).all()
        loc = session.query(RawLocation).limit(40).all()

        alchemy.match(asg[0:5])
        alchemy.match(asg[5:10])
        alchemy.match(asg[10:15])
        alchemy.match(asg[15:20])
        alchemy.match(loc[0:20])
        alchemy.match(loc[20:40])

        self.assertEqual(2, len(loc[19].location.assignees))
        self.assertEqual(1, len(asg[4].assignee.locations))
        self.assertEqual(2, len(asg[5].assignee.locations))

    def test_inventor_location(self):
        # insert an assignee first.
        # then location. make sure links ok
        inv = session.query(RawInventor).limit(20).all()
        loc = session.query(RawLocation).limit(40).all()

        alchemy.match(inv[0:5])
        alchemy.match(inv[5:10])
        alchemy.match(inv[10:15])
        alchemy.match(inv[15:20])
        alchemy.match(loc[0:20])
        alchemy.match(loc[20:40])

        self.assertEqual(1, len(inv[14].inventor.locations))
        self.assertEqual(2, len(inv[15].inventor.locations))
        self.assertEqual(4, len(loc[19].location.inventors))
        self.assertEqual(1, len(loc[20].location.inventors))

    def test_location_assignee(self):
        asg = session.query(RawAssignee).limit(20).all()
        loc = session.query(RawLocation).limit(40).all()

        alchemy.match(loc[0:20])
        alchemy.match(loc[20:40])
        alchemy.match(asg[0:5])
        alchemy.match(asg[5:10])
        alchemy.match(asg[10:15])
        alchemy.match(asg[15:20])

        self.assertEqual(2, len(loc[19].location.assignees))
        self.assertEqual(1, len(asg[4].assignee.locations))
        self.assertEqual(2, len(asg[5].assignee.locations))

    def test_location_inventor(self):
        # insert an assignee first.
        # then location. make sure links ok
        inv = session.query(RawInventor).limit(20).all()
        loc = session.query(RawLocation).limit(40).all()

        alchemy.match(loc[0:20])
        alchemy.match(loc[20:40])
        alchemy.match(inv[0:5])
        alchemy.match(inv[5:10])
        alchemy.match(inv[10:15])
        alchemy.match(inv[15:20])

        self.assertEqual(1, len(inv[14].inventor.locations))
        self.assertEqual(2, len(inv[15].inventor.locations))
        self.assertEqual(4, len(loc[19].location.inventors))
        self.assertEqual(1, len(loc[20].location.inventors))

if __name__ == '__main__':
    config = alchemy.get_config()
    session = alchemy.session
    unittest.main()
