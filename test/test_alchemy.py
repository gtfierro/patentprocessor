import unittest
import sys
sys.path.append('../lib/')
from alchemy import *


class TestAscit(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_general(self):
        pass

    def test_merge(self):
        asg = session.query(RawAssignee).filter(RawAssignee.uuid == "4ad62754-f43f-11e2-ab4e-080027072fcb").one()
        uuids = [
            "4ad62754-f43f-11e2-ab4e-080027072fcb"
            "4dc99504-f43f-11e2-ab4e-080027072fcb"
            "4ec5d1e8-f43f-11e2-ab4e-080027072fcb"]
        match([
            session.query(RawAssignee).filter(RawAssignee.uuid == "4ad62754-f43f-11e2-ab4e-080027072fcb").one(),
            session.query(RawAssignee).filter(RawAssignee.uuid == "4dc99504-f43f-11e2-ab4e-080027072fcb").one(),
            session.query(RawAssignee).filter(RawAssignee.uuid == "4ec5d1e8-f43f-11e2-ab4e-080027072fcb").one()])

        print asg.assignee
        print asg.assignee.patents
        print asg.assignee.rawassignees


if __name__ == '__main__':
    unittest.main()
