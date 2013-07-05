#!/usr/bin/env python


import unittest
import sys

sys.path.append( '.' )
sys.path.append( '../lib/' )

from geocode_setup import separate_row

class TestSepWrd(unittest.TestCase):

    def test_separate_row_comma(self):
        assert("foo" == separate_row("foo,bar", 0))
        assert("bar" == separate_row("foo,bar", 1))

    def test_separate_row_pipe(self):
        assert("foo" == separate_row("foo|bar", 0))
        assert("bar" == separate_row("foo|bar", 1))

    def test_nosplit(self):
        result = separate_row("foo bar", 0)
        assert("foo bar" == result)
        result = separate_row("foo bar", 1)
        assert("" == result)
        # Check out of bounds index, really ought to fail
        assert("" == separate_row("foo bar", 2))

    def test_seq_neg1(self):
        assert("foo bar" == separate_row("foo bar", -1))


if __name__ == '__main__':
    unittest.main()
