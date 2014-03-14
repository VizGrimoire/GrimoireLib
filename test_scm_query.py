# -*- coding: utf-8 -*-

## Copyright (C) 2014 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## Unit tests for scm_query.py
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from scm_query import buildSession
from datetime import datetime
import unittest

database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly'

class TestBuildSession (unittest.TestCase):

    def setUp (self):
        self.database = database

    def test_get_session (self):

        session = buildSession(database=self.database, echo=False)

class TestSCMQuery (unittest.TestCase):

    def setUp (self):

        self.database = database
        self.session = buildSession(database=self.database, echo=False)

    def test_query_ncommits (self):

        res = self.session.query().select_ncommits().all()
        self.assertEqual (res, [(4465L,)])

    def test_query_ncommits_nomerge (self):

        res = self.session.query().select_ncommits().filter_nomerges().all()
        self.assertEqual (res, [(4206L,)])

    def test_query_authors_nomerge (self):

        res = self.session.query().select_nauthors().filter_nomerges().all()
        self.assertEqual (res, [(14L,)])

    def test_query_authors (self):

        res = self.session.query().select_nauthors().all()
        self.assertEqual (res, [(14L,)])


    def _test_select_nscmlog (self, variable):

        # Number of commits
        res = self.session.query().select_nscmlog(variable).all()
        self.assertEqual (res, [(4465L,)])
        # Number of commits, except those that don't touch files (merges)
        res = self.session.query() \
            .select_nscmlog(variable) \
            .filter_nomerges() \
            .all()
        self.assertEqual (res, [(4206L,)])

    def test_select_ncsmlog (self):
        """Test select_ncsmlog"""

        self._test_select_nscmlog ("commits")

    def test_query_authors_period (self):
        """Test nauthors with period."""

        # Test for nauthors
        res = self.session.query().select_nauthors() \
            .filter_period(start=datetime(2014,1,1), \
                           end=datetime(2014,2,1)) \
            .all()
        self.assertEqual (res, [(8L,)])
        # Test for nauthors that "touch files" (merges are excluded)
        res = self.session.query().select_nauthors() \
            .filter_nomerges() \
            .filter_period(start=datetime(2014,1,1), \
                           end=datetime(2014,2,1)) \
            .all()
        self.assertEqual (res, [(7L,)])
        # Test for nauthors, using authoring date
        res = self.session.query().select_nauthors() \
            .filter_period(start=datetime(2014,1,1), \
                           end=datetime(2014,2,1),
                           date="author") \
            .all()
        self.assertEqual (res, [(8L,)])


if __name__ == "__main__":
    unittest.main()
