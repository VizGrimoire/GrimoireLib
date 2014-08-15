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
## Unit tests for scm.py
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from datetime import datetime
from common import DatabaseDefinition
from scm_query import SCMDatabase, SCMQuery
from scm import SCM, NomergesCondition, BranchesCondition, PeriodCondition
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'vizgrimoire_cvsanaly'
schema_id = 'vizgrimoire_cvsanaly'

class TestSCM (unittest.TestCase):

    def setUp (self):

        self.database = DatabaseDefinition (url = url,
                                            schema = schema,
                                            schema_id = schema_id)
        self.session = SCM (database = self.database).get_session()
        self.start = datetime(2013,11,13)
        self.end = datetime(2014,2,1)


    def test_no_condition (self):
        """Test SCM object with no conditions"""

        data = SCM (database = self.database, name = "ncommits")
        self.assertEqual (data.total(), 4465)


    def test_nomerges_condition (self):
        """Test SCM object with a no merges condition"""

        nomerges = NomergesCondition ()
        data = SCM (database = self.database, name = "ncommits",
                    conditions = (nomerges,))
        self.assertEqual (data.total(), 4206)


    def test_branches_condition (self):
        """Test SCM object with a branches condition"""

        # Master branch
        branches = BranchesCondition (branches = ("master",))
        data = SCM (database = self.database, name = "ncommits",
                    conditions = (branches,))
        self.assertEqual (data.total(), 3685)

    def test_period_condition (self):
        """Test SCM object with a period condition"""

        # Only start for period
        period = PeriodCondition (start = self.start, end = None)
        data = SCM (database = self.database, name = "ncommits",
                    conditions = (period,))
        self.assertEqual (data.total(), 839)
        # Start and end
        period = PeriodCondition (start = self.start, end = self.end)
        data = SCM (database = self.database, name = "ncommits",
                    conditions = (period,))
        self.assertEqual (data.total(), 730)
        # Start and end, authoring date
        period = PeriodCondition (start = self.start, end = self.end,
                                  date = "author")
        data = SCM (database = self.database, name = "ncommits",
                    conditions = (period,))
        self.assertEqual (data.total(), 728)


    def test_combined_conditions (self):
        """Test SCM object with a combination of conditions"""

        # Branches and period, and the other way around
        branches = BranchesCondition (branches = ("master",))
        period = PeriodCondition (start = self.start, end = self.end,
                                  date = "author")
        data = SCM (session = self.session, name = "ncommits",
                    conditions = (branches,period))
        self.assertEqual (data.total(), 647)
        data = SCM (session = self.session, name = "ncommits",
                    conditions = (period, branches))
        self.assertEqual (data.total(), 647)
        # Branches, period and merges (in several orders)
        nomerges = NomergesCondition ()
        data = SCM (session = self.session, name = "ncommits",
                    conditions = (nomerges, period, branches))
        self.assertEqual (data.total(), 647)
        data = SCM (session = self.session, name = "ncommits",
                    conditions = (branches, nomerges, period))
        self.assertEqual (data.total(), 647)

if __name__ == "__main__":
    unittest.main()
