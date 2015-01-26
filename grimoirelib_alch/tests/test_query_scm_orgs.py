# -*- coding: utf-8 -*-

## Copyright (C) 2015 Bitergia
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
## Unit tests for scm_query.py related to organizations
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB, Query
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'oscon_openstack_scm'
schema_id = 'oscon_openstack_scm'

class TestSCMQueryOrgs (unittest.TestCase):

    def setUp (self):
        database = DB (url = url,
                       schema = schema,
                       schema_id = schema_id)
        self.session = database.build_session(Query, echo = False)

    def test_select_orgs (self):
        """Test select_orgs

        """

        correct = [(1, "Red Hat"),
                   (2, "Midokura"),
                   (3, "Inktank"),
                   (4, "IBM"),
                   (5, "HP")]

        res = self.session.query().select_orgs()
        self.assertEqual (res.limit(5).all(), correct)

    def test_filter_orgs (self):
        """Test select_orgs

        """

        correct = [(1, "Red Hat"),
                   (2, "Midokura"),
                   (4, "IBM"),
                   (5, "HP")]

        res = self.session.query().select_orgs() \
            .filter_orgs(org[1] for org in correct)
        self.assertEqual (res.all(), correct)


if __name__ == "__main__":
    unittest.main()
