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
## Unit tests for family.scm.py related to organizations
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB
from grimoirelib_alch.family.scm import (
    SCM, OrgsCondition
    )
from datetime import datetime
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'oscon_openstack_scm'
schema_id = 'oscon_openstack_scm'
start = datetime(2013,11,13)
end = datetime(2014,2,1)

class TestSCMOrgs (unittest.TestCase):

    def setUp (self):

        self.database = DB (url = url,
                            schema = schema,
                            schema_id = schema_id)
        self.session = self.database.build_session()
        self.start = start
        self.end = end

    def test_orgs_condition (self):
        """Test SCM object with an organizations condition"""

        orgs = OrgsCondition (orgs = ("Red Hat", "Midokura", "HP"),
                              actors = "authors")
        data = SCM (datasource = self.database, name = "ncommits",
                    conditions = (orgs,))
        self.assertEqual (data.total(), 20115)

if __name__ == "__main__":
    unittest.main()
