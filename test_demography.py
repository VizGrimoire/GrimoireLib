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
## Unit tests for activity.py
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from datetime import datetime, timedelta
from demography import ActivityPersons, DurationPersons, \
    SnapshotCondition, ActiveCondition
import unittest
from support_testing import equal_JSON_file, write_JSON

# Database with CVSAnaly data for OpenStack
openstack_database = 'mysql://jgb:XXX@localhost/openstack_cvsanaly_2014-06-06'
# Directory with test files
tests_dir = 'tests'

class TestActivityPersons (unittest.TestCase):
    """Unit tests for class ActivityPersons"""

    def setUp (self):
        self.snapshot = SnapshotCondition (date = datetime (2014,6,6))
        self.active_period = ActiveCondition (after = datetime(2014,6,6) - \
                                                  timedelta(days=180))

    def test_openstack_birth (self):
        """Test openstack "birth" JSON file"""

        birth = "openstack_2014-06-06_scm-demographics-birth.json"
        data = ActivityPersons (
            database = openstack_database,
            var = "list_authors")
        age = DurationPersons (var = "age",
                               conditions = (self.snapshot,),
                               activity = data.activity())
        # write_JSON (tests_dir + '/' + "output.json", age.durations())
        self.assertTrue(equal_JSON_file(age.durations().json(),
                                        tests_dir + '/' + birth,
                                        details = True))

    def test_openstack_aging (self):
        """Test openstack "aging" JSON file"""

        aging = "openstack_2014-06-06_scm-demographics-aging.json"
        data = ActivityPersons (
            database = openstack_database,
            var = "list_authors")
        age = DurationPersons (var = "age",
                               conditions = (self.snapshot,
                                             self.active_period),
                               activity = data.activity())
        self.assertTrue(equal_JSON_file(age.durations().json(),
                                        tests_dir + '/' + aging,
                                        details = True))


if __name__ == "__main__":
    unittest.main()
