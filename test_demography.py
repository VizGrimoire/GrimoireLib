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

from datetime import datetime
from demography import ActivityPersons
import codecs
import unittest
from test_support import equalJSON_file_persons

# Database with CVSAnaly data for OpenStack
openstack_database = 'mysql://jgb:XXX@localhost/openstack_cvsanaly_2014-06-06'
# Directory with test files
tests_dir = 'tests'

class TestActivityPersons (unittest.TestCase):
    """Unit tests for class ActivityPersons"""

    def test_openstack_birth (self):
        """Test openstack "birth" JSON file"""

        birth = "openstack_2014-06-06_scm-demographics-birth.json"
        data = ActivityPersons (
            database = openstack_database,
            var = "list_authors")
        activity = data.activity()
        age = activity.age(datetime(2014,6,6))
        with codecs.open(tests_dir + '/' + "output.json", "w", "utf-8") as file:
            file.write(age.json())
        self.assertTrue( equalJSON_file_persons(age.json(),
                                         tests_dir + '/' + birth))

    # def test_openstack_aging (self):
    #     """Test openstack "aging" JSON file"""

    #     aging = "openstack_2014-06-06_scm-demographics-aging.json"
    #     data = ActivityPersons (
    #         database = openstack_database,
    #         var = "list_authors")
    #     activity = data.activity()
    #     idle = activity.idle(datetime(2014,6,6))
    #     self.assertTrue( _equalJSON_file_persons(idle.json(),
    #                                      tests_dir + '/' + aging))


if __name__ == "__main__":
    unittest.main()
