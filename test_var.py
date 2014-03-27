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
## Unit tests for var.py
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from var import VariableFactory
from json import loads
import unittest

database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly'
# Set UTF-8, and avoid the DBAPI Unicode support, to use SQLAlchemy's,
# which is said to be more efficient
database += '?charset=utf8&use_unicode=0'

class TestVar (unittest.TestCase):

    def _assertEqualJSON (self, jsonA, jsonB):
        """Compare two json strings

        - jsonA (string): first string to compare
        - jsonB (string): second string to compare

        Returns a boolean with the result of the comparison
        """

        self.assertEqual (loads(jsonA), loads(jsonB))


    def setUp (self):

        self.database = database
        self.start = "2013-11-13"
        self.end = "2014-2-1"


    def test_variable_scm_ncommits (self):
        """Test scm/ncommits variable"""

        correct_json = (
            """{
            "desc": "Number of commits",
            "id": "scm/ncommits",
            "type": "total",
            "value": 4465
            }""",
            """{
            "desc": "Number of commits",
            "id": "scm/ncommits",
            "type": "total",
            "value": 730
            }""",
            )

        var_factory = VariableFactory (database = database)
        ncommits = var_factory.make("scm/ncommits")
        self.assertEqual (ncommits.value(), 4465)
        ncommits_json = ncommits.json(pretty=True)
        self._assertEqualJSON (ncommits_json, correct_json[0])
        ncommits.set_conditions ({"period": (self.start, self.end)})
        self.assertEqual (ncommits.value(), 730)
        ncommits_json = ncommits.json(pretty=True)
        self._assertEqualJSON (ncommits_json, correct_json[1])

if __name__ == "__main__":
    unittest.main()
