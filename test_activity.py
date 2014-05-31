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
from activity import Period
from json import loads
from jsonpickle import encode
import unittest

class TestPeriod (unittest.TestCase):
    """Unit tests for class Period"""

    def _assertEqualJSON (self, jsonA, jsonB):
        """Compare two json strings

        - jsonA (string): first string to compare
        - jsonB (string): second string to compare

        Returns a boolean with the result of the comparison
        """

        self.assertEqual (loads(jsonA), loads(jsonB))


    def test_period_json (self):
        """Test Period producing JSON"""
        correct_json = """
{
    "end": "2012-11-01T00:00:00",
    "start": "2011-12-01T00:00:00"
}
"""
        period = Period(datetime(2011,12,1), datetime(2012,11,1))
        period_json = encode(period, unpicklable=False)
        self._assertEqualJSON (period_json, correct_json)


if __name__ == "__main__":
    unittest.main()
