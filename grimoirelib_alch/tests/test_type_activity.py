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

from support import equal_JSON
from grimoirelib_alch.type.activity import Period, ActivityList

from datetime import datetime
from sqlalchemy.util import KeyedTuple
from jsonpickle import encode
import unittest


class TestPeriod (unittest.TestCase):
    """Unit tests for class Period"""

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
        self.assertTrue( equal_JSON(period_json, correct_json))

class TestActivityList (unittest.TestCase):
    """Unit tests for class ActivityList"""

    def test_activity_json (self):
        """Test Period producing JSON"""

        correct_json = """
[
    {
        "id": 12,
        "name": "Fulano Larguiño",
        "period": {
            "end": "2012-11-01T00:00:00",
            "start": "2011-12-01T00:00:00"
        }
    },
    {
        "id": 3,
        "name": "Mengana Corta",
        "period": {
            "end": "2013-02-03T00:00:00",
            "start": "2010-02-03T00:00:00"
        }
    }
]
"""

        rowlabels = ["person_id", "name", "firstdate", "lastdate"]
        list = ActivityList((KeyedTuple([12, "Fulano Larguiño",
                                         datetime(2011,12,1),
                                         datetime(2012,11,1)],
                                        labels = rowlabels),
                             KeyedTuple([3, "Mengana Corta",
                                         datetime(2010,2,3),
                                         datetime(2013,2,3)],
                                        labels = rowlabels)))
        activity_json = encode(list, unpicklable=False)
        self.assertTrue( equal_JSON( activity_json, correct_json ))

if __name__ == "__main__":
    unittest.main()
