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
## Unit tests for query/its.py
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.its import DB, Query
from datetime import datetime
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'cp_bicho_GrimoireLibTests'
schema_id = 'cp_cvsanaly_GrimoireLibTests'
start = datetime(2013,11,13)
end = datetime(2014,2,1)

class TestSCMQuery (unittest.TestCase):

    def setUp (self):
        database = DB (url = url,
                       schema = schema,
                       schema_id = schema_id)
        self.session = database.build_session(Query, echo = False)


    def test_select_personsdata_uid (self):
        """Test select_personsdata_uid"""

        correct = [
            [(50L, u'Michael Movchin'), (51L, u'Peter Bena')],
            132,
            16500]

        res = self.session.query().select_personsdata_uid("openers")
        self.assertEqual (res.distinct().limit(2).all(),
                          correct[0])
        self.assertEqual (res.count(),
                          correct[1])
        self.assertEqual (res.filter_period(start=start, end=end).count(),
                          correct[2])


if __name__ == "__main__":
    unittest.main()
