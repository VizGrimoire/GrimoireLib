# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details. 
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#

"""Unit tests for analysis/ages.py"""

import sys
import unittest

from vizgrimoire.metrics.query_builder import DSQuery
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.SCM import SCM
from vizgrimoire.analysis.ages import Ages

db_cvsanaly = 'cp_cvsanaly_GrimoireLibTests'
db_identities = 'cp_sortinghat_GrimoireLibTests'

class TestAges(unittest.TestCase):

    def test_scm_simple(self):

        correct = """
{'aging': ActorsDuration:
{'age': datetime.timedelta(13, 3349), 'id': u'6872b4eeab3d91e5f47f142137b6672f64dc5102', 'name': u'6872b4eeab3d91e5f47f142137b6672f64dc5102'}
{'age': datetime.timedelta(30, 14598), 'id': u'86600750577bd979bcf1744a5d8aa387e3c0e088', 'name': u'86600750577bd979bcf1744a5d8aa387e3c0e088'}
{'age': datetime.timedelta(16, 1472), 'id': u'9d9c8d35f9a5e8537debda180d0d5a818ed453c1', 'name': u'9d9c8d35f9a5e8537debda180d0d5a818ed453c1'}
{'age': datetime.timedelta(29, 12176), 'id': u'c2c10e1bff19bb5783c0eea4beb9cf73df3e314e', 'name': u'c2c10e1bff19bb5783c0eea4beb9cf73df3e314e'}
, 'birth': ActorsDuration:
{'age': datetime.timedelta(13, 3349), 'id': u'6872b4eeab3d91e5f47f142137b6672f64dc5102', 'name': u'6872b4eeab3d91e5f47f142137b6672f64dc5102'}
{'age': datetime.timedelta(30, 14598), 'id': u'86600750577bd979bcf1744a5d8aa387e3c0e088', 'name': u'86600750577bd979bcf1744a5d8aa387e3c0e088'}
{'age': datetime.timedelta(16, 1472), 'id': u'9d9c8d35f9a5e8537debda180d0d5a818ed453c1', 'name': u'9d9c8d35f9a5e8537debda180d0d5a818ed453c1'}
{'age': datetime.timedelta(29, 12176), 'id': u'c2c10e1bff19bb5783c0eea4beb9cf73df3e314e', 'name': u'c2c10e1bff19bb5783c0eea4beb9cf73df3e314e'}
}
"""
        correct = ''.join(correct.split())

        filters = MetricFilters("months", "'2013-12-01'", "'2014-01-01'", [])
        dbcon = DSQuery(user = "jgb", password = "XXX", 
                        database = db_cvsanaly,
                        identities_db = db_identities)
        ages = Ages(dbcon, filters)
        result = ages.result(SCM)
        self.assertEqual(''.join(str(result).split()), correct)


if __name__ == "__main__":
    unittest.main()
