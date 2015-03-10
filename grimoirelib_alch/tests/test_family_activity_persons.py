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
## Unit tests for family.activity_persons
## 
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.scm import DB
from grimoirelib_alch.family.scm import (
    SCM, 
    PeriodCondition as SCMPeriodCondition,
    NomergesCondition as SCMNomergesCondition,
    OrgsCondition as SCMOrgsCondition
    )
from grimoirelib_alch.family.activity_persons import SCMActivityPersons 
from grimoirelib_alch.type.activity import Period, ActivityList
from datetime import datetime
from sqlalchemy.util import KeyedTuple
import unittest

url = 'mysql://jgb:XXX@localhost/'
schema = 'oscon_openstack_scm'
schema_id = 'oscon_openstack_scm'
start = datetime(2013,11,13)
end = datetime(2014,2,1)
datefmt = "%Y-%m-%d %H:%M:%S"
rowlabels = ["person_id", "name", "firstdate", "lastdate"]

class TestActivityPersons (unittest.TestCase):

    def setUp (self):

        self.database = DB (url = url,
                            schema = schema,
                            schema_id = schema_id)
        self.session = self.database.build_session()
        self.start = start
        self.end = end

    def test_orgs_condition (self):
        """Test SCMActivityPersons object with an organizations condition"""

        correct = ActivityList(
            (KeyedTuple([774, u'John Dickinson',
                         datetime.strptime("2013-11-15 15:10:39", datefmt),
                         datetime.strptime("2014-01-24 14:53:17", datefmt)
                         ],
                        labels = rowlabels
                        ),
             KeyedTuple([1094, u'Samuel Merritt',
                         datetime.strptime("2013-11-13 12:52:21", datefmt),
                         datetime.strptime("2014-01-31 13:17:00", datefmt)
                         ],
                        labels = rowlabels                        
                        ),
             KeyedTuple([2071, u'Darrell Bishop',
                         datetime.strptime("2013-11-27 00:37:02", datefmt),
                         datetime.strptime("2013-11-27 12:07:42", datefmt)
                         ],
                        labels = rowlabels                        
                        ),
             KeyedTuple([2110, u'Jon Snitow',
                         datetime.strptime("2014-01-29 13:02:54", datefmt),
                         datetime.strptime("2014-01-29 13:02:54", datefmt)
                         ],
                        labels = rowlabels                        
                        )
             )
            )

        period = SCMPeriodCondition (start = self.start, end = self.end)
        nomerges = SCMNomergesCondition()
        orgs = SCMOrgsCondition (orgs = ("SwiftStack", "Inktank"),
                                 actors = "authors")
        data = SCMActivityPersons (
            datasource = self.session,
            name = "list_authors", conditions = (period,nomerges,orgs))
        activity = data.activity()
        print activity
        self.assertEqual (data.activity(), correct)

if __name__ == "__main__":
    unittest.main()
