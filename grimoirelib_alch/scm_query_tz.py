#! /usr/bin/python
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
## Package to deal with queries for SCM data from *Grimoire, related to
##  timezones in dates (CVSAnalY databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy.sql import label
from sqlalchemy import func
from scm_query import DB, SCMQuery

class SCMTZQuery (SCMQuery):
    """Class for dealing with SCM queries involving time zones"""

    def select_tz (self):
        """Select time zones and other related fields from database.

        Selects count of commits, count of distinct authors,
        time zone, and month / year for author_date.

        Returns
        -------

        SCMTZQuery object

        """

        query = self.add_columns(
            label("tz",
                  ((DB.SCMLog.author_date_tz.op('div')(3600) + 36) % 24) - 12),
            label("commits", func.count(func.distinct(DB.SCMLog.id))),
            label("authors", func.count(func.distinct(DB.SCMLog.author_id))))
        return query

    def group_by_tz (self):
        """Group by timezone (field tz).

        Returns
        -------

        SCMTZQuery object

        """

        return self.group_by ("tz")


if __name__ == "__main__":

    import sys
    import codecs
    from standalone import print_banner

    from datetime import datetime
    import jsonpickle
    import csv

    # Trick to make the script work when using pipes
    # (pipes confuse the interpreter, which sets codec to None)
    # http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    database = DB (database = 'mysql://jgb:XXX@localhost/',
                   schema = 'oscon_opennebula_scm_tz',
                   schema_id = 'oscon_opennebula_scm_tz')
    session = database.build_session(SCMTZQuery, echo = False)

    #---------------------------------
    print_banner ("Number of commits")
    res = session.query().select_tz()
    res = res.filter_period(start=datetime(2014,1,1),
                            end=datetime(2014,7,1))
    res = res.group_by_tz()

    print res
    print res.all()

    print jsonpickle.encode(res.all(), unpicklable=False)

    with open ('/tmp/tz.csv', 'wb') as csvfile:
        tz_writer = csv.writer(csvfile, delimiter=' ',
                               quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in res.all():
            tz_writer.writerow (row)
