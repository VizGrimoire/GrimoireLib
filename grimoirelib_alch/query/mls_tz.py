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
## Package to deal with queries for MLS data from *Grimoire, related to
##  timezones in dates (MLStats databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy.sql import label
from sqlalchemy import func
from mls import DB
from mls import Query as MLSQuery


class Query (MLSQuery):
    """Class for dealing with SCM queries involving time zones"""

    def select_tz (self):
        """Select time zones and other related fields from database.

        Selects count of messages, count of distinct senders,
        time zone.

        Returns
        -------

        Query object

        """

        query = self.add_columns(
            label("tz",
                  ((DB.Messages.first_date_tz.op('div')(3600) + 36) % 24) - 12),
            label("messages",
                  func.count(func.distinct(DB.Messages.message_ID))),
            label("authors",
                  func.count(func.distinct(DB.MessagesPeople.email_address))))
        if DB.MessagesPeople not in self.joined:
            query = query.join (DB.MessagesPeople)
            self.joined.append (DB.MessagesPeople)
            query = query.filter (DB.MessagesPeople.type_of_recipient == "From")
        return query

    def group_by_tz (self):
        """Group by timezone (field tz).

        Returns
        -------

        Query object

        """

        return self.group_by ("tz")

    def timezones (self):
        """Return a TimeZones object.

        The query has to include a group_by_tz filter.

        """

        # tz is a dict with timezones as keys, and result rows as tuples
        tz = {}
        for row in self.all():
            tz[row.tz] = row
        ncols = len (row)
        fields = row._fields
        for zone in range (-12, 12):
            if zone not in tz:
                tz[zone] = [zone] + [0] * (ncols - 1)
        # timezone is the resulting dict, with a key for each result column
        timezones = {}
        for field in fields:
            timezones[field] = []
        for zone in range (-12, 12):
            col = 0
            for field in fields:
                timezones[field].append (tz[zone][col])
                col = col + 1
        return timezones

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner
    from datetime import datetime
    import jsonpickle
    import csv

    stdout_utf8()

    database = DB (url = 'mysql://jgb:XXX@localhost/',
                   schema = 'oscon_openstack_mls',
                   schema_id = 'oscon_openstack_scm')
    session = database.build_session(query_cls = Query, echo = False)

    #---------------------------------
    print_banner ("Activity per timezone, raw from database")
    res = session.query().select_tz()
    res = res.filter_period(start=datetime(2014,1,1),
                            end=datetime(2014,7,1))
    res = res.group_by_tz()

    print res.all()

    #---------------------------------
    print_banner ("Activity per timezone, raw from database, JSON")
    print jsonpickle.encode(res.all(), unpicklable=False)

    #---------------------------------
    print_banner ("Activity per timezone, raw, CSV in /tmp/tz.csv")
    with open ('/tmp/mls-tz.csv', 'wb') as csvfile:
        tz_writer = csv.writer(csvfile, delimiter=' ',
                               quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in res.all():
            tz_writer.writerow (row)

    #---------------------------------
    print_banner ("Activity per timezone, timezones()")
    print res.timezones()
