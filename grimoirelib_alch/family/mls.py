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
## Package to deal with MLS data from *Grimoire (MLStats databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from common import DBFamily, DBCondition
from grimoirelib_alch.query.mls import DB, Query

class PeriodCondition (DBCondition):
    """Period Condition for qualifying an entity.

    Specifies the period when the variable has to be considered"""

    def filter (self, query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query.filter_period(start = self.start,
                                   end = self.end,
                                   date = self.date)

    def __init__ (self, start = None, end = None, date = "arrival"):
        """Instatiation of the object.

        start: datetime.datetime
            Start of the period.
        end: datetime.datetime
            End of the period.
        date: { "first", "arrival" }
            Select "first_date" or "arrival_date" from messages.
            Default: "arrival".

        """

        self.start = start
        self.end = end
        self.date = date


if __name__ == "__main__":

    print "Nothing to be done (yet)"
