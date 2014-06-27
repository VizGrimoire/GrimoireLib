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
## Package to deal with ITS Conditions
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##


class Condition ():
    """Root of all conditions

    Provides a filter method which will be called when applying the condition.
    """

    def filter (query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query


class PeriodCondition (Condition):
    """Period Condition for qualifying a variable

    Specifies the period when the variable has to be considered"""

    def filter (self, query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query.filter_period(start = self.start,
                                   end = self.end,
                                   date = self.date)

    def __init__ (self, start = None, end = None, date = "change"):
        """Instatiation of the object.

        - start (datetime): start of the period
        - end (datetime): end of the period
        - date: "commit" or "author"
            Git maintains "commit" and "author" date
        """

        self.start = start
        self.end = end
        self.date = date


if __name__ == "__main__":

    from datetime import datetime

    data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                var = "ncommits")
    print data.timeseries()
    print data.total()

    period = PeriodCondition (start = datetime(2013,1,1), end = None)

    data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                var = "ncommits", conditions = (period,))
    print data.timeseries()
    print data.total()

    data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                var = "listcommits")
    print data.list()

    data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                var = "nauthors", conditions = (period,))
    print data.timeseries()
    print data.total()

    branches = BranchesCondition (branches = ("master",))
    data = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                var = "nauthors", conditions = (period, branches))
    print data.timeseries()
    print data.total()
