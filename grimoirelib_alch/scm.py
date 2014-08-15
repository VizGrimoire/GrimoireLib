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
## Package to deal with SCM data from *Grimoire (CVSAnalY databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from common import DatabaseDefinition, Family
from scm_query import SCMDatabase, SCMQuery


class SCM (Family):
    """Entities of the SCM family.

    This class can be used to instantiate entities from the SCM
    fammily: direct entities obtained querying a CVSAnalY database.

    The entity to be instantiated is specified using its name, and
    a set of zero or more conditions that provide context.

    This class provides functions to return different kinds of aggregation
    (timeseries, total, list) and selection (dates, etc.).

    Valid entity names: "ncommits", "listcommits", "nauthors", "listauthors"

    """

    def timeseries (self):
        """Return a timeseries for the entity"""

        return self.query.group_by_period().timeseries()

    def total (self):
        """Return the total count for the entity"""

        return self.query.scalar()

    def list (self, limit = 10):
        """Return a list for the specified entity"""

        return self.query.limit(limit).all()


    def _init (self, name, conditions):
        """Initialize the entity, once a session is ready.

        Parameters
        ----------

        name: {"ncommits", "listcommits", "nauthors", "listauthors"}
           Entity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.

        """

        if name == "ncommits":
            self.query = self.session.query().select_nscmlog(["commits",])
        elif name == "listcommits":
            self.query = self.session.query().select_listcommits()
        elif name == "nauthors":
            self.query = self.session.query().select_nscmlog(["authors",])
        elif name == "listauthors":
            self.query = self.session.query().select_listauthors()
        else:
            raise Exception ("No valid entity name for this family: " + \
                                 name)
        for condition in conditions:
            self.query = condition.filter(self.query)

    def _create_session (self, database, echo):
        """Creates a session given a database definition.

        Parameters
        ----------

        database: Common.DatabaseDefinition
           Names defining the database.
        echo: Boolean
           Write SQL queries to output stream or not.

        Returns
        -------

        session: SQLAlchemy session suitable for querying.

        """

        DB = SCMDatabase(database = database.url,
                         schema = database.schema,
                         schema_id = database.schema_id)
        return DB.build_session(SCMQuery, echo = echo)


class Condition ():
    """Root of all conditions

    Provides a filter method which will be called when applying the condition.
    """

    def filter (query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query


class NomergesCondition (Condition):
    """No merges Condition for qualifying an entity

    Specifies that only "no merges" commits are to be considered,
    that is, commits that touch files"""

    def filter (self, query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query.filter_nomerges()

    def __init__ (self):
        """Instatiation of the object.
        """

        pass


class BranchesCondition (Condition):
    """Branches Condition for qualifying an entity.

    Specifies the branches to be considered"""

    def filter (self, query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query.filter_branches(branches = self.branches)

    def __init__ (self, branches):
        """Instatiation of the object.

        - branches (list of string): list of branches to consider
        """

        self.branches = branches


class PeriodCondition (Condition):
    """Period Condition for qualifying an entity.

    Specifies the period when the entity has to be considered"""

    def filter (self, query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return query.filter_period(start = self.start,
                                   end = self.end,
                                   date = self.date)

    def __init__ (self, start = None, end = None, date = "commit"):
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

    database = DatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
                                   schema = "vizgrimoire_cvsanaly",
                                   schema_id = "vizgrimoire_cvsanaly")
    data = SCM (database = database,
                name = "ncommits")
    print data.timeseries()
    print data.total()

    period = PeriodCondition (start = datetime(2013,1,1), end = None)

    data = SCM (database = database,
                name = "ncommits", conditions = (period,))
    print data.timeseries()
    print data.total()

    data = SCM (database = database,
                name = "listcommits")
    print data.list()

    session = data.get_session()

    data = SCM (session = session,
                name = "nauthors", conditions = (period,))
    print data.timeseries()
    print data.total()

    branches = BranchesCondition (branches = ("master",))
    data = SCM (session = session,
                name = "nauthors", conditions = (period, branches))
    print data.timeseries()
    print data.total()
