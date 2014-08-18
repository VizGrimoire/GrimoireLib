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

from common import DatabaseDefinition, DBFamily, DBCondition
from scm_query import SCMDatabase, SCMQuery

class SCMDatabaseDefinition (DatabaseDefinition):
    """Class for defining a SCM (CVSAnalY) Grimoire database.

    """

    def _datasource_cls(self):
        """Return classes related to datasource.

        Returns:
        --------

        common_query.GrimoireDatabase: subclass for Grimoire database to use
        common_query.GrimoireQuery: subclass for Grimoire Query to use

        """

        return SCMDatabase, SCMQuery


class SCM (DBFamily):
    """Constructor of entities in the SCM family.

    This class can be used to instantiate entities from the SCM
    fammily: direct entities obtained querying a CVSAnalY database.

    The entity to be instantiated is specified using its name, and
    a set of zero or more conditions that provide context.

    This class provides functions to return different kinds of aggregation
    (timeseries, total, list) and selection (dates, etc.).

    Valid entity names: "ncommits", "listcommits", "nauthors", "listauthors"

    """

    def _produce_query (self, name):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: {"list_authors" | "list_committers" |
           "list_uauthors" | "list_ucommitters"}
           Entity name.

        """

        if name == "ncommits":
            self.query = self.query.select_nscmlog(["commits",])
        elif name == "listcommits":
            self.query = self.query.select_listcommits()
        elif name == "nauthors":
            self.query = self.query.select_nscmlog(["authors",])
        elif name == "listauthors":
            self.query = self.query.select_listauthors()
        else:
            raise Exception ("SCM: Invalid entity name for this family, " + \
                                 name)

    def timeseries (self):
        """Return a timeseries for the entity"""

        return self.query.group_by_period().timeseries()

    def total (self):
        """Return the total count for the entity"""

        return self.query.scalar()

    def list (self, limit = 10):
        """Return a list for the specified entity"""

        return self.query.limit(limit).all()


class NomergesCondition (DBCondition):
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


class BranchesCondition (DBCondition):
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


class PeriodCondition (DBCondition):
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

    database = SCMDatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
                                   schema = "vizgrimoire_cvsanaly",
                                   schema_id = "vizgrimoire_cvsanaly")
    data = SCM (datasource = database,
                name = "ncommits")
    print data.timeseries()
    print data.total()

    period = PeriodCondition (start = datetime(2013,1,1), end = None)

    data = SCM (datasource = database,
                name = "ncommits", conditions = (period,))
    print data.timeseries()
    print data.total()

    data = SCM (datasource = database,
                name = "listcommits")
    print data.list()

    session = database.create_session()

    data = SCM (datasource = session,
                name = "nauthors", conditions = (period,))
    print data.timeseries()
    print data.total()

    branches = BranchesCondition (branches = ("master",))
    data = SCM (datasource = session,
                name = "nauthors", conditions = (period, branches))
    print data.timeseries()
    print data.total()
