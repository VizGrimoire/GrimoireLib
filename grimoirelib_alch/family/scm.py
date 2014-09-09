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

from common import DBFamily, DBCondition
from grimoirelib_alch.query.scm import DB, Query

class Entities(type):
    """Watcher for a metaclass, maintaining a dictionary with its subclasses

    This watcher class maintains a dictionary with all the subclasses of
    the metaclass that uses it.

    Attributes
    ----------

    subclasses : dictionary
        Subclasses of metaclass, keyed by name attribute of each subclass
        (values are the subclasses themselves)

    """

    subclasses = {}

    def __init__(cls, name, bases, clsdict):
        """Add subclass cls to subclasses, keyed by its name attribute.

        """

        if clsdict['name'] is not None:
            Entities.subclasses[clsdict['name']] = cls
    
class Entity ():
    """Metaclass, root of all entities.

    This is the root of the hierarchy of entities.
    It uses a watcher for keeping a dictionary with all its subclasses.
    Defines attributes and methods to be used in all the hierarchy.

    Attributes
    ----------

    name: str
        Entity name used for the entity represented by the class
    desc: str
        Description of the entity (one line)
    longdesc: str
        Description of the entity (long)

    Methods
    -------

    query (q)
        Returns the query to produce the entity

    """

    __metaclass__ = Entities

    name = None
    desc = None
    longdesc = None

    @staticmethod
    def query (q):
        """Return the query to produce this entity.

        Parameters
        ----------

        q: query.scm.Query
            Base query

        Returns
        -------

        query.scm.Query
            Query with the needed filters to produce the entity.

        """

        raise Exception ("query(): real entities should provide real code")

class NCommits (Entity):
    """Number of commits

    """

    name = "ncommits"
    desc = "Number of commits"
    longdesc = "Number of commits"

    @staticmethod
    def query (q):

        return q.select_nscmlog(["commits",])

class ListCommits (Entity):
    """List of commits

    """

    name = "listcommits"
    desc = "List of commits"
    longdesc = "List of commits"

    @staticmethod
    def query (q):

        return q.select_listcommits()

class NAuthors (Entity):
    """Number of authors

    """

    name = "nauthors"
    desc = "Number of authors"
    longdesc = "Number of authors"

    @staticmethod
    def query (q):

        return q.select_nscmlog(["authors",])

class ListAuthors (Entity):
    """List of authors

    """

    name = "listauthors"
    desc = "List of authors"
    longdesc = "List of authors"

    @staticmethod
    def query (q):

        return q.query.select_listauthors()


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

        if name in Entities.subclasses:
            self.query = Entities.subclasses[name].query(self.query)
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

        Parameters
        ----------

        query: Query
           Query to which the filter will be applied

        """

        return query.filter_period(start = self.start,
                                   end = self.end,
                                   date = self.date)

    def __init__ (self, start = None, end = None, date = "commit"):
        """Instatiation of the object.

        Parameters
        ----------

        start: datetime.datetime
           Start of the period
        end: datetime.datetime)
           End of the period
        date: { "commit", "author" }
           Consider "commit" or "author" date for the period

        """

        self.start = start
        self.end = end
        self.date = date


if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner
    from datetime import datetime

    stdout_utf8()

    database = DB (url = "mysql://jgb:XXX@localhost/",
                   schema = "vizgrimoire_cvsanaly",
                   schema_id = "vizgrimoire_cvsanaly")

    #---------------------------------
    print_banner ("Number of commits (timeseries, total)")
    data = SCM (datasource = database,
                name = "ncommits")
    print data.timeseries()
    print data.total()

    #---------------------------------
    print_banner ("Number of commits (timeseries, total), for period")
    period = PeriodCondition (start = datetime(2013,1,1), end = None)
    data = SCM (datasource = database,
                name = "ncommits", conditions = (period,))
    print data.timeseries()
    print data.total()

    #---------------------------------
    print_banner ("List of commits")
    data = SCM (datasource = database,
                name = "listcommits")
    print data.list()

    session = database.build_session()

    #---------------------------------
    print_banner ("Number of authors (timeseries, total)")
    data = SCM (datasource = session,
                name = "nauthors", conditions = (period,))
    print data.timeseries()
    print data.total()

    #---------------------------------
    print_banner ("Number of authors (timeseries, total) for a period and branch")
    branches = BranchesCondition (branches = ("master",))
    data = SCM (datasource = session,
                name = "nauthors", conditions = (period, branches))
    print data.timeseries()
    print data.total()
