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

from common import DBFamily, DBCondition, Entities, Entity
from grimoirelib_alch.query.mls import DB, Query
from sqlalchemy import inspect

class Persons (Entity):
    """Number of persons

    """

    name = "npersons"
    desc = "Number of persons in MLS"
    longdesc = "Number of persons in mailing lists"

    @staticmethod
    def query (q):

        return q.select_personsdata_uid("senders").distinct()


class MLS (DBFamily):
    """Constructor of entities in the MLS family.

    This class can be used to instantiate entities from the MLS
    fammily: direct entities obtained querying a Bicho database.

    The entity to be instantiated is specified using its name, and
    a set of zero or more conditions that provide context.

    This class provides functions to return different kinds of aggregation
    (timeseries, total, list) and selection (dates, etc.).

    Valid entity names: "npersons".

    """

    def _produce_query (self, name):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: {"npersons"}
           Entity name.

        """

        if name in Entities.subclasses:
            self.query = Entities.subclasses[name].query(self.query)
        else:
            raise Exception ("MLS: Invalid entity name for this family, " + \
                                 name)

    def timeseries (self):
        """Return a timeseries for the entity"""

        return self.query.count().group_by_period().timeseries()

    def total (self):
        """Return the total count for the entity"""

        return self.query.count()

    def list (self, limit = 10):
        """Return a list for the specified entity"""

        return self.query.limit(limit).all()


class PeriodCondition (DBCondition):
    """Period Condition for qualifying an entity.

    Specifies the period when the variable has to be considered"""

    def filter (self, query):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        if self.date == "check":
            # Check if arrival_date is available
            if query.null_arrival().count() == 0:
                self.date = "arrival"
            else:
                self.date = "first"
        return query.filter_period(start = self.start,
                                   end = self.end,
                                   date = self.date)

    def __init__ (self, start = None, end = None, date = "arrival"):
        """Instatiation of the object.

        start: datetime.datetime
            Start of the period.
        end: datetime.datetime
            End of the period.
        date: { "first" | "arrival" | "check" }
            Select "first_date" or "arrival_date" from messages,
            ir check which one is available (arrival if available,
            first otherwise).
            Default: "arrival".

        """

        self.start = start
        self.end = end
        self.date = date


class OrgsCondition (DBCondition):
    """Organization Condition for qualifying an entity.

    Specifies the organizations of interest: only their activity will
    be considered.
    """

    def __init__ (self, orgs, actors = "senders", date = "arrival"):
        """Instatiation of the object.

        Parameters
        ----------

        orgs: list of str
           List of organziations of interest 
        actors: {"senders"}
            Kind of actors to consider
        date: { "first" | "arrival" | "check" }
            Select "first_date" or "arrival_date" from messages,
            ir check which one is available (arrival if available,
            first otherwise).
            Default: "arrival".

        """

        self.org_names = orgs
        self.actors = actors
        self.date = date

    def filter (self, query):
        """Filter to apply for this condition

        Parameters
        ----------

        query: Query
           Query to which the filter will be applied

        """

        if self.date == "check":
            # Check if arrival_date is available
            if query.null_arrival().count() == 0:
                self.date = "arrival"
            else:
                self.date = "first"
        # Get the session for this query, use it for getting org ids,
        # and build the filter
        session = inspect(query).session
        query_orgs = session.query().select_orgs() \
            .filter_orgs (self.org_names)
        self.org_ids = [org.org_id for org in query_orgs.all()]
        return query.filter_org_ids(list = self.org_ids,
                                    kind = self.actors,
                                    date = self.date)


if __name__ == "__main__":

    print "Nothing to be done (yet)"
