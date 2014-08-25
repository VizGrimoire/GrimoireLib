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
## Module to deal with timezones as found in the *Grimoire
##  (CVSAnalY, MLStats) databases
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from common import DBFamily
from scm_query import DB as SCMDatabase
from scm_query_tz import Query as SCMTZQuery
from mls_query import DB as MLSDatabase
from mls_query_tz import Query as MLSTZQuery

class ActivityTZ (DBFamily):
    """Root factory of entities in the ActivityTZ family.

    This class can be used to instantiate entities from the
    ActivityTZ family: those related to timezones in dates
    in git records, or in email messages.

    This is the root of a hierarchy of classes for the ActivityTZ
    families corresponding to the different data sources (SCM, MLS, etc.).

    Objects of this class provide the function timezones() to
    obtain a Timezones object, which is a dictionary with a "tz" key, with
    a list with values for all time zones (-12 .. 11) as data, and keys
    with lists of the values corresponding to all these timezones for the
    values of the entities of interest.

    """

    def _query_cls(self):
        """Return the default Query class.

        If None, the underlaying Query class used by default by the
        datasource will be used. This function returns None, child
        classes should override that if needed (usually, when a
        Query class which is not the default one for the datasource
        is needed).

        Returns
        -------

        GrimoireQuery: Query class.

        """

        return None

    def _produce_query (self, name):
        """Produce the base query to obtain activity per timezone.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: string
           Entity name.

        """

        raise Exception ("_produce_query should be provided by child class")

    def timezones (self):
        """Obtain the Timezones object.

        Extracts the activity list by querying the database
        according to the initialization of the object.
        The result is buffered, so that this method is invoked once
        again, the result is returned directly, with no need to query
        the database.

        Returns
        -------

        Timezones: activity for all timezones

        """

        try:
            return self.tz
        except AttributeError:
            self.tz = self.query.timezones()
            return self.tz

class SCMActivityTZ (ActivityTZ):
    """Constructor of entities in the SCMActivityTZ family.

    Interface to entities related to activity of persons in SCM.
    
    """

    def _query_cls(self):
        """Return the default Query class.

        Returns
        -------

        GrimoireQuery: Query class.

        """

        return SCMTZQuery

    def _produce_query (self, name):
        """Produce the base query to obtain activity per timezone.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: string
           Entity name.

        """

        if name in ("authors"):
            self.query = self.query.select_tz() \
                .filter_nomerges() \
                .group_by_tz()
        else:
            raise Exception ("SCMActivityTZ: " + \
                                 "Invalid entity name for this family, " + \
                                 name)


class MLSActivityTZ (ActivityTZ):
    """Constructor of entities in the MLSActivityTZ family.

    Interface to entities related to activity of persons in MLS.
    
    """

    def _query_cls(self):
        """Return the default Query class.

        Returns
        -------

        GrimoireQuery: Query class.

        """

        return MLSTZQuery

    def _produce_query (self, name):
        """Produce the base query to obtain activity per timezone.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: string
           Entity name.

        """

        if name in ("senders"):
            self.query = self.query.select_tz() \
                .group_by_tz()
        else:
            raise Exception ("MLSActivityTZ: " + \
                                 "Invalid entity name for this family, " + \
                                 name)


if __name__ == "__main__":

    from standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta
    from scm import PeriodCondition as SCMPeriodCondition
    from scm import NomergesCondition as SCMNomergesCondition

    stdout_utf8()

    database = SCMDatabase (url = "mysql://jgb:XXX@localhost/",
                   schema = "oscon_opennebula_scm_tz",
                   schema_id = "oscon_opennebula_scm_tz")

    #---------------------------------
    print_banner("Author activity per time zone")
    data = SCMActivityTZ (
        datasource = database,
        name = "authors")
    tz = data.timezones()
    print tz

    #---------------------------------
    print_banner("Author activity per time zone (now using session)")
    session = database.build_session(query_cls = SCMTZQuery)
    data = SCMActivityTZ (
        datasource = session,
        name = "authors")
    tz = data.timezones()
    print tz

    #---------------------------------
    print_banner("Author activity per time zone (using session, conditions)")
    period = SCMPeriodCondition (start = datetime(2014,1,1), end = None)
    nomerges = SCMNomergesCondition()
    data = SCMActivityTZ (
        datasource = session,
        name = "authors",
        conditions = (period,nomerges))
    tz = data.timezones()
    print tz

    database = MLSDatabase (url = 'mysql://jgb:XXX@localhost/',
                   schema = 'oscon_openstack_mls',
                   schema_id = 'oscon_openstack_scm')

    #---------------------------------
    print_banner("Sender activity per time zone")
    data = MLSActivityTZ (
        datasource = database,
        name = "senders")
    tz = data.timezones()
    print tz
