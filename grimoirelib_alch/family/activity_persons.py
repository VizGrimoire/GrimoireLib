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
## Module to deal with activity of persons as found in the *Grimoire
##  (CVSAnalY, Bicho, MLStats) databases
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from common import DBFamily
from grimoirelib_alch.query.scm import (
    DB as SCMDatabase,
    Query as SCMQuery
    )
from grimoirelib_alch.query.its import (
    DB as ITSDatabase,
    Query as ITSQuery
    )
from grimoirelib_alch.query.mls import (
    DB as MLSDatabase,
    Query as MLSQuery
    )


class ActivityPersons (DBFamily):
    """Root factory of entities in the ActivityPersons family.

    This class can be used to instantiate entities from the ActivityPersons
    family: those related to activity of persons. In this classes, activity
    is understood as periods (start data, end date) with activity.

    This is the root of a hierarchy of classes for the ActivityPersons
    families corresponding to the different data sources (SCM, ITS, MLS, etc.).

    Objects of this class provide the function activity() to
    obtain an ActivityList object, which are a list with the period of
    activity for each developer. From this ActivityList object, ages or
    idle period for actors can be obtained.

    """

    def __str__ (self):

        try:
            string = "Activity list: \n" + str(self.activity_list)
        except AttributeError:
            string = "Activity list not yet defined."
        print "Query: " + str(self.query)
        return string

    def __repr__ (self):

        return self.__str__()

    def _produce_query (self, name):
        """Produce the base query to obtain activity per person.

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

    def activity (self):
        """Obtain the activity list (ActivityList object).

        Extracts the activity list by querying the database
        according to the initialization of the object.
        The result is buffered, so that this method is invoked once
        again, the result is returned directly, with no need to query
        the database.

        Returns
        -------

        ActivityList: activity list for all actors

        """

        try:
            return self.activity_list
        except AttributeError:
            self.activity_list = self.query.activity()
            return self.activity_list

class SCMActivityPersons (ActivityPersons):
    """Constructor of entities in the SCMActivityPersons family.

    Interface to entities related to activity of persons in SCM.
    
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

        if name in ("list_authors", "list_uauthors"):
            persons = "authors"
        elif name in ("list_committers", "list_ucommitters"):
            persons = "committers"
        else:
            raise Exception ("SCMActivityPersons: " + \
                                 "Invalid entity name for this family, " + \
                                 name)
        if name in ("list_authors", "list_committers"):
            self.query = self.query.select_personsdata(persons)
        elif name in ("list_uauthors", "list_ucommitters"):
            self.query = self.query.select_personsdata_uid(persons)
        self.query = self.query \
            .select_commitsperiod() \
            .group_by_person()


class ITSActivityPersons (ActivityPersons):
    """Constructor of entities in the ITSActivityPersons family.

    Interface to entities related to activity of persons in ITS.
    
    """

    def _produce_query (self, name):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: {"list_changers" | "list_uchangers"}
           Entity name.

        """

        if name in ("list_changers", "list_uchangers"):
            persons = "changers"
        else:
            raise Exception ("ITSActivityPersons: " + \
                                 "Invalid entity name for this family, " + \
                                 name)
        self.query = self.session.query()
        if name in ("list_changers"):
            self.query = self.query.select_personsdata(persons)
        elif name in ("list_uchangers"):
            self.query = self.query.select_personsdata_uid(persons)
        self.query = self.query \
            .select_changesperiod() \
            .group_by_person()


class MLSActivityPersons (ActivityPersons):
    """Constructor of entities in the MLSActivityPersons family.

    Interface to entities related to activity of persons in MLS.
    
    """

    def __init__ (self, datasource, name, conditions = (),
                  date_kind = "arrival", echo = False):
        """Initialization, which will include calling parent init.

        This initialization is defined for this class so that
        the kind of date to be used can be specified. Message data
        can include two dates: first date and arrival date.
        This parameter sets which one will be used to compute activity.

        Parameters
        ----------
        
        datasource: database, object, etc.
           Definition of the data source used to compute values for
           the entity.
        name: string
           Entity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).
        date_kind: { "arrival" | "first" | "check"}
            Kind of date to consider. Default: "arrival".
            "check" means: if arrival available, use "arrival". Else,
            use "first"
        echo: Boolean
           Write debugging information to output stream or not.

        """

        self.date_kind = date_kind
        ActivityPersons.__init__(self, datasource, name, conditions, echo)


    def _produce_query (self, name):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: {"list_senders" | "list_usenders"}
           Entity name.

        """

        if name in ("list_senders", "list_usenders"):
            persons = "senders"
        else:
            raise Exception ("MLSActivityPersons: " + \
                                 "Invalid entity name for this family, " + \
                                 name)
        if self.date_kind == "check":
            if self._arrival_available():
                self.date_kind = "arrival"
            else:
                self.date_kind = "first"
        self.query = self.session.query()
        date_kind = self.date_kind
        if name in ("list_senderss"):
            self.query = self.query.select_personsdata(persons)
        elif name in ("list_usenders"):
            self.query = self.query.select_personsdata_uid(persons)
        self.query = self.query \
            .select_activeperiod(date_kind) \
            .group_by_person()
        
    def _arrival_available (self):
        """Are arrival dates for messages available?

        Check if all messages have arrival dates. Return True if yes,
        False otherwise.

        Returns
        -------

        bool: Arrival dates are available.

        """

        if self.query.null_arrival().count() == 0:
            return True
        else:
            return False

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta
    from scm import PeriodCondition as SCMPeriodCondition
    from scm import NomergesCondition as SCMNomergesCondition
    from its import PeriodCondition as ITSPeriodCondition

    stdout_utf8()

    # SCM database
    database = SCMDatabase (url = "mysql://jgb:XXX@localhost/",
                   schema = "vizgrimoire_cvsanaly",
                   schema_id = "vizgrimoire_cvsanaly")
    session = database.build_session()

    #---------------------------------
    print_banner("List of activity for each author")
    data = SCMActivityPersons (
        datasource = database,
        name = "list_authors")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("Age (days since first activity) for each author.")
    age = activity.age(datetime(2014,1,1))
    print age.json()
    #---------------------------------
    print_banner("Idle (days since last activity) for each author.")
    idle = activity.idle(datetime(2014,1,1))
    print idle.json()

    #---------------------------------
    print_banner("List of activity for each author (no merges)")
    period = SCMPeriodCondition (start = datetime(2014,1,1), end = None)
    nomerges = SCMNomergesCondition()

    data = SCMActivityPersons (
        datasource = database,
        name = "list_authors", conditions = (period,nomerges))
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("Age (days since first activity) for each author.")
    age = activity.age(datetime(2012,1,1))
    print age.json()
    #---------------------------------
    print_banner("Idle (days since last activity) for each author.")
    idle = activity.idle(datetime(2012,1,1))
    print idle.json()

    #---------------------------------
    print_banner("List of activity for each committer (no merges, uid)")
    data = SCMActivityPersons (name = "list_ucommitters",
                               conditions = (nomerges,),
                               datasource = session)
    print data.activity()
    print data.activity() \
        .active(after = datetime(2014,1,1) - timedelta(days=183))
    print data.activity() \
        .active(after = datetime(2014,1,1) - timedelta(days=183)) \
        .age(datetime(2014,1,1)).json()
    
    # ITS database
    database = ITSDatabase (url = "mysql://jgb:XXX@localhost/",
                            schema = "vizgrimoire_bicho",
                            schema_id = "vizgrimoire_cvsanaly")

    #---------------------------------
    print_banner("List of activity for each changer")
    data = ITSActivityPersons (
        datasource = database,
        name = "list_changers")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each changer, with period condition")
    period = ITSPeriodCondition (start = datetime(2014,1,1),
                                             end = None)
    data = ITSActivityPersons (
        datasource = database,
        name = "list_changers",
        conditions = (period,))
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each changer (unique ids)")
    data = ITSActivityPersons (
        datasource = database,
        name = "list_uchangers")
    activity = data.activity()
    print activity

    # MLS database
    database = MLSDatabase (url = "mysql://jgb:XXX@localhost/",
                            schema = "oscon_openstack_mls",
                            schema_id = "oscon_openstack_scm")
    
    #---------------------------------
    print_banner("List of activity for each sender")
    data = MLSActivityPersons (
        datasource = database,
        name = "list_senders")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each sender (unique ids)")
    data = MLSActivityPersons (
        datasource = database,
        name = "list_usenders")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each sender (unique ids, cached)")
    activity = data.activity()
    print activity

