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
## Package to deal with Demography from *Grimoire (CVSAnalY, Bicho, MLStats
## databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from scm_query import buildSession, SCMQuery
import its_query
import mls_query
from scm import PeriodCondition, NomergesCondition
import its_conditions
from sqlalchemy.orm.session import Session

class ActivityPersons:
    """High level interface to variables related to activity of persons.

    Root of ahierarchy of classes for the different data sources
    (SCM, ITS, MLS, etc.).

    Objects of this class are instantiated with a SQLAlchemy url
    (or a SQLAlchemy session), a variable to be obtained from it,
    and a list of conditions.
    
    Objects of this class provide the function activity() to
    obtain an ActivityList object. From this ActivityList, ages or
    idle period for actors can be obtained.

    """

    def __init__ (self, var, conditions = (), 
                  session = None,
                  database = None, id_database = None,
                  echo = False):
        """Instantiation of the object.

        Instantiation can be specified with an SQLAlchemy url or
        with an SQLAlchemy session.
        Uses _produce_query(), which should be produced by child classes.
 
        Parameters
        ----------
        
        var: {"list_authors" | "list_committers" |
           "list_uauthors" | "list_ucommitters"}
           Variable
        conditions: list of Condition objects
           Conditions to be applied to get the values
        session: sqlalchemy.orm.session.Session
           SQLAlchemy session
        database: string
           SQLAlchemy url of the database to work with (default: "")
        id_database: string
           SQLAlchemy url of the identities database (default: same as database)
        echo: boolean
           Write SQL queries to output stream
        """

        if session is not None:
            self.session = session
        elif database is not None:
            self.session = buildSession(
                database=database, id_database=id_database,
                echo=echo)
        else:
            raise Exception ("ActivityPersons: Either a session or a " + \
                                 "database must be specified")
        self.query = self.session.query()
        self._produce_query(var)
        for condition in conditions:
            self.query = condition.filter(self.query)


class SCMActivityPersons (ActivityPersons):
    """Interface to variables related to activity of persons in SCM.
    

    """

    def _produce_query (self, var):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        var: {"list_authors" | "list_committers" |
           "list_uauthors" | "list_ucommitters"}
           Variable

        """

        if var in ("list_authors", "list_uauthors"):
            persons = "authors"
        elif var in ("list_committers", "list_ucommitters"):
            persons = "committers"
        else:
            raise Exception ("ActivityPersons: Unknown variable %s." % var)
        if var in ("list_authors", "list_committers"):
            self.query = self.query.select_personsdata(persons)
        elif var in ("list_uauthors", "list_ucommitters"):
            self.query = self.query.select_personsdata_uid(persons)
        else:
            raise Exception ("ActivityPersons: Unknown variable %s." % var)
        self.query = self.query \
            .select_commitsperiod() \
            .group_by_person()


    def activity (self):
        """Obtain the activity list (ActivityList object).

        Extracts the activity list by querying the database
        according to the initialization of the object.
        
        Returns
        -------

        ActivityList: activity list for all actors

        """

        return self.query.activity()

class ITSActivityPersons (SCMActivityPersons):
    """Interface to variables related to activity of persons in ITS
    
    """

    def __init__ (self, var, conditions = (), 
                  session = None,
                  database = None, schema = None, schema_id = None,
                  echo = False):
        """Instantiation of the object.

        Instantiation can be specified with an SQLAlchemy url or
        with an SQLAlchemy session.

        Parameters
        ----------
        
        var: {"list_changers"}
           Variable
        conditions: list of Condition objects
           Conditions to be applied to get the values
        session: sqlalchemy.orm.session.Session
           SQLAlchemy session
        database: string
           SQLAlchemy url for the database to be used, such as
           mysql://user:passwd@host:port/
        schema: string
           Schema name for the ITS data
        schema_id: string
           Schema name for the unique ids data
        echo: boolean
           Write SQL queries to output stream
        """

        if session is not None:
            self.session = session
        elif database is not None:
            if schema is None or schema_id is None:
                raise Exception ("ITSActivityPersons: if database is a " + \
                                     "parameter, both schema and schema_id " + \
                                     "should be parameters too.")
            ITSDB = its_query.ITSDatabase (database = database,
                                           schema = schema,
                                           schema_id = schema_id)
            self.session = ITSDB.build_session(echo=echo)
        else:
            raise Exception ("ITSActivityPersons: Either a session or a " + \
                                 "database must be specified")
        self._produce_query(var)
        for condition in conditions:
            self.query = condition.filter(self.query)


    def _produce_query (self, var):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        var: {"list_changers" | "list_uchangers"}
           Variable

        """

        if var in ("list_changers", "list_uchangers"):
            persons = "changers"
        else:
            raise Exception ("ITSActivityPersons: Unknown variable %s." % var)
        self.query = self.session.query()
        if var in ("list_changers"):
            self.query = self.query.select_personsdata(persons)
        elif var in ("list_uchangers"):
            self.query = self.query.select_personsdata_uid(persons)
        else:
            raise Exception ("ITSActivityPersons: Unknown variable %s." % var)
        self.query = self.query \
            .select_changesperiod() \
            .group_by_person()


class MLSActivityPersons (SCMActivityPersons):
    """Interface to variables related to activity of persons in ITS
    
    """

    def __init__ (self, var, conditions = (), 
                  session = None,
                  database = None, schema = None, schema_id = None,
                  echo = False):
        """Instantiation of the object.

        Instantiation can be specified with an SQLAlchemy url or
        with an SQLAlchemy session.

        Parameters
        ----------
        
        var: {"list_changers"}
           Variable
        conditions: list of Condition objects
           Conditions to be applied to get the values
        session: sqlalchemy.orm.session.Session
           SQLAlchemy session
        database: string
           SQLAlchemy url for the database to be used, such as
           mysql://user:passwd@host:port/
        schema: string
           Schema name for the ITS data
        schema_id: string
           Schema name for the unique ids data
        echo: boolean
           Write SQL queries to output stream
        """

        if session is not None:
            self.session = session
        elif database is not None:
            if schema is None or schema_id is None:
                raise Exception ("MLSActivityPersons: if database is a " + \
                                     "parameter, both schema and schema_id " + \
                                     "should be parameters too.")
            MLSDB = mls_query.MLSDatabase (database = database,
                                           schema = schema,
                                           schema_id = schema_id)
            self.session = MLSDB.build_session(echo=echo)
        else:
            raise Exception ("MLSActivityPersons: Either a session or a " + \
                                 "database must be specified")
        self._produce_query(var)
        for condition in conditions:
            self.query = condition.filter(self.query)


    def _produce_query (self, var):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        var: {"list_senders" | "list_usenders"}
           Variable

        """

        if var in ("list_senders", "list_usenders"):
            persons = "senders"
        else:
            raise Exception ("MLSActivityPersons: Unknown variable %s." % var)
        self.query = self.session.query()
        if var in ("list_senderss"):
            self.query = self.query.select_personsdata(persons)
        elif var in ("list_usenders"):
            self.query = self.query.select_personsdata_uid(persons)
        else:
            raise Exception ("MLSActivityPersons: Unknown variable %s." % var)
        self.query = self.query \
            .select_activeperiod() \
            .group_by_person()


class DurationCondition ():
    """Root of all conditions for DurationPersons objects

    Provides a filter method which will be called when applying
    the condition.
    """

    def filter (object):
        """Filter to apply for this condition

        - query: query to which the filter will be applied
        """

        return object

class SnapshotCondition (DurationCondition):
    """Condition for specifiying "origin" of durations.

    Durations (age, idle) are to be calculated from the time
    specified by this condition.

    """

    def __init__ (self, date):
        """Instatiation of the object.

        Parameters
        ----------

        date: datetime.datetime
           Time used as reference for the snapshot.

        """

        self.date = date

    def modify (self, object):
        """Modification for this condition.

        Specifies the time for the snapshot.

        """

        object.set_snapshot(self.date)


class ActiveCondition (DurationCondition):
    """Condition for filtering persons active during a period

    Only persons active during the specified period are to
    be considered.

    """

    def __init__ (self, after = None, before = None):
        """Instatiation of the object.

        Parameters
        ----------

        after: datetime.datetime
           Start of the activity period to consider (default: None).
           None means "since the begining of time"
        before: datetime.datetime
           End of the activity period to consider (default: None).
           None means "until the end of time

        """

        self.after = after
        self.before = before

    def modify (self, object):
        """Modification for this condition.

        Sets the new activity list, considering only active
        persons during the period.

        """

        object.set_activity(object.activity.active(after = self.after,
                                                   before = self.before))
    

class DurationPersons:
    """High level interface to variables related to duration of persons.
    
    Duration can be different periods, such as age or idle time.
    Objects of this class are instantiated with an ActivityPersons
    object, and some relevant dates.
    
    Objects of this class provide the functions durations() to
    obtain an ActorsDuration object.

    """

    def __init__ (self, var, activity, conditions = ()):
        """Instantiation of the object.

        Instantiation can be specified with an ActivityPersons object.

        Parameters
        ----------
        
        var: {"age" | "idle"}
           Variable
        conditions: list of DurationCondition objects
           Conditions to be applied to get the values
        activity: ActivityPersons
           ActivityPersons object with the activity of persons to consider.

        """

        self.activity = activity
        if var not in ("age", "idle"):
            raise Exception ("Not a valid variable: " + self.var)
        self.var = var
        self.snapshot = None
        for condition in conditions:
            condition.modify(self)

    def set_snapshot (self, time):
        """Define a snapshot time for durations.

        Durations (age, idle) are to be calculated from the time
        specified.

        Parameters
        ----------

        time: datetime.datetime
           Time of snapshot.
        """

        self.snapshot = time

    def set_activity (self, activity):
        """Change the activity considered by the object.

        Paraeters
        ---------

        activity: ActivityPersons
           New list of activity per person to be used.

        """

        self.activity = activity

    def durations (self):
       """Durations for each person (age, idle,...) depending on variable

       """

       if self.snapshot is None:
           snapshot = self.activity.maxend()
       else:
           snapshot = self.snapshot
       if self.var == "age":
           durations = self.activity.age(date = snapshot)
       elif self.var == "idle":
           durations = self.activity.idle(date = snapshot)
       return durations

if __name__ == "__main__":

    from standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta

    stdout_utf8()

    #---------------------------------
    print_banner("List of activity for each author")
    data = SCMActivityPersons (
        database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        var = "list_authors")
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
    period = PeriodCondition (start = datetime(2014,1,1), end = None)
    nomerges = NomergesCondition()

    data = SCMActivityPersons (
        database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        var = "list_authors", conditions = (period,nomerges))
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
    session = buildSession(
        database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        echo = False)
    data = SCMActivityPersons (var = "list_ucommitters",
                            conditions = (nomerges,),
                            session = session)
    print data.activity()
    print data.activity() \
        .active(after = datetime(2014,1,1) - timedelta(days=183))
    print data.activity() \
        .active(after = datetime(2014,1,1) - timedelta(days=183)) \
        .age(datetime(2014,1,1)).json()

    #---------------------------------
    print_banner("Age, using variables")
    age = DurationPersons (var = "age",
                           activity = data.activity())
    print age.durations().json()

    #---------------------------------
    print_banner("Age, using variables and conditions")
    snapshot = SnapshotCondition (date = datetime (2014,1,1))
    active_period = ActiveCondition (after = datetime(2014,1,1) - \
                                         timedelta(days=10))
    age = DurationPersons (var = "age",
                           conditions = (snapshot, active_period),
                           activity = data.activity())
    print age.durations().json()
    
    #---------------------------------
    print_banner("List of activity for each changer")
    data = ITSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'vizgrimoire_bicho',
        schema_id = 'vizgrimoire_cvsanaly',
        var = "list_changers")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each changer, with period condition")
    period = its_conditions.PeriodCondition (start = datetime(2014,1,1),
                                             end = None)
    data = ITSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'vizgrimoire_bicho',
        schema_id = 'vizgrimoire_cvsanaly',
        var = "list_changers",
        conditions = (period,))
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each changer (unique ids)")
    data = ITSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'vizgrimoire_bicho',
        schema_id = 'vizgrimoire_cvsanaly',
        var = "list_uchangers")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each sender")
    data = MLSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'oscon_openstack_mls',
        schema_id = 'oscon_openstack_scm',
        var = "list_senders")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("List of activity for each sender (unique ids)")
    data = MLSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'oscon_openstack_mls',
        schema_id = 'oscon_openstack_scm',
        var = "list_usenders")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("MLS: Birth")
    data = MLSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'oscon_openstack_mls',
        schema_id = 'oscon_openstack_scm',
        var = "list_usenders")
    # Birth has the ages of all actors, consiering enddate as
    # current (snapshot) time
    enddate = datetime(2014,7,1)
    snapshot = SnapshotCondition (date = enddate)
    birth = DurationPersons (var = "age",
                             conditions = (snapshot,),
                             activity = data.activity())
    print birth

    #---------------------------------
    print_banner("MLS: Aging")
    data = MLSActivityPersons (
        database = 'mysql://jgb:XXX@localhost/',
        schema = 'oscon_openstack_mls',
        schema_id = 'oscon_openstack_scm',
        var = "list_usenders")
    # "Aging" has the ages of those actors active during the 
    # last half year (that is, the period from enddate - half year
    # to enddate)
    active_period = ActiveCondition (after = enddate - \
                                         timedelta(days=182))
    aging = DurationPersons (var = "age",
                             conditions = (snapshot, active_period),
                             activity = data.activity())
    print aging


    #---------------------------------
    # print_banner("List of activity for each committer (OpenStack)")
    # session = buildSession(
    #     database = 'mysql://jgb:XXX@localhost/openstack_cvsanaly_2014-06-06',
    #     echo = False)
    # data = ActivityPersons (var = "list_committers",
    #                         session = session)
    # print data.activity()
    # #---------------------------------
    # print_banner("Age for each committer (OpenStack)")
    # print data.activity().age(datetime(2014,6,6)).json()
    # #---------------------------------
    # print_banner("Time idle for each committer (OpenStack)")
    # print data.activity().idle(datetime(2014,6,6)).json()
    # #---------------------------------
    # print_banner("Age for committers active during a period (OpenStack)")
    # print data.activity() \
    #     .active(after = datetime(2014,6,6) - timedelta(days=180)) \
    #     .age(datetime(2014,6,6)).json()
