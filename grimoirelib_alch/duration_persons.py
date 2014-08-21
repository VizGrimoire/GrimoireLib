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
## Module to deal with entities related to the duration of persons in
##  a project, based on information found in the *Grimoire (CVSAnalY,
##  Bicho, MLStats) databases.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from common import Family
from common import Condition as RootCondition
from activity import ActivityList
from activity_persons import ActivityPersons

class DurationPersons (Family):
    """Factory of entities in the DurationPersons family.

    This class can be used to instantiate entities from the DurationPersons
    family: those related to duration of persons.
    
    Duration can be periods of different kinds, such as age or idle time.
    
    Objects of this class provide the function durations() to
    obtain an ActorsDuration object.

    """

    def __init__ (self, datasource, name, conditions = ()):
        """Instantiation of an entity of the family.

        An ActivityPersons object is used as datasource.

        The following entities can be produced:

        "age": Age of persons (time from first activity to snaphsot)

        "idle": Idle time (time from last activity to snapshot)

        Snapshot is "last activity in datasource" if none is specified,
        or the date specified by a SnapshotCondition.

        Parameters
        ----------
        
        datasource: { activity_persons.ActivityPersons | activity.ActivityList }
           Object with the activity of persons to consider.
           If it is ActivityPersons, object.activity() will invoked to
           obtain the ActivityList.
        name: {"age" | "idle"}
           Enitity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).

        """

        if isinstance (datasource, ActivityPersons):
            self.activity = datasource.activity()
        elif isinstance (datasource, ActivityList):
            self.activity = datasource
        else:
            raise Exception ("DurationPersons: datasource must be of " + \
                                 "ActivityPersons class hierarchy.")
        if name not in ("age", "idle"):
            raise Exception ("DurationPersons: " + \
                                 "Invalid entity name for this family: " + \
                                 name)
        self.name = name
        self.snapshot = None
        for condition in conditions:
            condition.modify(self)

    def set_snapshot (self, time):
        """Define a snapshot time for durations.

        Durations (age, idle) are to be calculated back from the time
        specified. This can be used, for example, by conditions
        defininf a certain snapshot.

        Parameters
        ----------

        time: datetime.datetime
           Time of snapshot.
        """

        self.snapshot = time

    def set_activity (self, activity):
        """Change the activity list considered by the entity.

        This is used, for example, to apply a condition on the entity,
        such as "active before such and such date". For doing that
        kind of stuff, the activity list is changed.

        Parameters
        ---------

        activity: activity.ActivityList
           New list of activity per person to be used.

        """

        self.activity = activity

    def durations (self):
       """Return durations for each person (age, idle,...) depending on entity

       If there is no snapshot date, it considers the maximum date in the
       datasource as snapshot (date to count back ages).

       Returns
       -------

       activity.ActorsDuration: list of actors, with duration for each of them.

       """

       if self.snapshot is None:
           snapshot = self.activity.maxend()
       else:
           snapshot = self.snapshot
       if self.name == "age":
           durations = self.activity.age(date = snapshot)
       elif self.name == "idle":
           durations = self.activity.idle(date = snapshot)
       return durations


class Condition (RootCondition):
    """Root of all conditions specific for DurationPersons entities

    Provides a modify method, which will be called by the entity
    to let this condition modify it.

    """

    @staticmethod
    def _check_class (object):
        """Check that object is an instance of DurationPersons.

        Raise exception if it is not.

        """

        if not isinstance (object, DurationPersons):
            raise Exception ("Condition: " + \
                                 "This condition can only be applied to " + \
                                 "DurationPersons entities.")

    def modify (self, object):
        """Modification produced by this condition into object.

        Parameters
        ----------

        object: DurationPersons
           Entity to be modified.
        
        """

        Condition._check_class (object)
        print "DurationPersonsCondition: " + \
            "Root class, does nothing."

class SnapshotCondition (Condition):
    """Condition for specifiying "snapshot" of durations.

    Durations (age, idle) are to be calculated back from the date
    specified by this condition. If the snapshot date is as of now,
    this will be the real "age" or "idle time" in the project.

    """

    def __init__ (self, date):
        """Set the snapshot date.

        Parameters
        ----------

        date: datetime.datetime
           Time used as reference for the snapshot.

        """

        self.date = date

    def modify (self, object):
        """Modification produced by this condition into object.

        Specifies the time for the snapshot.

        """

        Condition._check_class (object)
        object.set_snapshot(self.date)


class ActiveCondition (Condition):
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
           None means "since the begining of time".
        before: datetime.datetime
           End of the activity period to consider (default: None).
           None means "until the end of time".

        """

        self.after = after
        self.before = before

    def modify (self, object):
        """Modification for this condition.

        Sets the new activity list, considering only active
        persons during the period.

        """

        Condition._check_class (object)
        object.set_activity(object.activity.active(after = self.after,
                                                   before = self.before))


if __name__ == "__main__":

    from standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta
    from scm_query import DB as SCMDatabase
    from mls_query import DB as MLSDatabase

    # from scm import SCMDatabaseDefinition
    # from mls import MLSDatabaseDefinition
    from scm import NomergesCondition as SCMNomergesCondition
    from activity_persons import SCMActivityPersons, MLSActivityPersons

    stdout_utf8()

    # SCM database
    database = SCMDatabase (url = "mysql://jgb:XXX@localhost/",
                            schema = "vizgrimoire_cvsanaly",
                            schema_id = "vizgrimoire_cvsanaly")
    session = database.build_session()

    #---------------------------------
    print_banner("Age, using entity")
    nomerges = SCMNomergesCondition()
    data = SCMActivityPersons (datasource = session,
                               name = "list_ucommitters",
                               conditions = (nomerges,))
    age = DurationPersons (datasource = data.activity(),
                           name = "age")
    print age.durations().json()

    #---------------------------------
    print_banner("Age, using entity and ActivityPersons as datasource")
    nomerges = SCMNomergesCondition()
    data = SCMActivityPersons (datasource = session,
                               name = "list_ucommitters",
                               conditions = (nomerges,))
    age = DurationPersons (name = "age",
                           datasource = data)
    print age.durations().json()

    #---------------------------------
    print_banner("Using (void) root condition.")
    condition = Condition ()
    age = DurationPersons (datasource = data.activity(),
                           name = "age",
                           conditions = (condition,))

    #---------------------------------
    print_banner("Age, using entity and conditions")
    snapshot = SnapshotCondition (date = datetime (2014,1,1))
    active_period = ActiveCondition (after = datetime(2014,1,1) - \
                                         timedelta(days=10))
    age = DurationPersons (datasource = data.activity(),
                           name = "age",
                           conditions = (snapshot, active_period))
    print age.durations().json()
    
    # MLS database
    database = MLSDatabase (url = "mysql://jgb:XXX@localhost/",
                            schema = "oscon_openstack_mls",
                            schema_id = "oscon_openstack_scm")
    data = MLSActivityPersons (
        datasource = database,
        name = "list_usenders")

    #---------------------------------
    print_banner("MLS: Birth")
    # Birth has the ages of all actors, considering enddate as
    # current (snapshot) time
    enddate = datetime(2014,7,1)
    snapshot = SnapshotCondition (date = enddate)
    birth = DurationPersons (datasource = data.activity(),
                             name = "age",
                             conditions = (snapshot,))
    print birth.durations()

    #---------------------------------
    print_banner("MLS: Aging")
    # "Aging" has the ages of those actors active during the 
    # last half year (that is, the period from enddate - half year
    # to enddate)
    active_period = ActiveCondition (after = enddate - \
                                         timedelta(days=182))
    aging = DurationPersons (datasource = data.activity(),
                             name = "age",
                             conditions = (snapshot, active_period))
    print aging.durations()
