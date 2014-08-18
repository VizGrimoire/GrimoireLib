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

class DurationPersons (Family):
    """Constructor of entities in the DurationPersons family.

    This class can be used to instantiate entities from the DurationPersons
    family: those related to duration of persons.
    
    Duration can be periods of different kinds, such as age or idle time.
    
    Objects of this class provide the functions durations() to
    obtain an ActorsDuration object.

    """

    def __init__ (self, name, conditions = (), activity = None):
        """Instantiation of an entity of the family.

        Instantiation can be specified with an ActivityPersons object.

        Parameters
        ----------
        
        name: {"age" | "idle"}
           Enitity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).
        activity: ActivityPersons
           ActivityPersons object with the activity of persons to consider.

        """

        if activity is None:
            raise Exception ("DurationPersons: " + \
                                 "acitivity parameter is needed.")
        self.activity = activity
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

        Parameters
        ---------

        activity: ActivityPersons
           New list of activity per person to be used.

        """

        self.activity = activity

    def durations (self):
       """Durations for each person (age, idle,...) depending on entity

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


class Condition ():
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
            raise Exception ("DurationPersonsCondition: " + \
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
    from scm import SCMDatabaseDefinition
    from mls import MLSDatabaseDefinition
    from scm import NomergesCondition as SCMNomergesCondition
    from activity_persons import SCMActivityPersons, MLSActivityPersons

    stdout_utf8()

    # SCM database
    database = SCMDatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
                                      schema = "vizgrimoire_cvsanaly",
                                      schema_id = "vizgrimoire_cvsanaly")
    session = database.create_session()

    #---------------------------------
    print_banner("Age, using entity")
    nomerges = SCMNomergesCondition()
    data = SCMActivityPersons (name = "list_ucommitters",
                               conditions = (nomerges,),
                               datasource = session)
    age = DurationPersons (name = "age",
                           activity = data.activity())
    print age.durations().json()

    #---------------------------------
    print_banner("Using (void) root condition.")
    condition = Condition ()
    age = DurationPersons (name = "age",
                           conditions = (condition,),
                           activity = data.activity())

    #---------------------------------
    print_banner("Age, using entity and conditions")
    snapshot = SnapshotCondition (date = datetime (2014,1,1))
    active_period = ActiveCondition (after = datetime(2014,1,1) - \
                                         timedelta(days=10))
    age = DurationPersons (name = "age",
                           conditions = (snapshot, active_period),
                           activity = data.activity())
    print age.durations().json()
    
    # MLS database
    database = MLSDatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
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
    birth = DurationPersons (name = "age",
                             conditions = (snapshot,),
                             activity = data.activity())
    print birth.durations()

    #---------------------------------
    print_banner("MLS: Aging")
    # "Aging" has the ages of those actors active during the 
    # last half year (that is, the period from enddate - half year
    # to enddate)
    active_period = ActiveCondition (after = enddate - \
                                         timedelta(days=182))
    aging = DurationPersons (name = "age",
                             conditions = (snapshot, active_period),
                             activity = data.activity())
    print aging.durations()
