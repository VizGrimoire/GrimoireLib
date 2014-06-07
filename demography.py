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
from scm import PeriodCondition, NomergesCondition
from sqlalchemy.orm.session import Session

class ActivityPersons:
    """High level interface to variables related to demography studies.
    
    Objects of this class are instantiated with a SQLAlchemy url,
    a variable to be obtained from it, and a list of conditions.
    
    Objects of this class provide the function activity() to
    obtain an ActivityList object. From this ActivityList, ages or
    idle period for actors can be obtained.

    """

    def __init__ (self, var, conditions = (), 
                  session = None, database = None, echo = False):
        """Instantiation of the object.

        Instantiation can be specified with an SQLAlchemy url or
        with an SQLAlchemy session.

        Parameters
        ----------
        
        var: {"list_authors" | "list_committers"}
           Variable
        conditions: list of Condition objects
           Conditions to be applied to get the values
        session: sqlalchemy.orm.session.Session
           SQLAlchemy session
        database: string
           SQLAlchemy url of the database to work with (default: "")
        echo: boolean
           Write SQL queries to output stream
        """

        if session is not None:
            self.session = session
        elif database is not None:
            self.session = buildSession(
                database=database,
                echo=echo)
        else:
            raise Exception ("ActivityPersons: Either a session or a " + \
                                 "database must be specified")
        if var in ("list_authors", "list_uauthors"):
            persons = "authors"
        elif var in ("list_committers", "list_ucommitters"):
            persons = "committers"
        else:
            raise Exception ("ActivityPersons: Unknown variable %s." % var)
        self.query = self.session.query()
        if var in ("list_authors", "list_committers"):
            self.query = self.query.select_personsdata(persons)
        elif var in ("list_uauthors", "list_ucommitters"):
            self.query = self.query.select_personsdata_uid(persons)
        else:
            raise Exception ("ActivityPersons: Unknown variable %s." % var)
        self.query = self.query \
            .select_commitsperiod() \
            .group_by_person()
        for condition in conditions:
            self.query = condition.filter(self.query)

    def activity (self):
        """Obtain the activity list (ActivityList object).

        Extracts the activity list by querying the database
        according to the initialization of the object.
        
        Returns
        -------

        ActivityList: activity list for all actors

        """

        return self.query.activity()

if __name__ == "__main__":

    from standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta

    stdout_utf8()

    #---------------------------------
    print_banner("List of activity for each author")
    data = ActivityPersons (
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

    data = ActivityPersons (
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
    data = ActivityPersons (var = "list_ucommitters",
                            conditions = (period,nomerges),
                            session = session)
    print data.activity()

    #---------------------------------
    print_banner("List of activity for each committer (OpenStack)")
    session = buildSession(
        database = 'mysql://jgb:XXX@localhost/openstack_cvsanaly_2014-06-06',
        echo = False)
    data = ActivityPersons (var = "list_committers",
                            session = session)
    print data.activity()
    #---------------------------------
    print_banner("Age for each committer (OpenStack)")
    print data.activity().age(datetime(2014,6,6)).json()
    #---------------------------------
    print_banner("Time idle for each committer (OpenStack)")
    print data.activity().idle(datetime(2014,6,6)).json()
    #---------------------------------
    print_banner("Age for committers active during a period (OpenStack)")
    print data.activity() \
        .active(after = datetime(2014,6,6) - timedelta(days=180)) \
        .age(datetime(2014,6,6)).json()
