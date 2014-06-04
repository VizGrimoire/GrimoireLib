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

class ActivityPersons:
    """High level interface to variables related to demography studies.

    Tracks activity periods for persons.

    """

    def __init__ (self, database, var, conditions = (), echo = False):
        """Instantiation of the object.

        Parameters
        ----------
        
        database: string
           SQLAlchemy url of the database to work with
        var: {"list_authors" | "list_committers"}
           Variable
        conditions: list of Condition objects
           Conditions to be applied to get the values
        echo: boolean
           Write SQL queries to output stream
        """

        self.session = buildSession(
            database=database,
            echo=echo)
        if var == "list_authors":
            persons = "authors"
        elif var == "list_committers":
            persons = "committers"
        else:
            raise Exception ("ActivityPersons: Unknown variable %s." % var)
        self.query = self.session.query() \
            .select_personsdata(persons) \
            .select_commitsperiod() \
            .group_by_person()
        for condition in conditions:
            self.query = condition.filter(self.query)

    def activity (self):
        """Return an ActivityList for the specified variable"""

        return self.query.activity()

if __name__ == "__main__":

    from datetime import datetime
    import sys
    import codecs
    # Trick to make the script work when using pipes
    # (pipes confuse the interpreter, which sets codec to None)
    # http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    def print_banner (banner):
        """Print a simple banner for a kind of result"""

        print
        print "===================================="
        print banner
        print

    #---------------------------------
    print_banner("List of activity for each author")
    data = ActivityPersons (
        database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        var = "list_authors")
    activity = data.activity()
    print activity

    #---------------------------------
    print_banner("Age (days since first activity) for each author.")
    age = activity.get_age(datetime(2014,1,1))
    print age.json()
    #---------------------------------
    print_banner("Idle (days since last activity) for each author.")
    idle = activity.get_idle(datetime(2014,1,1))
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
    age = activity.get_age(datetime(2012,1,1))
    print age.json()
    #---------------------------------
    print_banner("Idle (days since last activity) for each author.")
    idle = activity.get_idle(datetime(2012,1,1))
    print idle.json()

