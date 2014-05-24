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
        var: {"listauthors" | "listcommitters"}
           Variable
        conditions: list of Condition objects
           Conditions to be applied to get the values
        echo: boolean
           Write SQL queries to output stream
        """

        self.session = buildSession(
            database=database,
            echo=echo)
        if var == "listauthors":
            persons = "authors"
        elif var == "listcommitters":
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

    data = ActivityPersons (
        database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        var = "listauthors")
    print data.activity()

