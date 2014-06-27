#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2014 Bitergia
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#       Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#
# Demography analysis. Age of developers in the project, age of
# developers still with activity, and so on.

from analyses import Analyses
from scm import PeriodCondition, NomergesCondition
import its_conditions
from demography import ActivityPersons, ActivityPersonsITS, DurationPersons, \
    SnapshotCondition, ActiveCondition
from datetime import datetime, timedelta

class Demography(Analyses):

    id = "demography"
    name = "Demography"
    desc = "Age of developers in project"

    def __get_sql__(self):
   
        raise NotImplementedError


    def result(self, data_source = None):

        # Prepare the SQLAlchemy database url
        database = 'mysql://' + self.db.user + ':' + \
            self.db.password + '@' + self.db.host + '/' + \
            self.db.database
        # Get startdate, endate as datetime objects
        startdate = datetime.strptime(self.filters.startdate, "'%Y-%m-%d'")
        enddate = datetime.strptime(self.filters.enddate, "'%Y-%m-%d'")
        # Activity data (start time, end time for contributions) for
        # all the actors, considering only actiivty during
        # the startdate..enddate period (merges are not considered
        # as activity)
        period = PeriodCondition (start = startdate, end = enddate)
        nomerges = NomergesCondition()
        data = ActivityPersons (
            database = database,
            var = "list_uauthors",
            conditions = (period,nomerges))
        # Birth has the ages of all actors, consiering enddate as
        # current (snapshot) time
        snapshot = SnapshotCondition (date = enddate)
        birth = DurationPersons (var = "age",
                                 conditions = (snapshot,),
                                 activity = data.activity())
        # "Aging" has the ages of those actors active during the 
        # last half year (that is, the period from enddate - half year
        # to enddate)
        active_period = ActiveCondition (after = enddate - \
                                             timedelta(days=182))
        aging = DurationPersons (var = "age",
                                 conditions = (snapshot, active_period),
                                 activity = data.activity())
        demos = {"birth": birth.durations(),
                 "aging": aging.durations()}
        return demos


class AgesITS(Ages):

    id = "ages_its"
    name = "Ages ITS"
    desc = "Age of developers in ITS repositories"

    def result(self, data_source = None):

        # Prepare the SQLAlchemy database url
        database = 'mysql://' + self.db.user + ':' + \
            self.db.password + '@' + self.db.host + '/' + \
            self.db.database
        id_database = 'mysql://' + self.db.user + ':' + \
            self.db.password + '@' + self.db.host + '/' + \
            self.db.identities_db
        # Get startdate, endate as datetime objects
        startdate = datetime.strptime(self.filters.startdate, "'%Y-%m-%d'")
        enddate = datetime.strptime(self.filters.enddate, "'%Y-%m-%d'")
        # Activity data (start time, end time for contributions) for
        # all the actors, considering only actiivty during
        # the startdate..enddate period (merges are not considered
        # as activity)
        period = its_conditions.PeriodCondition (start = startdate,
                                                 end = enddate)
        data = ActivityPersonsITS (
            database = database,
            id_database = id_database,
            var = "list_changers",
            conditions = (period,))
        # Birth has the ages of all actors, consiering enddate as
        # current (snapshot) time
        snapshot = SnapshotCondition (date = enddate)
        birth = DurationPersons (var = "age",
                                 conditions = (snapshot,),
                                 activity = data.activity())
        # "Aging" has the ages of those actors active during the 
        # last half year (that is, the period from enddate - half year
        # to enddate)
        active_period = ActiveCondition (after = enddate - \
                                             timedelta(days=182))
        aging = DurationPersons (var = "age",
                                 conditions = (snapshot, active_period),
                                 activity = data.activity())
        demos = {"birth": birth.durations(),
                 "aging": aging.durations()}
        return demos


if __name__ == '__main__':

    from query_builder import DSQuery
    from metrics_filter import MetricFilters
    from jsonpickle import encode, set_encoder_options

    # Get 
    filters = MetricFilters("months", "'2013-06-01'", "'2014-01-01'", [])
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "openstack_cvsanaly_2014-06-06",
                    identities_db = "openstack_cvsanaly_2014-06-06")
    dem = Demography(dbcon, filters)

    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(dem.result(), unpicklable=False)

    # Produce compact JSON output
    set_encoder_options('json', separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(dem.result(), unpicklable=False)

    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "vizgrimoire_bicho",
                    identities_db = "vizgrimoire_cvsanaly")
    ages = AgesITS(dbcon, filters)
    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(), unpicklable=False)
