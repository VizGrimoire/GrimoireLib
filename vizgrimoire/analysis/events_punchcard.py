#!/usr/bin/env python

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
#
# Authors:
#     Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>

from sets import Set

from vizgrimoire.analysis.analyses import Analyses

from vizgrimoire.metrics.query_builder import EventizerQuery

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.EventsDS import EventsDS

from vizgrimoire.GrimoireUtils import completePeriodIds, createJSON

from copy import deepcopy


class PunchcardEvents(Analyses):

    id = "events_punchcard"
    name = "Punchcard of mean rsvps per event"
    desc = "Punchcard of mean rsvps per event"
    data_source = EventsDS

    def result(self):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        #subquery
        fields.add("gro.name as group_name")
        fields.add("dayofweek(eve.local_time) as weekday")
        fields.add("hour(eve.local_time) as hour")
        fields.add("eve.id as event_id")
        fields.add("count(rsvps.id) as rsvps")

        tables.add("groups gro")
        tables.add("events eve")
        tables.add("rsvps")

        filters.add("gro.id = eve.group_id")
        filters.add("eve.id = rsvps.event_id")

        subquery = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " eve.local_time ", fields,
                                   tables, filters, False, self.filters.type_analysis)

        subquery = subquery + " group by gro.name, hour(eve.local_time), eve.id "

        #query
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("t.group_name")
        fields.add("t.weekday")
        fields.add("t.hour")
        fields.add("avg(t.rsvps) as mean")

        tables.add("(" + subquery + ") t")

        fields_string = self.db._get_fields_query(fields)
        tables_string = self.db._get_tables_query(tables)

        query = "select " + fields_string
        query = query + " from " + tables_string
        query = query + " group by t.group_name, t.weekday, t.hour;"

        data = self.db.ExecuteQuery(query)

        punchcard = {}
        empty_week_structure = {}
        for weekday in range(1,8):
            empty_week_structure[weekday] = []
            for hour in range(0,24):
                empty_week_structure[weekday].append(0)
        week_structure = deepcopy(empty_week_structure)

        count = 0
        old_group = ""
        current_group = data["group_name"][0]
        for group in data["group_name"]:
            old_group = current_group
            current_group = group
            if current_group <> old_group:
                # we need to restart the process
                punchcard[old_group] = deepcopy(week_structure)
                # Init data structure
                week_structure = deepcopy(empty_week_structure)

            weekday = int(data["weekday"][count])
            hour = int(data["hour"][count])
            mean = float(data["mean"][count])
            week_structure[weekday][hour] = mean
            count = count + 1
            if count == len(data["group_name"]):
                # last loop
                punchcard[current_group] = deepcopy(week_structure)

        # TODO: Hardcoded creation of file
        #createJSON(punchcard, "../../../../json/eventizer-punchcard.json")
        return punchcard


if __name__ == '__main__':

    filters = MetricFilters("month", "'2013-04-01'", "'2016-01-01'")
    dbcon = EventizerQuery("root", "", "meetuptest_eventizer", "meetuptest_eventizer")

    punchcard = PunchcardEvents(dbcon, filters)
    print punchcard.result()
