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

from vizgrimoire.metrics.query_builder import ITSQuery

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.ITS import ITS

from vizgrimoire.GrimoireUtils import completePeriodIds, createJSON


class StatusChangers(Analyses):

    id = "status_changers"
    name = "Top people changing status"
    desc = "Top people changing the status of the tickets"
    data_source = ITS

    def result(self):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("pro.name as name")
        fields.add("new_value as state")
        fields.add("count(distinct(ch.id)) as changes")

        tables.add("issues i")
        tables.add("changes ch")
        tables.add("people_upeople pup")
        tables.add(self.db.identities_db + ".profile pro")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.add("ch.issue_id = i.id")
        filters.add("ch.field = 'Status'")
        filters.add("ch.changed_by = pup.people_id")
        filters.add("pup.uuid = pro.uuid")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " ch.changed_on ", fields,
                                   tables, filters, False, self.filters.type_analysis)

        query = query + " group by name, state "
        query = query + " order by state, count(distinct(ch.id)) desc, name "
        #print query
        data = self.db.ExecuteQuery(query)

        # TODO: Hardcoded creation of file
        #createJSON(data, "../../../../json/its-changers.json")

        return data

class StatusChanges(Analyses):

    id = "status_changes"
    name = "Changes per status"
    desc = "Changes per status"
    data_source = ITS

    def _sql(self, status):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(ch.id)) as changes")

        tables.add("issues i")
        tables.add("changes ch")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.add("ch.issue_id = i.id")
        filters.add("ch.field = 'Status'")
        filters.add("ch.new_value = '" + status + "'")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " ch.changed_on ", fields,
                                   tables, filters, True, self.filters.type_analysis)

        return query

    def result(self):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        query = """select distinct(new_value) as states
                   from changes
                   where field = 'Status' """
        states = self.db.ExecuteQuery(query)

        data = {}
        for state in states["states"]:
            query = self._sql(state)
            state_data = self.db.ExecuteQuery(query)
            state_data = completePeriodIds(state_data, self.filters.period,
                                           self.filters.startdate, self.filters.enddate)
            if not data:
                data = state_data
                data[state] = data["changes"]
                data.pop("changes") # remove not needed data
            else:
                data[state] = state_data["changes"]

        # TODO: Hardcoded creation of file
        #createJSON(data, "../../../../json/its-changes.json")

        return data

if __name__ == '__main__':

    filters = MetricFilters("month", "'2014-04-01'", "'2015-01-01'", [])
    dbcon = ITSQuery("root", "", "cp_bicho_cloudera", "cp_cvsanaly_cloudera")

    all_people_changing = StatusChangers(dbcon, filters)
    print all_people_changing.result()

    all_changes = StatusChanges(dbcon, filters)
    print all_changes.result()

