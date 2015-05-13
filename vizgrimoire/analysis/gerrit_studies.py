#!/usr/bin/env python

# Copyright (C) 2015 Bitergia
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

from vizgrimoire.metrics.query_builder import SCRQuery

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.SCR import SCR

from vizgrimoire.GrimoireUtils import completePeriodIds, createJSON


class OldestChangesets(Analyses):

    """ This class provides the oldest changesets without activity
        ordered by the last upload
    """

    id = "oldest_changesets"
    name = "The oldest changesets"
    desc = "The oldest changesets"
    data_source = SCR

    def result(self):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("tr.url as project_name")
        fields.add("p.name as author_name")
        fields.add("i.issue as gerrit_issue_id")
        fields.add("i.summary as summary")
        fields.add("i.submitted_on as first_upload")
        fields.add("t.last_upload as last_upload")

        tables.add("issues i")
        tables.add("trackers tr")
        tables.add("people p")
        tables.add("(select issue_id, max(changed_on) as last_upload from changes where field='status' and new_value='UPLOADED' group by issue_id) t")

        filters.add("t.issue_id = i.id")
        filters.add("i.id not in (select distinct(issue_id) from changes where field='Code-Review')")
        filters.add("i.status<>'Abandoned'")
        filters.add("i.status<>'Merged'")
        filters.add("tr.id=i.tracker_id")
        filters.add("i.submitted_by=p.id")

        query = " select " + self.db._get_fields_query(fields)
        query = query + " from " + self.db._get_tables_query(tables)
        query = query + " where " + self.db._get_filters_query(filters)

        query = query + " order by last_upload limit 100"

        data = self.db.ExecuteQuery(query)
        # TODO: Hardcoded creation of file
        #createJSON(data, "../../../../json/oldest_changesets.json")

        return data


if __name__ == '__main__':

    filters = MetricFilters("month", "'2011-04-01'", "'2016-01-01'")
    dbcon = SCRQuery("root", "", "openstack_2015q1_gerrit", "openstack_2015q1_git")

    oldest_changesets = OldestChangesets(dbcon, filters)
    print oldest_changesets.result()

