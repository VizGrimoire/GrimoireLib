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
#     Alvaro del Castillo <acs@bitergia.com>
#     Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#

from vizgrimoire.analysis.analyses import Analyses

from vizgrimoire.metrics.query_builder import SCMQuery

from vizgrimoire.metrics.metrics_filter import MetricFilters

class TopCompaniesProjects(Analyses):
    # this class provides a list of top organizations
    # by number of commits

    id = "toporganizations"
    name = "Top Organizations"
    desc = "Top organizations committing changes to the source code"

    def __get_sql__(self):
        return ""

    def result(self, data_source = None):
        project = self.filters.type_analysis[1]

        projects_from = self.db.GetSQLProjectFrom()
        # Remove first and
        projects_where = " WHERE  " + self.db.GetSQLProjectWhere(project)[3:]

        fields =  "SELECT COUNT(DISTINCT(s.id)) as company_commits, c.name as companies "
        fields += "FROM actions a, scmlog s, people_upeople pup, upeople u, upeople_companies upc, companies c "
        q = fields + projects_from + projects_where
        q += " AND pup.people_id = s.author_id AND u.id = pup.upeople_id "
        q += " AND u.id = upc.upeople_id AND c.id = upc.company_id "
        q += " AND s.date >= upc.init and s.date < upc.end "
        q += " AND s.date>=" + self.filters.startdate + " and s.date < " + self.filters.enddate
        q += " AND a.commit_id = s.id "
        q += " GROUP by c.name ORDER BY company_commits DESC, c.name"
        q += " limit " + str(self.filters.npeople)

        res = self.db.ExecuteQuery(q)

        return res



if __name__ == '__main__':
    #example using this class
    filters = MetricFilters("week", "'2014-01-01'", "'2014-04-01'", ["project", "integrated"])
    dbcon = SCMQuery("root", "", "dic_cvsanaly_openstack_2259", "dic_cvsanaly_openstack_2259")
    top_orgs = TopCompaniesProjects(dbcon, filters)
    print top_orgs.result()

    #example using query_builder function
    from vizgrimoire.metrics.query_builder import SCMQuery
    dbcon = SCMQuery("root", "", "dic_cvsanaly_openstack_2259", "dic_cvsanaly_openstack_2259")
    print dbcon.get_project_top_companies("integrated", "'2014-01-01'", "'2014-04-01'", 10)
