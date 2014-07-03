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
#

from analyses import Analyses

from query_builder import SCMQuery

from metrics_filter import MetricFilters

class TopAuthorsProjects(Analyses):
    # this class provides a list of top contributors
    # A top contributor is defined in this function as the aggregation
    # of questions, comments and answers.

    id = "topauthors"
    name = "Top Authors"
    desc = "Top people committing changes to the source code"

    def __get_sql__(self):
        return ""

    def result(self, data_source = None):

        project = self.filters.type_analysis[1]
        projects_from = self.db.GetSQLProjectFrom()

        # Remove first and
        projects_where = " WHERE  " + self.db.GetSQLProjectWhere(project)[3:]

        fields =  "SELECT COUNT(DISTINCT(s.id)) as commits, u.id, u.identifier as authors "
        fields += "FROM scmlog s, people_upeople pup, upeople u "
        q = fields + projects_from + projects_where
        q += " AND pup.people_id = s.author_id AND u.id = pup.upeople_id "
        q += " GROUP by u.id ORDER BY commits DESC, u.id"
        q += " limit " + str(self.filters.npeople)

        res = self.db.ExecuteQuery(q)

        return res



if __name__ == '__main__':
    #example using this class
    filters = MetricFilters("week", "'2014-01-01'", "'2014-04-01'", ["project", "integrated"])
    dbcon = SCMQuery("root", "", "dic_cvsanaly_openstack_2259", "dic_cvsanaly_openstack_2259")
    top_authors = TopAuthorsProjects(dbcon, filters)
    print top_authors.result()

    #example using query_builder function
    from query_builder import SCMQuery
    dbcon = SCMQuery("root", "", "dic_cvsanaly_openstack_2259", "dic_cvsanaly_openstack_2259")
    print dbcon.get_project_top_authors("integrated", "'2014-01-01'", "'2014-04-01'", 10)
