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
#     Alvaro del Castillor <acs@bitergia.com>
#

""" Companies activity  """

import time

from analyses import Analyses
from query_builder import DSQuery
from metrics_filter import MetricFilters
from GrimoireUtils import createJSON
from SCM import SCM
from report import Report

class CompaniesActivity(Analyses):
    id = "companies_activity"
    name = "Companies Activity"
    desc = "Companies activity for different periods."

    def get_sql(self, year = None):
        where = ""
        field = "commits"

        if year is not None:
            where = " WHERE YEAR(s.date) = " + str(year)
            field = field + "_" + str(year)
        sql = """
            select c.name, count(c.id) as %s
            from scmlog s 
              join people_upeople pup ON s.author_id = pup.people_id 
              JOIN upeople_companies upc ON upc.upeople_id = pup.upeople_id 
              JOIN companies c ON c.id = upc.company_id 
              JOIN people p ON p.id = s.author_id
            %s
            group by c.id
            order by %s desc
        """ % (field, where, field)

        return (sql)

    def create_report(self, data_source, destdir):
        if data_source != SCM: return
        self.result(data_source, destdir)

    def add_companies_data (self, activity, data):
        """ Add companies data in an already existing complete companies activity dictionary """
        new_activity = []
        field = None
        # Find the name of the field to be uses to get values
        for key in data.keys():
            if key != "name": 
                field = key
                break

        for company in activity['name']:
            if company in data['name']:
                index = data['name'].index(company)
                value = data[field][index]
                new_activity.append(value)
            else:
                new_activity.append(0)

        activity[field] = new_activity
        return activity

    def result(self, data_source, destdir = None):
        if data_source != SCM or destdir is None: return None

        automator = Report.get_config()
        start_date = automator['r']['start_date']
        if 'end_date' not in automator['r']:
            end_date = time.strftime('%Y-%m-%d')
        else:
            end_date = automator['r']['end_date']

        start_year = int(start_date.split("-")[0])
        end_year = int(end_date.split("-")[0])

        # First activity should include all companies name
        activity = self.db.ExecuteQuery(self.get_sql())
        for i in range(start_year, end_year+1):
            data = self.db.ExecuteQuery(self.get_sql(i))
            activity = self.add_companies_data (activity, data)


        # activity = dict (activity.keys() + data.keys())
        createJSON(data, destdir+"/scm-companies-activity.json")

    def get_report_files(self, data_source = None):
        return ["scm-companies-activity.json"]