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

import logging, time

from analyses import Analyses
from query_builder import DSQuery
from metrics_filter import MetricFilters
from GrimoireUtils import createJSON
import GrimoireSQL
from SCM import SCM
from ITS import ITS
from MLS import MLS
from report import Report

class CompaniesActivity(Analyses):
    id = "companies_activity"
    name = "Companies Activity"
    desc = "Companies activity for different periods: commits, authors, actions ..."

    def get_mls_from_companies(self):
        automator = Report.get_config()
        identities_db = automator['generic']['db_identities']

        from_ = """
            FROM messages m
              JOIN messages_people mp ON m.message_ID = mp.message_id
              JOIN people_upeople pup ON mp.email_address = pup.people_id 
              JOIN %s.upeople_companies upc ON upc.upeople_id = pup.upeople_id 
              JOIN %s.companies c ON c.id = upc.company_id 
        """ % (identities_db, identities_db)
        return from_


    def get_its_from_companies(self):
        automator = Report.get_config()
        identities_db = automator['generic']['db_identities']

        from_ = """
            FROM issues i
              JOIN people_upeople pup ON i.submitted_by = pup.people_id 
              JOIN %s.upeople_companies upc ON upc.upeople_id = pup.upeople_id 
              JOIN %s.companies c ON c.id = upc.company_id 
        """ % (identities_db, identities_db)
        return from_

    def get_scm_from_companies(self):
        from_ = """
            FROM scmlog s 
              JOIN people_upeople pup ON s.author_id = pup.people_id 
              JOIN upeople_companies upc ON upc.upeople_id = pup.upeople_id 
              JOIN companies c ON c.id = upc.company_id 
        """
        return from_

    def get_sql_commits(self, year = None):
        where = ""
        field = "commits"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where = " WHERE YEAR(s.date) = " + str(year)
            field = field + "_" + str(year)
        sql = """
            select c.name, count(c.id) as %s
            %s %s
            group by c.id
            order by %s desc, c.name
        """ % (field, from_, where, field)

        return (sql)

    def get_sql_authors(self, year = None):
        where = ""
        field = "authors"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where = " WHERE YEAR(s.date) = " + str(year)
            field = field + "_" + str(year)
        sql = """
            select c.name, count(distinct(s.author_id)) as %s
            %s %s
            group by c.id
            order by %s desc, c.name
        """ % (field, from_, where, field)

        return (sql)

    def get_sql_actions(self, year = None):
        where = ""
        field = "actions"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where = " WHERE YEAR(s.date) = " + str(year)
            field = field + "_" + str(year)
        sql = """
            select c.name, count(a.id) as %s
            %s
              JOIN actions a ON a.commit_id = s.id 
            %s
            group by c.id
            order by %s desc, c.name
        """ % (field, from_, where, field)

        return (sql)

    def get_sql_tickets(self, field = None, year = None):
        where = "WHERE"
        from_ =  self.get_its_from_companies()
        if field == "opened":
            if year is None: where = ""
        elif field == "closed":
            where = "WHERE i.status='RESOLVED' "
        elif field == "pending":
            where = "WHERE i.status='NEW' "
        else: return

        if year is not None:
            if where != "WHERE": where += " AND "
            where += " YEAR(i.submitted_on) = " + str(year) 
            field = field + "_" + str(year)
        sql = """
            select c.name, count(distinct(i.id)) as %s
            %s %s
            group by c.id
            order by %s desc, c.name
        """ % (field, from_, where, field)
        return (sql)

    def get_sql_opened(self, year = None):
        return self.get_sql_tickets("opened", year)

    def get_sql_closed(self, year = None):
        return self.get_sql_tickets("closed", year)

    def get_sql_pending(self, year = None):
        return self.get_sql_tickets("pending", year)

    def get_sql_sent(self, year = None):
        where = ""
        field = "sent"
        from_ =  self.get_mls_from_companies()

        if year is not None:
            where = " WHERE YEAR(m.first_date) = " + str(year)
            field = field + "_" + str(year)

        sql = """
            select c.name, count(distinct(m.message_ID)) as %s
            %s %s
            group by c.id
            order by %s desc, c.name
        """ % (field, from_, where, field)
        return (sql)

    def create_report(self, data_source, destdir):
        if data_source != SCM: return
        self.result(data_source, destdir)

    def add_companies_data (self, activity, data):
        """ Add companies data in an already existing complete companies activity dictionary """
        new_activity = []
        field = None

        # Check all data names are already in activity. If not add it with zero value.
        for item in data['name']:
            if item not in activity['name']:
                activity['name'].append(item)
                for metric in activity:
                    if metric == "name": continue
                    activity[metric].append(0)

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

    def check_array_values(self, data):
        for item in data: 
            if not isinstance(data[item], list): data[item] = [data[item]]

    def add_metric_years(self, metric, activity, start, end):
        metrics = ['commits','authors','actions','opened','closed','sent']
        if metric not in metrics:
            logging.error(metric + " not supported in companies activity.")
            return
        for i in range(start, end+1):
            if metric == "commits":
                data = self.db.ExecuteQuery(self.get_sql_commits(i))
            elif metric == "authors":
                data = self.db.ExecuteQuery(self.get_sql_authors(i))
            elif metric == "actions":
                data = self.db.ExecuteQuery(self.get_sql_actions(i))
            elif metric == "opened":
                data = self.db.ExecuteQuery(self.get_sql_opened(i))
            elif metric == "closed":
                data = self.db.ExecuteQuery(self.get_sql_closed(i))
            elif metric == "sent":
                data = self.db.ExecuteQuery(self.get_sql_sent(i))
            self.check_array_values(data)
            activity = self.add_companies_data (activity, data)

    def result(self, data_source = None, destdir = None):
        if data_source != SCM or destdir is None: return None

        automator = Report.get_config()
        db_identities = automator["generic"]["db_identities"]
        dbuser = automator["generic"]["db_user"]
        dbpass = automator["generic"]["db_password"]

        start_date = automator['r']['start_date']
        if 'end_date' not in automator['r']:
            end_date = time.strftime('%Y-%m-%d')
        else:
            end_date = automator['r']['end_date']

        start_year = int(start_date.split("-")[0])
        end_year = int(end_date.split("-")[0])

        activity = {}
        activity['name'] = []
        # Commits
        data = self.db.ExecuteQuery(self.get_sql_commits())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("commits",activity,start_year,end_year)
        # Authors
        data = self.db.ExecuteQuery(self.get_sql_authors())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("authors",activity,start_year,end_year)
        # Actions
        data = self.db.ExecuteQuery(self.get_sql_actions())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("actions",activity,start_year,end_year)

        # We need to change the db to tickets
        dbname = automator["generic"]["db_bicho"]
        dsquery = ITS.get_query_builder()
        dbcon = dsquery(dbuser, dbpass, dbname, db_identities)
        self.db = dbcon
        # Tickets opened
        data = self.db.ExecuteQuery(self.get_sql_opened())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("opened",activity,start_year,end_year)
        # Tickets closed
        data = self.db.ExecuteQuery(self.get_sql_closed())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("closed",activity,start_year,end_year)

        # Messages sent 
        dbname = automator["generic"]["db_mlstats"]
        dsquery = MLS.get_query_builder()
        dbcon = dsquery(dbuser, dbpass, dbname, db_identities)
        self.db = dbcon

        data = self.db.ExecuteQuery(self.get_sql_sent())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("sent",activity,start_year,end_year)

        createJSON(activity, destdir+"/companies-activity.json")

    def get_report_files(self, data_source = None):
        return ["companies-activity.json"]