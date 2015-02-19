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

from vizgrimoire.analysis.analyses import Analyses
from vizgrimoire.metrics.query_builder import DSQuery
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.GrimoireUtils import createJSON
import vizgrimoire.GrimoireSQL
from vizgrimoire.SCM import SCM
from vizgrimoire.ITS import ITS
from vizgrimoire.MLS import MLS
from vizgrimoire.report import Report
import logging

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

    def get_scm_from_companies(self, committers = False):
        if (committers): field = "s.committer_id"
        else:  field = "s.author_id"
        from_ = """
            FROM scmlog s 
              JOIN people_upeople pup ON %s = pup.people_id
              JOIN upeople_companies upc ON upc.upeople_id = pup.upeople_id 
              JOIN companies c ON c.id = upc.company_id 
        """ % (field)
        return from_

    def get_sql_commits(self, year = None):
        where = ""
        field = "commits"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where = " WHERE YEAR(s.author_date) = " + str(year)
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
            where = " WHERE YEAR(s.author_date) = " + str(year)
            field = field + "_" + str(year)
        sql = """
            select c.name, count(distinct(s.author_id)) as %s
            %s %s
            group by c.id
            order by %s desc, c.name
        """ % (field, from_, where, field)

        return (sql)

    def get_sql_committers(self, year = None, active = False):
        if year is not None and active:
            logging.error("Active committers is not valid for past years.")
            return None
        # An active committers has done a commit in last 90 days
        active_max_days = "90"
        where = ""
        field = "committers"
        if active: field = "committers_active"
        from_ =  self.get_scm_from_companies(True)

        if year is not None:
            where = " WHERE YEAR(s.author_date) = " + str(year)
            field = field + "_" + str(year)
        if active:
	    if where == "": where = " WHERE "
  	    else: where += " AND "
            where += " DATEDIFF(NOW(), s.author_date) < " + active_max_days
        sql = """
            select c.name, count(distinct(s.committer_id)) as %s
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
            where = " WHERE YEAR(s.author_date) = " + str(year)
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

    def get_sql_sloc(self, year = None):
        """ Metric not used. Use lines_added and lines_removed """
        where = ""
        field = "sloc"
        from_ =  self.get_scm_from_companies()

        # Remove commits from cvs2svn migration with removed lines issues
        where = "WHERE  message NOT LIKE '%cvs2svn%'"
        if year is not None:
            where += " AND YEAR(s.author_date) = " + str(year)
            field = field + "_" + str(year)

        sql = """
            select name, added-removed as %s FROM (
              select c.name, SUM(removed) as removed, SUM(added) as added
              %s JOIN commits_lines cl ON cl.commit_id = s.id
              %s
              group by c.id
            ) t
            order by %s desc, name
        """ % (field, from_, where, field)

        return (sql)


    def get_lines_filters(self):
        # Remove commits from cvs2svn migration with removed lines issues
        where = "WHERE  message NOT LIKE '%cvs2svn%'"
        return where

    def get_sql_lines_added(self, year = None):
        where = self.get_lines_filters()
        field = "lines_added"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where += " AND YEAR(s.author_date) = " + str(year)
            field = field + "_" + str(year)

        sql = """
            select name, added as %s FROM (
              select c.name, SUM(added) as added
              %s JOIN commits_lines cl ON cl.commit_id = s.id
              %s
              group by c.id
            ) t
            order by %s desc, name
        """ % (field, from_, where, field)

        return (sql)

    def get_sql_lines_removed(self, year = None):
        where = self.get_lines_filters()
        field = "lines_removed"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where += " AND YEAR(s.author_date) = " + str(year)
            field = field + "_" + str(year)

        sql = """
            select name, removed as %s FROM (
              select c.name, SUM(removed) as removed
              %s JOIN commits_lines cl ON cl.commit_id = s.id
              %s
              group by c.id
            ) t
            order by %s desc, name
        """ % (field, from_, where, field)

        return (sql)

    def get_sql_lines_total(self, year = None):
        where = self.get_lines_filters()
        field = "lines_total"
        from_ =  self.get_scm_from_companies()

        if year is not None:
            where += " AND YEAR(s.author_date) = " + str(year)
            field = field + "_" + str(year)

        sql = """
            select name, (added+removed) as %s FROM (
              select c.name, SUM(removed) as removed, SUM(added) as added
              %s JOIN commits_lines cl ON cl.commit_id = s.id
              %s
              group by c.id
            ) t
            order by %s desc, name
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
        metrics = ['commits','authors','committers','actions','sloc',
                   'lines-total','lines-added','lines-removed','opened','closed','sent']
        if metric not in metrics:
            logging.error(metric + " not supported in companies activity.")
            return
        for i in range(start, end+1):
            if metric == "commits":
                data = self.db.ExecuteQuery(self.get_sql_commits(i))
            elif metric == "authors":
                data = self.db.ExecuteQuery(self.get_sql_authors(i))
            elif metric == "committers":
                data = self.db.ExecuteQuery(self.get_sql_committers(i))
            elif metric == "actions":
                data = self.db.ExecuteQuery(self.get_sql_actions(i))
            elif metric == "sloc":
                data = self.db.ExecuteQuery(self.get_sql_sloc(i))
            elif metric == "lines-added":
                data = self.db.ExecuteQuery(self.get_sql_lines_added(i))
                data = self._convert_dict_field(data, "lines_added","lines-added")
            elif metric == "lines-removed":
                data = self.db.ExecuteQuery(self.get_sql_lines_removed(i))
                data = self._convert_dict_field(data, "lines_removed","lines-removed")
            elif metric == "lines-total":
                data = self.db.ExecuteQuery(self.get_sql_lines_total(i))
                data = self._convert_dict_field(data, "lines_total","lines-total")
            elif metric == "opened":
                data = self.db.ExecuteQuery(self.get_sql_opened(i))
            elif metric == "closed":
                data = self.db.ExecuteQuery(self.get_sql_closed(i))
            elif metric == "sent":
                data = self.db.ExecuteQuery(self.get_sql_sent(i))
            self.check_array_values(data)
            activity = self.add_companies_data (activity, data)


    def _convert_dict_field(self, dict, str_old, str_new):
        """ Change field dict names replacing str_old with str_new in the field names"""
        for key in dict.keys():
            if str_old in key:
                new_key = key.replace(str_old, str_new)
                dict[new_key] = dict.pop(key)
        return dict

    def add_metric_lines_commit(self, activity, start, end):
        # Global
        activity['lines-per-commit'] = []
        for i in range(0, len(activity['commits'])):
            if activity['commits'][i] == 0:
                activity['lines-per-commit'].append(None)
            else:
                activity['lines-per-commit'].append(\
                (activity['lines-total'][i]) / activity['commits'][i])
        # Per year
        for i in range(start, end+1):
            activity['lines-per-commit_'+str(i)] = []
            for j in range(0, len(activity['commits_'+str(i)])):
                if activity['commits_'+str(i)][j] == 0:
                    activity['lines-per-commit_'+str(i)].append(None)
                else:
                    activity['lines-per-commit_'+str(i)].append(\
                    (activity['lines-total_'+str(i)][j]) / activity['commits_'+str(i)][j])

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
        self.add_metric_years("commits", activity, start_year, end_year)
        # Authors
        data = self.db.ExecuteQuery(self.get_sql_authors())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("authors", activity, start_year, end_year)
        # Committers
        data = self.db.ExecuteQuery(self.get_sql_committers())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("committers", activity, start_year, end_year)
        # Committers active: only valid for today
        data = self.db.ExecuteQuery(self.get_sql_committers(None, True))
        data = self._convert_dict_field(data, "committers_active","committers-active")
        activity = self.add_companies_data (activity, data)
        # Committers inactive: only valid for today
        activity['committers-inactive'] = \
            [ activity['committers'][i] - activity['committers-active'][i] \
             for i in range(0, len(activity['committers']))]
        activity['committers-percent-active'] = []
        for i in range(0, len(activity['committers'])):
            if activity['committers'][i] == 0:
                activity['committers-percent-active'].append(100)
            else:
                activity['committers-percent-active'].append(\
                (activity['committers-active'][i]*100) / activity['committers'][i])
        # Actions
        data = self.db.ExecuteQuery(self.get_sql_actions())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("actions", activity, start_year, end_year)
        # Source lines of code added
        data = self.db.ExecuteQuery(self.get_sql_lines_added())
        data = self._convert_dict_field(data, "lines_added", "lines-added")
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("lines-added", activity, start_year, end_year)
        # Source lines of code removed
        data = self.db.ExecuteQuery(self.get_sql_lines_removed())
        data = self._convert_dict_field(data, "lines_removed","lines-removed")
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("lines-removed", activity, start_year, end_year)
        # Source lines of code total (added+removed)
        data = self.db.ExecuteQuery(self.get_sql_lines_total())
        data = self._convert_dict_field(data, "lines_total","lines-total")
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("lines-total", activity, start_year, end_year)
        # Lines per commit
        self.add_metric_lines_commit(activity, start_year, end_year)

        # We need to change the db to tickets
        dbname = automator["generic"]["db_bicho"]
        dsquery = ITS.get_query_builder()
        dbcon = dsquery(dbuser, dbpass, dbname, db_identities)
        self.db = dbcon
        # Tickets opened
        data = self.db.ExecuteQuery(self.get_sql_opened())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("opened", activity, start_year, end_year)
        # Tickets closed
        data = self.db.ExecuteQuery(self.get_sql_closed())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("closed", activity, start_year, end_year)

        # Messages sent 
        dbname = automator["generic"]["db_mlstats"]
        dsquery = MLS.get_query_builder()
        dbcon = dsquery(dbuser, dbpass, dbname, db_identities)
        self.db = dbcon

        data = self.db.ExecuteQuery(self.get_sql_sent())
        activity = self.add_companies_data (activity, data)
        self.add_metric_years("sent", activity, start_year, end_year)

        createJSON(activity, destdir+"/companies-activity.json")
        logging.info(destdir+"/companies-activity.json created")

    def get_report_files(self, data_source = None):
        return ["companies-activity.json"]
