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
#
# Code Contributors New and Gone from source code review system
# 

from vizgrimoire.analysis.analyses import Analyses
from vizgrimoire.metrics.query_builder import DSQuery
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.GrimoireUtils import createJSON, completePeriodIds

class ContributorsNewGone(Analyses):
    id = "contributors_new_gone"
    name = "ContributorsNewGone"
    desc = "Number of contributors new and gone in a project."

    def create_report(self, data_source, destdir):
        ds = data_source.get_name()
        if ds not in ['scm','scr']: return None

        if data_source.get_name() == "scr":
            study = ContributorsNewGoneSCR(self.db, self.filters)
        elif data_source.get_name() == "scm":
            study = ContributorsNewGoneSCM(self.db, self.filters)
        study.create_report(data_source, destdir)

    def result(self, data_source):
        ds = data_source.get_name()
        if ds not in ['scm','scr']: return None

        if data_source.get_name() == "scr":
            study = ContributorsNewGoneSCR(self.db, self.filters)
        elif data_source.get_name() == "scm":
            study = ContributorsNewGoneSCM(self.db, self.filters)
        return study.result(data_source)


    def get_report_files(self, data_source = None):
        if data_source is None: return []
        ds = data_source.get_name()
        if ds not in ['scm','scr']: return []

        return [ds+"-people-intake-evolutionary.json",
                "gone-people-activity-"+ds+"-evolutionary.json",
                "new-people-activity-"+ds+"-evolutionary.json",
                ds+"-code-contrib-gone.json",
                ds+"-code-contrib-new.json"]

class ContributorsNewGoneSCM(ContributorsNewGone):

    id = "contributors_new_gone_scm"
    name = "ContributorsNewGoneSCM"
    desc = "Number of contributors new and gone in a project in the code management system."

    # People Code Contrib New and Gone KPI
    def GetNewPeopleListSQL(self, period):
        filters = self.db.GetCommitsFiltered()
        if (filters != ""): filters  = " WHERE " + filters

        q_people = """
            SELECT author_id FROM (SELECT MIN(date) AS first, author_id
            FROM scmlog
            %s
            GROUP BY author_id
            HAVING DATEDIFF(NOW(), first) <= %s) plist """ % (filters, period)
        return q_people

    def GetGonePeopleListSQL(self,period):
        filters = self.db.GetCommitsFiltered()
        if (filters != ""): filters  = " WHERE " + filters

        q_people = """
            SELECT author_id FROM (SELECT MAX(date) AS last, author_id
            FROM scmlog
            %s
            GROUP BY author_id
            HAVING DATEDIFF(NOW(), last)>%s) plist """ % (filters, period)

        return q_people

    # Total commits for people in period
    def GetNewPeopleTotalListSQL(self,period, filters=""):
        scmlog_filters = self.db.GetCommitsFiltered()
        if (filters != ""):
            if scmlog_filters != "": filters += " AND " + scmlog_filters
        else: filters = scmlog_filters

        if (filters != ""): filters  = " WHERE " + filters
        q_total_period = """
            SELECT COUNT(id) as total, author_id, MIN(date) AS first
            FROM scmlog
            %s
            GROUP BY author_id
            HAVING DATEDIFF(NOW(), first) <= %s
            ORDER BY total
            """ % (filters, period)
        return q_total_period

    # Total commits for people in period
    def GetGonePeopleTotalListSQL(self,period, filters=""):
        scmlog_filters = self.db.GetCommitsFiltered()
        if (filters != ""):
            if scmlog_filters != "": filters += " AND " + scmlog_filters
        else: filters = scmlog_filters

        if (filters != ""): filters  = " WHERE " + filters
        q_total_period = """
            SELECT COUNT(id) as total, author_id, MAX(date) AS last
            FROM scmlog
            %s
            GROUP BY author_id
            HAVING DATEDIFF(NOW(), last)>%s
            ORDER BY total
            """ % (filters, period)

        return q_total_period

    # New/Gone people using period as analysis time frame
    def GetNewGoneAuthorsSQL(self,period, fields = "", tables = "", filters = "",
                                order_by = "", gone = False):

        q_people = self.GetNewPeopleListSQL(period)
        q_total_period = self.GetNewPeopleTotalListSQL(period)
        if (gone):
            q_people = self.GetGonePeopleListSQL(period)
            q_total_period = self.GetGonePeopleTotalListSQL(period)

        if (tables != ""): tables +=  ","
        if (filters != ""): filters  += " AND "
        if (self.db.GetCommitsFiltered() != ""): filters += self.db.GetCommitsFiltered() + " AND "
        if (fields != ""): fields  += ","
        if (order_by != ""): order_by  += ","

        newgone = "<=";
        if (gone): newgone = ">";

        # Get the first submission for newcomers
        # SELECT %s url, author_id, name, email, date, status
        q= """
        SELECT %s author_id, name, email, date
        FROM %s people, scmlog
        WHERE %s author_id = people.id AND DATEDIFF(NOW(), date) %s %s
              AND author_id IN (%s)
        ORDER BY %s date""" % \
            (fields, tables, filters, newgone, period, q_people, order_by)
        # Order so the group by take the first submission and add total
        # SELECT * FROM ( %s ) nc, (%s) total
        date_field = "first"
        if (gone): date_field = "last";
        q = """
        SELECT revtime, nc.author_id, name, email, date, total, %s, upeople_id
        FROM ( %s ) nc, (%s) total, people_upeople pup
        WHERE total.author_id = nc.author_id AND pup.people_id =  nc.author_id
        GROUP BY nc.author_id ORDER BY nc.date DESC
        """ % (date_field, q, q_total_period)

        return q

    def GetNewGoneAuthors(self, gone = False):
        period = 90 # period of days to be analyzed
        if (gone): period = 180
        fields = "TIMESTAMPDIFF(SECOND, date, NOW())/(24*3600) AS revtime"
        tables = ""
        filters = ""
        # filters = "status<>'MERGED' AND status<>'ABANDONED'"
        q = self.GetNewGoneAuthorsSQL(period, fields, tables, filters)
        if gone:
            q = self.GetNewGoneAuthorsSQL(period, fields, tables, filters, "", gone)
        return(self.db.ExecuteQuery(q))

    def GetNewAuthors(self):
        return self.GetNewGoneAuthors()

    def GetGoneAuthors(self):
        return self.GetNewGoneAuthors(True)

    # New people activity patterns

    def GetNewAuthorsActivity(self):
        period = 90 # days people activity recorded

        # Submissions total activity in period 
        q_total_period = self.GetNewPeopleTotalListSQL(period)

        q_people = self.GetNewPeopleListSQL(period)

        filters = self.db.GetCommitsFiltered()
        if (filters != ""): filters  += " AND "

        # Total commits for new people in period
        q = """
            SELECT total, name, email, first, people_upeople.upeople_id
            FROM (%s) total_period, people, people_upeople
            WHERE %s author_id = people.id
              AND people.id = people_upeople.people_id
              AND author_id IN (%s)
            ORDER BY total DESC
            """ % (q_total_period, filters, q_people)

        return(self.db.ExecuteQuery(q))

    # People leaving the project
    def GetGoneAuthorsActivity(self):
        date_leaving = 90 # last contrib 3 months ago
        date_gone = 180 # last contrib 6 months ago

        q_all_people = """
            SELECT COUNT(scmlog.id) AS total, author_id,
                   MAX(date) AS date, name, email
           FROM scmlog, people
           WHERE people.id = scmlog.author_id
           GROUP BY author_id ORDER BY total
           """

    #    q_leaving = """
    #        SELECT total, name, email, date, people_upeople.upeople_id  from
    #          (%s) t, people_upeople
    #        WHERE DATEDIFF(NOW(),date)>%s and DATEDIFF(NOW(),date)<=%s
    #            AND people_upeople.people_id = author_id
    #        ORDER BY date, total DESC
    #        """ % (q_all_people,date_leaving,date_gone)

        filters = self.db.GetCommitsFiltered()
        if (filters != ""): filters  += " AND "

        q_gone  = """
            SELECT total, name, email, date, people_upeople.upeople_id from
              (%s) t, people_upeople
            WHERE %s DATEDIFF(NOW(),date)>%s
                AND people_upeople.people_id = author_id
            ORDER BY date, total DESC
            """ % (q_all_people, filters, date_gone)

        data = self.db.ExecuteQuery(q_gone)

        return data

    def create_report(self, data_source, destdir):
        from vizgrimoire.SCM import SCM
        if data_source != SCM: return
        self.result(data_source, destdir)

    def result(self, data_source, destdir = None):
        from vizgrimoire.SCM import SCM
        if data_source != SCM or destdir is None: return

        result_dict = {}

        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        code_contrib = {}
        code_contrib["authors"] = self.GetNewAuthors()
        result_dict['people_new'] = code_contrib
        createJSON(code_contrib, destdir+"/scm-code-contrib-new.json")

        code_contrib = {}
        code_contrib["authors"] = self.GetGoneAuthors()
        result_dict['people_gone'] = code_contrib
        createJSON(code_contrib, destdir+"/scm-code-contrib-gone.json")

        data = self.GetNewAuthorsActivity()

        evol = {}
        evol['people'] = {}
        for upeople_id in data['upeople_id']:
            pdata = self.db.GetEvolPeopleSCM(upeople_id, period, startdate, enddate)
            pdata = completePeriodIds(pdata, period, startdate, enddate)
            evol['people'][upeople_id] = {"commits":pdata['commits']}
            # Just to have the time series data
            evol = dict(evol.items() + pdata.items())

        result_dict['people_new_ts'] = evol
        createJSON(evol, destdir+"/new-people-activity-scm-evolutionary.json")

        data = self.GetGoneAuthorsActivity()
        evol = {}
        evol['people'] = {}
        for upeople_id in data['upeople_id']:
            pdata = self.db.GetEvolPeopleSCM(upeople_id, period, startdate, enddate)
            pdata = completePeriodIds(pdata, period, startdate, enddate)
            evol['people'][upeople_id] = {"commits":pdata['commits']}
            # Just to have the time series data
            evol = dict(evol.items() + pdata.items())
        if 'changes' in evol:
            del evol['changes'] # closed (metrics) is included in people
        result_dict['people_gone_ts'] = evol
        createJSON(evol, destdir+"/gone-people-activity-scm-evolutionary.json")

        # data = GetPeopleLeaving()
        # createJSON(data, destdir+"/leaving-people-scr.json")

        evol = {}
        data = completePeriodIds(self.db.GetPeopleIntake(0,1), period, startdate, enddate)
        evol['month'] = data['month']
        evol['id'] = data['id']
        evol['date'] = data['date']
        evol['num_people_1'] = data['people']
        evol['num_people_1_5'] = completePeriodIds(self.db.GetPeopleIntake(1,5),period, startdate, enddate)['people']
        evol['num_people_5_10'] = completePeriodIds(self.db.GetPeopleIntake(5,10), period, startdate, enddate)['people']
        result_dict['people_intake_ts'] = evol
        createJSON(evol, destdir+"/scm-people-intake-evolutionary.json")

        return result_dict

class ContributorsNewGoneSCR(ContributorsNewGone):

    id = "contributors_new_gone_Scr"
    name = "ContributorsNewGoneSCR"
    desc = "Number of contributors new and gone in a project in the code revision system."

    # People Code Contrib New and Gone KPI
    def GetNewPeopleListSQL(self, period):
        filters = self.db.GetIssuesFiltered()
        if (filters != ""): filters  = " WHERE " + filters

        q_people = """
            SELECT submitted_by FROM (SELECT MIN(submitted_on) AS first, submitted_by
            FROM issues
            %s
            GROUP BY submitted_by
            HAVING DATEDIFF(NOW(), first) <= %s) plist """ % (filters, period)
        return q_people

    def GetGonePeopleListSQL(self,period):
        filters = self.db.GetIssuesFiltered()
        if (filters != ""): filters  = " WHERE " + filters

        q_people = """
            SELECT submitted_by FROM (SELECT MAX(submitted_on) AS last, submitted_by
            FROM issues
            %s
            GROUP BY submitted_by
            HAVING DATEDIFF(NOW(), last)>%s) plist """ % (filters, period)
        return q_people

    # Total submissions for people in period
    def GetNewPeopleTotalListSQL(self,period, filters=""):
        issues_filters = self.db.GetIssuesFiltered()
        if (filters != ""):
            if issues_filters != "": filters += " AND " + issues_filters
        else: filters = issues_filters

        if (filters != ""): filters  = " WHERE " + filters
        q_total_period = """
            SELECT COUNT(id) as total, submitted_by, MIN(submitted_on) AS first
            FROM issues
            %s
            GROUP BY submitted_by
            HAVING DATEDIFF(NOW(), first) <= %s
            ORDER BY total
            """ % (filters, period)
        return q_total_period

    # Total submissions for people in period
    def GetGonePeopleTotalListSQL(self,period, filters=""):
        issues_filters = self.db.GetIssuesFiltered()
        if (filters != ""):
            if issues_filters != "": filters += " AND " + issues_filters
        else: filters = issues_filters

        if (filters != ""): filters  = " WHERE " + filters
        q_total_period = """
            SELECT COUNT(id) as total, submitted_by, MAX(submitted_on) AS last
            FROM issues
            %s
            GROUP BY submitted_by
            HAVING DATEDIFF(NOW(), last)>%s
            ORDER BY total
            """ % (filters, period)
        return q_total_period

    # New/Gone people using period as analysis time frame
    def GetNewGoneSubmittersSQL(self,period, fields = "", tables = "", filters = "",
                                order_by = "", gone = False):
        # Adapt filters for total: use issues table only
        filters_total = filters
        if "new_value='ABANDONED'" in filters:
            filters_total = "status='ABANDONED'"
        if "new_value='MERGED'" in filters:
            filters_total = "status='MERGED'"

        q_people = self.GetNewPeopleListSQL(period)
        q_total_period = self.GetNewPeopleTotalListSQL(period, filters_total)
        if (gone):
            q_people = self.GetGonePeopleListSQL(period)
            q_total_period = self.GetGonePeopleTotalListSQL(period, filters_total)

        if (tables != ""): tables +=  ","
        if (filters != ""): filters  += " AND "
        if (self.db.GetIssuesFiltered() != ""): filters += self.db.GetIssuesFiltered() + " AND "
        if (fields != ""): fields  += ","
        if (order_by != ""): order_by  += ","

        newgone = "<=";
        if (gone): newgone = ">";

        # Get the first submission for newcomers
        # SELECT %s url, submitted_by, name, email, submitted_on, status
        q= """
        SELECT %s url, submitted_by, name, email, submitted_on, status
        FROM %s people, issues_ext_gerrit, issues
        WHERE %s submitted_by = people.id AND DATEDIFF(NOW(), submitted_on) %s %s
              AND issues_ext_gerrit.issue_id = issues.id
              AND submitted_by IN (%s)
        ORDER BY %s submitted_on""" % \
            (fields, tables, filters, newgone, period, q_people, order_by)
        # Order so the group by take the first submission and add total
        # SELECT * FROM ( %s ) nc, (%s) total
        date_field = "first"
        if (gone): date_field = "last";
        q = """
        SELECT revtime, url,  nc.submitted_by, name, email, submitted_on, status, total, %s, upeople_id
        FROM ( %s ) nc, (%s) total, people_upeople pup
        WHERE total.submitted_by = nc.submitted_by AND pup.people_id =  nc.submitted_by
        GROUP BY nc.submitted_by ORDER BY nc.submitted_on DESC
        """ % (date_field, q, q_total_period)

        return q

    def GetNewGoneSubmitters(self, gone = False):
        period = 90 # period of days to be analyzed
        if (gone): period = 180
        fields = "TIMESTAMPDIFF(SECOND, submitted_on, NOW())/(24*3600) AS revtime"
        tables = ""
        filters = ""
        # filters = "status<>'MERGED' AND status<>'ABANDONED'"
        q = self.GetNewGoneSubmittersSQL(period, fields, tables, filters)
        if gone:
            q = self.GetNewGoneSubmittersSQL(period, fields, tables, filters, "", gone)
        return(self.db.ExecuteQuery(q))

    def GetNewSubmitters(self):
        return self.GetNewGoneSubmitters()

    def GetGoneSubmitters(self):
        return self.GetNewGoneSubmitters(True)

    def GetNewGoneMergers(self, gone = False):
        period = 90 # period of days to be analyzed
        if (gone): period = 180
        fields = "TIMESTAMPDIFF(SECOND, submitted_on, changed_on)/(24*3600) AS revtime"
        tables = "changes"
        filters = " changes.issue_id = issues.id "
        filters += "AND field='status' AND new_value='MERGED'"
        order_by = "revtime DESC"
        q = self.GetNewGoneSubmittersSQL(period, fields, tables, filters, order_by)
        if (gone):
            q = self.GetNewGoneSubmittersSQL(period, fields, tables, filters, order_by, gone)
        return(self.db.ExecuteQuery(q))

    def GetNewMergers(self):
        return self.GetNewGoneMergers()

    def GetGoneMergers(self):
        return self.GetNewGoneMergers(True)

    def GetNewGoneAbandoners(self,gone = False):
        period = 90 # period of days to be analyzed
        if (gone): period = 180
        fields = "TIMESTAMPDIFF(SECOND, submitted_on, changed_on)/(24*3600) AS revtime"
        tables = "changes"
        filters = " changes.issue_id = issues.id "
        filters += "AND field='status' AND new_value='ABANDONED'"
        order_by = "revtime DESC"
        q = self.GetNewGoneSubmittersSQL(period, fields, tables, filters, order_by)
        if (gone):
            q = self.GetNewGoneSubmittersSQL(period, fields, tables, filters, order_by, gone)
        return(self.db.ExecuteQuery(q))

    def GetNewAbandoners(self):
        return self.GetNewGoneAbandoners()

    def GetGoneAbandoners(self):
        return self.GetNewGoneAbandoners(True)

    # New people activity patterns

    def GetNewSubmittersActivity(self):
        period = 90 # days people activity recorded

        # Submissions total activity in period 
        q_total_period = self.GetNewPeopleTotalListSQL(period)

        q_people = self.GetNewPeopleListSQL(period)

        filters = self.db.GetIssuesFiltered()
        if (filters != ""): filters  += " AND "

        # Total submissions for new people in period
        q = """
            SELECT total, name, email, first, people_upeople.upeople_id
            FROM (%s) total_period, people, people_upeople
            WHERE %s submitted_by = people.id
              AND people.id = people_upeople.people_id
              AND submitted_by IN (%s)
            ORDER BY total DESC
            """ % (q_total_period, filters, q_people)
        return(self.db.ExecuteQuery(q))

    # People leaving the project
    def GetGoneSubmittersActivity(self):
        date_leaving = 90 # last contrib 3 months ago
        date_gone = 180 # last contrib 6 months ago

        q_all_people = """
            SELECT COUNT(issues.id) AS total, submitted_by,
                   MAX(submitted_on) AS submitted_on, name, email
           FROM issues, people
           WHERE people.id = issues.submitted_by
           GROUP BY submitted_by ORDER BY total
           """

    #    q_leaving = """
    #        SELECT total, name, email, submitted_on, people_upeople.upeople_id  from
    #          (%s) t, people_upeople
    #        WHERE DATEDIFF(NOW(),submitted_on)>%s and DATEDIFF(NOW(),submitted_on)<=%s
    #            AND people_upeople.people_id = submitted_by
    #        ORDER BY submitted_on, total DESC
    #        """ % (q_all_people,date_leaving,date_gone)

        filters = self.db.GetIssuesFiltered()
        if (filters != ""): filters  += " AND "

        q_gone  = """
            SELECT total, name, email, submitted_on, people_upeople.upeople_id from
              (%s) t, people_upeople
            WHERE %s DATEDIFF(NOW(),submitted_on)>%s
                AND people_upeople.people_id = submitted_by
            ORDER BY submitted_on, total DESC
            """ % (q_all_people, filters, date_gone)

        data = self.db.ExecuteQuery(q_gone)

        return data

    def create_report(self, data_source, destdir):
        from vizgrimoire.SCR import SCR
        if data_source != SCR: return
        self.result(destdir)

    def result(self, data_source, destdir = None):
        from vizgrimoire.SCR import SCR
        if data_source != SCR or destdir is None: return

        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        code_contrib = {}
        code_contrib["submitters"] = self.GetNewSubmitters()
        code_contrib["mergers"] = self.GetNewMergers()
        code_contrib["abandoners"] = self.GetNewAbandoners()
        createJSON(code_contrib, destdir+"/scr-code-contrib-new.json")

        code_contrib = {}
        code_contrib["submitters"] = self.GetGoneSubmitters()
        code_contrib["mergers"] = self.GetGoneMergers()
        code_contrib["abandoners"] = self.GetGoneAbandoners()
        createJSON(code_cdestdir+"/scr-code-contrib-gone.json")


        data = self.GetNewSubmittersActivity()
        evol = {}
        evol['people'] = {}
        for upeople_id in data['upeople_id']:
            pdata = self.db.GetPeopleEvolSubmissionsSCR(upeople_id, period, startdate, enddate)
            pdata = completePeriodIds(pdata, period, startdate, enddate)
            evol['people'][upeople_id] = {"submissions":pdata['submissions']}
            # Just to have the time series data
            evol = dict(evol.items() + pdata.items())
        if 'changes' in evol:
            del evol['changes'] # closed (metrics) is included in people
        createJSON(evol, destdir+"/new-people-activity-scr-evolutionary.json")

        data = self.GetGoneSubmittersActivity()
        evol = {}
        evol['people'] = {}
        for upeople_id in data['upeople_id']:
            pdata = self.db.GetPeopleEvolSubmissionsSCR(upeople_id, period, startdate, enddate)
            pdata = completePeriodIds(pdata, period, startdate, enddate)
            evol['people'][upeople_id] = {"submissions":pdata['submissions']}
            # Just to have the time series data
            evol = dict(evol.items() + pdata.items())
        if 'changes' in evol:
            del evol['changes'] # closed (metrics) is included in people
        createJSON(evol, destdir+"/gone-people-activity-scr-evolutionary.json")

        # data = GetPeopleLeaving()
        # createJSON(data, destdir+"/leaving-people-scr.json")

        evol = {}
        data = completePeriodIds(self.db.GetPeopleIntake(0,1), period, startdate, enddate)
        evol['month'] = data['month']
        evol['id'] = data['id']
        evol['date'] = data['date']
        evol['num_people_1'] = data['people']
        evol['num_people_1_5'] = completePeriodIds(self.db.GetPeopleIntake(1,5),period, startdate, enddate)['people']
        evol['num_people_5_10'] = completePeriodIds(self.db.GetPeopleIntake(5,10), period, startdate, enddate)['people']
        createJSON(evol, destdir+"/scr-people-intake-evolutionary.json")
