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
#     Santiago Duenas <sduenas@bitergia.com>
#     Alvaro del Castillor <acs@bitergia.com>
#

""" People and Companies evolution per quarters """

from analyses import Analyses
from GrimoireUtils import completePeriodIds, medianAndAvgByPeriod, get_median, get_avg
from query_builder import DSQuery
from metrics_filter import MetricFilters

class TimesTickets(Analyses):
    id = "times_tickets"
    name = "Times Tickets"
    desc = "Tickets time to response and time from opened"

    def getMedianAndAvg(self, period, alias, dates, values):
        data = medianAndAvgByPeriod(period, dates, values)
        result = {period : data[period],
                  'median_' + alias : data['median'],
                  'avg_' + alias : data['avg']}
        return result

    def GetViewFirstCommentPerIssueQueryITS(self):
        """Returns those issues without changes, only comments made
           but others than the reporter."""

        q = "CREATE OR REPLACE VIEW first_comment_per_issue AS " +\
            "SELECT c.issue_id issue_id, MIN(c.submitted_on) date " +\
            "FROM comments c, issues i " +\
            "WHERE c.submitted_by <> i.submitted_by " +\
            "AND c.issue_id = i.id " +\
            "GROUP BY c.issue_id"
        return q

    # shared with top issues study
    def GetViewFirstActionPerIssueQueryITS(self):
        """Returns the first action of each issue.
           Actions means changes or comments that were made by others than
           the reporter."""

        q = "CREATE OR REPLACE VIEW first_action_per_issue AS " +\
            "SELECT issue_id, MIN(date) date " +\
            "FROM first_change_and_comment_issues " +\
            "GROUP BY issue_id"
        return q

    def CreateViews(self):
        q = self.GetViewFirstCommentPerIssueQueryITS()
        self.db.ExecuteViewQuery(q)
        q = self.GetViewFirstActionPerIssueQueryITS()
        self.db.ExecuteViewQuery(q)


    def GetTimeToFirstAction (self, period, startdate, enddate, condition, alias=None) :
        q = """SELECT submitted_on date, TIMESTAMPDIFF(SECOND, submitted_on, fa.date)/(24*3600) AS %(alias)s
               FROM first_action_per_issue fa, issues i
               WHERE i.id = fa.issue_id
               AND submitted_on >= %(startdate)s AND submitted_on < %(enddate)s """

        if condition:
            q += condition

        q += """ ORDER BY date """

        params = {'alias' : alias or 'time_to_action',
                  'startdate' : startdate,
                  'enddate' : enddate}
        query = q % params

        data = self.db.ExecuteQuery(query)
        return (data)

    def GetTimeToFirstComment (self, period, startdate, enddate, condition, alias=None) :
        q = """SELECT submitted_on date, TIMESTAMPDIFF(SECOND, submitted_on, fc.date)/(24*3600) AS %(alias)s
               FROM first_comment_per_issue fc, issues i
               WHERE i.id = fc.issue_id
               AND submitted_on >= %(startdate)s AND submitted_on < %(enddate)s """

        if condition:
            q += condition

        q += """ ORDER BY date """

        params = {'alias' : alias or 'time_to_comment',
                  'startdate' : startdate,
                  'enddate' : enddate}
        query = q % params

        data = self.db.ExecuteQuery(query)
        return (data)

    def GetTimeClosed (self, period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
        q = """SELECT submitted_on date, TIMESTAMPDIFF(SECOND, submitted_on, ch.changed_on)/(24*3600) AS %(alias)s
               FROM issues i, changes ch
               WHERE i.id = ch.issue_id
               AND submitted_on >= %(startdate)s AND submitted_on < %(enddate)s
               AND """

        q += closed_condition

        if ext_condition:
            q += ext_condition

        q += """ ORDER BY date """

        params = {'alias' : alias or 'time_opened',
                  'startdate' : startdate,
                  'enddate' : enddate}
        query = q % params

        data = self.db.ExecuteQuery(query)
        return (data)

    def GetIssuesOpenedAtQuery (self, startdate, enddate, closed_condition, ext_condition=None):
        q = """SELECT issue_id
               FROM issues_log_bugzilla log,
                 (SELECT MAX(id) id
                  FROM issues_log_bugzilla
                  WHERE date >= %(startdate)s AND date < %(enddate)s
                  GROUP BY issue_id) g
               WHERE log.id = g.id  AND NOT """
        q += closed_condition

        if ext_condition:
            q += ext_condition

        q += """ ORDER BY issue_id """

        params = {'startdate' : startdate,
                  'enddate' : enddate}
        query = q % params
        return query

    def GetIssuesWithoutFirstActionAt (self, period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
        q = """SELECT i.id issue_id, TIMESTAMPDIFF(SECOND, submitted_on, %(enddate)s)/(24*3600) AS %(alias)s
               FROM issues i, ("""
        q += self.GetIssuesOpenedAtQuery(startdate, enddate, closed_condition, ext_condition)
        q += """ ) log
                WHERE i.id = log.issue_id AND i.id NOT IN
                (SELECT issue_id
                 FROM first_action_per_issue
                 WHERE date >= %(startdate)s AND date < %(enddate)s)"""

        params = {'alias' : alias or 'time_opened',
                  'startdate' : startdate,
                  'enddate' : enddate}
        query = q % params

        data = self.db.ExecuteQuery(query)
        return (data)

    def GetIssuesWithoutFirstCommentAt (self, period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
        q = """SELECT i.id issue_id, TIMESTAMPDIFF(SECOND, submitted_on, %(enddate)s)/(24*3600) AS %(alias)s
               FROM issues i, ("""
        q += self.GetIssuesOpenedAtQuery(startdate, enddate, closed_condition, ext_condition)
        q += """ ) log
                WHERE i.id = log.issue_id AND i.id NOT IN
                (SELECT issue_id
                 FROM first_comment_per_issue
                 WHERE date >= %(startdate)s AND date < %(enddate)s)"""

        params = {'alias' : alias or 'time_opened',
                  'startdate' : startdate,
                  'enddate' : enddate}
        query = q % params

        data = self.db.ExecuteQuery(query)
        return (data)


    def GetIssuesOpenedAt (self, period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
        q = """SELECT i.id issue_id, TIMESTAMPDIFF(SECOND, submitted_on, %(enddate)s)/(24*3600) AS %(alias)s
               FROM issues i, ("""
        q += self.GetIssuesOpenedAtQuery(startdate, enddate, closed_condition, ext_condition)
        q += """ ) log
                WHERE i.id = log.issue_id"""

        params = {'alias' : alias or 'time_opened',
                  'enddate' : enddate}
        query = q % params

        data = self.db.ExecuteQuery(query)
        return (data)


    def ticketsTimeToResponse(self, period, startdate, enddate, identities_db, backend):
        time_to_response_priority = self.ticketsTimeToResponseByField(period, startdate, enddate,
                                                                      backend.closed_condition,
                                                                      'priority', backend.priority)
        time_to_response_severity = self.ticketsTimeToResponseByField(period, startdate, enddate,
                                                                      backend.closed_condition,
                                                                      'type', backend.severity)

        evol = dict(time_to_response_priority.items() + time_to_response_severity.items())
        return evol

    def ticketsTimeOpened(self, period, startdate, enddate, identities_db, backend):
        log_close_condition_mediawiki = "(status = 'RESOLVED' OR status = 'CLOSED' OR status = 'VERIFIED' OR priority = 'Lowest')"

        evol = {}

        for result_type in ['action', 'comment', 'open']:
            time_opened = self.ticketsTimeOpenedByType(period, startdate, enddate, log_close_condition_mediawiki, result_type)

            time_opened_priority = self.ticketsTimeOpenedByField(period, startdate, enddate, log_close_condition_mediawiki,
                                                                 'priority', backend.priority, result_type)

            time_opened_severity = self.ticketsTimeOpenedByField(period, startdate, enddate, log_close_condition_mediawiki,
                                                                 'type', backend.severity, result_type)

            evol = dict(evol.items() + time_opened.items() + time_opened_priority.items() + time_opened_severity.items())
        return evol

    def ticketsTimeToResponseByField(self, period, startdate, enddate, closed_condition, field, values_set):
        condition = "AND i." + field + " = '%s'"
        evol = {}

        for field_value in values_set:
            field_condition = condition % field_value

            fa_alias = 'tfa_%s' % field_value
            data = self.GetTimeToFirstAction(period, startdate, enddate, field_condition, fa_alias)
            if not isinstance(data[fa_alias], (list)): 
                data[fa_alias] = [data[fa_alias]]
                data['date'] = [data['date']]
            if len(data[fa_alias]) == 0: continue
            time_to_fa = self.getMedianAndAvg(period, fa_alias, data['date'], data[fa_alias])
            time_to_fa = completePeriodIds(time_to_fa, period, startdate, enddate)

            fc_alias = 'tfc_%s' % field_value
            data = self.GetTimeToFirstComment(period, startdate, enddate, field_condition, fc_alias)
            if not isinstance(data[fc_alias], (list)): 
                data[fc_alias] = [data[fc_alias]]
                data['date'] = [data['date']]
            time_to_fc = self.getMedianAndAvg(period, fc_alias, data['date'], data[fc_alias])
            time_to_fc = completePeriodIds(time_to_fc, period, startdate, enddate)

            tclosed_alias = 'ttc_%s' % field_value
            data = self.GetTimeClosed(period, startdate, enddate, closed_condition, field_condition, tclosed_alias)
            if not isinstance(data[tclosed_alias], (list)): 
                data[tclosed_alias] = [data[tclosed_alias]]
                data['date'] = [data['date']]
            time_closed = self.getMedianAndAvg(period, tclosed_alias, data['date'], data[tclosed_alias])
            time_closed = completePeriodIds(time_closed, period, startdate, enddate)

            evol = dict(evol.items() + time_to_fa.items() + time_to_fc.items() + time_closed.items())
        return evol

    def ticketsTimeOpenedByType(self, period, startdate, enddate, closed_condition, result_type):
        # Build a set of dates
        dates = completePeriodIds({period : []}, period, startdate, enddate)[period]
        dates.append(dates[-1] + 1) # add one more month

        if result_type == 'action':
            alias = "topened_tfa"
        elif result_type == 'comment':
            alias = "topened_tfc"
        else:
            alias = "topened"

        time_opened = self.getTicketsTimeOpened(period, dates, closed_condition, result_type, alias)
        time_opened = completePeriodIds(time_opened, period, startdate, enddate)
        return time_opened

    def ticketsTimeOpenedByField(self, period, startdate, enddate, closed_condition, field, values_set, result_type):
        condition = "AND " + field + " = '%s'"
        evol = {}

        # Build a set of dates
        dates = completePeriodIds({period : []}, period, startdate, enddate)[period]
        dates.append(dates[-1] + 1) # add one more month

        for field_value in values_set:
            field_condition = condition % field_value

            if result_type == 'action':
                alias = "topened_tfa_%s" % field_value
            elif result_type == 'comment':
                alias = "topened_tfc_%s" % field_value
            else:
                alias = "topened_%s" % field_value

            time_opened = self.getTicketsTimeOpened(period, dates, closed_condition, result_type, alias, field_condition)
            time_opened = completePeriodIds(time_opened, period, startdate, enddate)
            evol = dict(evol.items() + time_opened.items())

        return evol

    def getTicketsTimeOpened(self, period, dates, closed_condition, result_type, alias, field_condition=None):
        period_dates = []
        median_values = []
        avg_values = []
        current_period = dates[0]  # The first month there aren't remaining issues opened

        startdate = self.filters.startdate
        enddate = self.filters.enddate


        for dt in dates[1:]:
            # Convert dates to readable format (YY-MM-DD)
            year = dt / 12
            month = dt % 12
            if month == 0:
                year = year - 1
                month = 12
            enddate = "'" + str(year) + "-" + str(month) + "-1'"

            if result_type == 'action':
                open_issues = self.GetIssuesWithoutFirstActionAt(period, startdate, enddate, closed_condition,
                                                                      field_condition, alias)[alias]
            elif result_type == 'comment':
                open_issues = self.GetIssuesWithoutFirstCommentAt(period, startdate, enddate, closed_condition,
                                                                       field_condition, alias)[alias]
            else:
                open_issues = self.GetIssuesOpenedAt(period, startdate, enddate, closed_condition,
                                                          field_condition, alias)[alias]

            m = get_median(open_issues)
            avg = get_avg(open_issues)

            period_dates.append(current_period)
            median_values.append(m)
            avg_values.append(avg)
            current_period = dt

        time_opened = {period : period_dates,
                       'median_' + alias : median_values,
                       'avg_' + alias : avg_values}
        return time_opened



    def result(self):
        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        idb = self.db.identities_db
        from ITS import ITS
        backend = ITS._get_backend()

        self.CreateViews()
        time_to_response = self.ticketsTimeToResponse(period, startdate, enddate, idb, backend)
        time_from_opened = self.ticketsTimeOpened(period, startdate, enddate, idb, backend)
        return dict(time_to_response.items() + time_from_opened.items())
