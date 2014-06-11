## Copyright (C) 2013 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details. 
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
## People.R
##
## Queries for source code review data analysis
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

from GrimoireSQL import ExecuteQuery,ExecuteViewQuery

# _filter_submitter_id as a static global var to avoid SQL re-execute
def _init_filter_submitter_id():
    people_userid = 'l10n-bot'
    q = "SELECT id FROM people WHERE user_id = '%s'" % (people_userid)
    globals()['_filter_submitter_id'] = ExecuteQuery(q)['id']

# To be used for issues table
def GetIssuesFiltered():
    if ('_filter_submitter_id' not in globals()): _init_filter_submitter_id()
    filters = " submitted_by <> %s" % (globals()['_filter_submitter_id'])
    return filters

# To be used for changes table
def GetChangesFiltered():
    if ('_filter_submitter_id' not in globals()): _init_filter_submitter_id()
    filters = " changed_by <> %s" % (globals()['_filter_submitter_id'])
    return filters


################
# KPI queries
################

#####
# ITS
#####

#################
# Views
#################

# Actions : changes or comments that were made by others than the reporter.


def GetViewFirstCommentPerIssueQueryITS():
    """Returns those issues without changes, only comments made
       but others than the reporter."""

    q = "CREATE OR REPLACE VIEW first_comment_per_issue AS " +\
        "SELECT c.issue_id issue_id, MIN(c.submitted_on) date " +\
        "FROM comments c, issues i " +\
        "WHERE c.submitted_by <> i.submitted_by " +\
        "AND c.issue_id = i.id " +\
        "GROUP BY c.issue_id"
    return q


# Is being used? - acs
def GetViewNoActionIssuesQueryITS():
    """Returns those issues without actions.
       Actions means changes or comments that were made by others than
       the reporter."""

    q = "CREATE OR REPLACE VIEW no_action_issues AS " +\
        "SELECT id issue_id " +\
        "FROM issues " +\
        "WHERE id NOT IN ( " +\
        "SELECT DISTINCT(c.issue_id) " +\
        "FROM issues i, changes c " +\
        "WHERE i.id = c.issue_id AND c.changed_by <> i.submitted_by)"
    return q


def CreateViewsITS():
    #FIXME: views should be only created once
    q = GetViewFirstCommentPerIssueQueryITS()
    ExecuteViewQuery(q)

    q = GetViewNoActionIssuesQueryITS()
    ExecuteViewQuery(q)

#########################
# Time to first response
#########################

def GetTimeToFirstAction (period, startdate, enddate, condition, alias=None) :
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

    CreateViewsITS()

    data = ExecuteQuery(query)
    return (data)

def GetTimeToFirstComment (period, startdate, enddate, condition, alias=None) :
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

    CreateViewsITS()

    data = ExecuteQuery(query)
    return (data)


def GetTimeClosed (period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
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

    CreateViewsITS()

    data = ExecuteQuery(query)
    return (data)

def GetIssuesOpenedAtQuery (startdate, enddate, closed_condition, ext_condition=None):
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

def GetIssuesWithoutFirstActionAt (period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
    q = """SELECT i.id issue_id, TIMESTAMPDIFF(SECOND, submitted_on, %(enddate)s)/(24*3600) AS %(alias)s
           FROM issues i, ("""
    q += GetIssuesOpenedAtQuery(startdate, enddate, closed_condition, ext_condition)
    q += """ ) log
            WHERE i.id = log.issue_id AND i.id NOT IN
            (SELECT issue_id
             FROM first_action_per_issue
             WHERE date >= %(startdate)s AND date < %(enddate)s)"""

    params = {'alias' : alias or 'time_opened',
              'startdate' : startdate,
              'enddate' : enddate}
    query = q % params

    CreateViewsITS()

    data = ExecuteQuery(query)
    return (data)


def GetIssuesWithoutFirstCommentAt (period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
    q = """SELECT i.id issue_id, TIMESTAMPDIFF(SECOND, submitted_on, %(enddate)s)/(24*3600) AS %(alias)s
           FROM issues i, ("""
    q += GetIssuesOpenedAtQuery(startdate, enddate, closed_condition, ext_condition)
    q += """ ) log
            WHERE i.id = log.issue_id AND i.id NOT IN
            (SELECT issue_id
             FROM first_comment_per_issue
             WHERE date >= %(startdate)s AND date < %(enddate)s)"""

    params = {'alias' : alias or 'time_opened',
              'startdate' : startdate,
              'enddate' : enddate}
    query = q % params

    CreateViewsITS()

    data = ExecuteQuery(query)
    return (data)


def GetIssuesOpenedAt (period, startdate, enddate, closed_condition, ext_condition=None, alias=None):
    q = """SELECT i.id issue_id, TIMESTAMPDIFF(SECOND, submitted_on, %(enddate)s)/(24*3600) AS %(alias)s
           FROM issues i, ("""
    q += GetIssuesOpenedAtQuery(startdate, enddate, closed_condition, ext_condition)
    q += """ ) log
            WHERE i.id = log.issue_id"""

    params = {'alias' : alias or 'time_opened',
              'enddate' : enddate}
    query = q % params

    CreateViewsITS()

    data = ExecuteQuery(query)
    return (data)
