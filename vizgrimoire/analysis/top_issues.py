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
#     Alvaro del Castillo <acs@bitergia.com>
#

""" Top issues by age  for different criteria """

from vizgrimoire.analysis.analyses import Analyses
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.GrimoireUtils import completeTops

class TopIssues(Analyses):
    id = "top_issues"
    name = "Top Issues"
    desc = "Top issues by age"

    def GetViewFirstChangeAndCommentQueryITS(self):
        """Returns the first change and comment of each issue done by others
        than the reporter"""

        q = "CREATE OR REPLACE VIEW first_change_and_comment_issues AS " +\
            "SELECT c.issue_id issue_id, MIN(c.changed_on) date " +\
            "FROM changes c, issues i " +\
            "WHERE changed_by <> submitted_by AND i.id = c.issue_id " +\
            "GROUP BY c.issue_id " +\
            "UNION " + \
            "SELECT c.issue_id issue_id, MIN(c.submitted_on) date " +\
            "FROM comments c, issues i " +\
            "WHERE c.submitted_by <> i.submitted_by AND c.issue_id = i.id " +\
            "GROUP BY c.issue_id"
        return q


    def GetViewFirstActionPerIssueQueryITS(self):
        """Returns the first action of each issue.
           Actions means changes or comments that were made by others than
           the reporter."""

        q = "CREATE OR REPLACE VIEW first_action_per_issue AS " +\
            "SELECT issue_id, MIN(date) date " +\
            "FROM first_change_and_comment_issues " +\
            "GROUP BY issue_id"
        return q

    def GetViewLastCommentPerIssueQueryITS(self):
        """Returns those issues without changes, only comments made
           but others than the reporter."""

        q = "CREATE OR REPLACE VIEW last_comment_per_issue AS " +\
            "SELECT c.issue_id issue_id, MAX(c.submitted_on) date " +\
            "FROM comments c, issues i " +\
            "WHERE c.submitted_by <> i.submitted_by " +\
            "AND c.issue_id = i.id " +\
            "GROUP BY c.issue_id"
        return q

    def CreateViews(self):
        q = self.GetViewFirstChangeAndCommentQueryITS()
        self.db.ExecuteViewQuery(q)
        q = self.GetViewLastCommentPerIssueQueryITS()
        self.db.ExecuteViewQuery(q)
        q = self.GetViewFirstActionPerIssueQueryITS()
        self.db.ExecuteViewQuery(q)

    def GetTopIssuesWithoutAction(self, startdate, enddate, closed_condition, limit):
        q = "SELECT issue_id, TIMESTAMPDIFF(SECOND, date, NOW())/(24*3600) AS time " +\
            "FROM ( " +\
            "  SELECT issue AS issue_id, submitted_on AS date " +\
            "  FROM issues " +\
            "  WHERE id NOT IN ( " +\
            "    SELECT issue_id " +\
            "    FROM first_action_per_issue " +\
            "  ) " +\
            "  AND NOT ( " + closed_condition + ") " +\
            "  AND submitted_on >= " + startdate + " AND submitted_on < " + enddate +\
            ") no_actions " +\
            "GROUP BY issue_id " +\
            "ORDER BY time DESC,issue_id " +\
            "LIMIT " + limit
        data = self.db.ExecuteQuery(q)
        return (data)


    def GetTopIssuesWithoutComment(self, startdate, enddate, closed_condition, limit):
        q = "SELECT issue_id, TIMESTAMPDIFF(SECOND, date, NOW())/(24*3600) AS time " +\
            "FROM ( " +\
            "  SELECT issue AS issue_id, submitted_on AS date " +\
            "  FROM issues " +\
            "  WHERE id NOT IN ( " +\
            "    SELECT issue_id " +\
            "    FROM last_comment_per_issue " +\
            "  ) " +\
            "  AND NOT ( " + closed_condition + ") " +\
            "  AND submitted_on >= " + startdate + " AND submitted_on < " + enddate +\
            ") no_comments " +\
            "GROUP BY issue_id " +\
            "ORDER BY time DESC, issue_id " +\
            "LIMIT " + limit
        data = self.db.ExecuteQuery(q)
        return (data)

    def GetTopIssuesWithoutResolution(self, startdate, enddate, closed_condition, limit):
        q = "SELECT issue AS issue_id, TIMESTAMPDIFF(SECOND, submitted_on, NOW())/(24*3600) AS time " +\
            "FROM issues " +\
            "WHERE NOT ( " + closed_condition + ") " +\
            "AND submitted_on >= " + startdate + " AND submitted_on < " + enddate +\
            "GROUP BY issue_id " +\
            "ORDER BY time DESC, issue_id " +\
            "LIMIT " + limit
        data = self.db.ExecuteQuery(q)
        return (data)

    def GetIssuesDetails(self):
        q = "SELECT i.issue AS issue_id, i.summary AS summary, t.url AS tracker_url " +\
            "FROM issues i, trackers t " +\
            "WHERE i.tracker_id = t.id "
        data = self.db.ExecuteQuery(q)

        details = {}
        i = 0
        for issue_id in data['issue_id']:
            details[issue_id] = (data['summary'][i], data['tracker_url'][i])
            i += 1
        return details


    def create_report(self, data_source = None, destdir = None):
    	return self.result(data_source)

    def result(self, data_source = None):
        """ Returns a JSON to be included in top file """
        from vizgrimoire.ITS import ITS
        if  data_source is not None and data_source != ITS: return None

        # Closed condition for MediaWiki
        top_close_condition_mediawiki = """
            (status = 'RESOLVED' OR status = 'CLOSED' OR status = 'VERIFIED'
             OR priority = 'Lowest')
        """
        nissues = "40"

        startdate = self.filters.startdate
        enddate = self.filters.enddate

        self.CreateViews()

        issues_details = self.GetIssuesDetails()

        top_issues_data = {}

        tops = self.GetTopIssuesWithoutAction(startdate, enddate, top_close_condition_mediawiki, nissues)
        top_issues_data['issues.no action']= completeTops(tops, issues_details)

        tops = self.GetTopIssuesWithoutComment(startdate, enddate, top_close_condition_mediawiki, nissues)
        top_issues_data['issues.no comment']= completeTops(tops, issues_details)

        tops = self.GetTopIssuesWithoutResolution(startdate, enddate, top_close_condition_mediawiki, nissues)
        top_issues_data['issues.no resolution']= completeTops(tops, issues_details)

        return top_issues_data
