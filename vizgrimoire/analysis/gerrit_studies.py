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
import pandas

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

    def create_report(self, data_source, destdir):
        if data_source != SCR: return
        self.result(data_source, destdir)

    def result(self, data_source = None, destdir = None):

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
        filters.add("i.summary not like '%WIP%'")

        query = " select " + self.db._get_fields_query(fields)
        query = query + " from " + self.db._get_tables_query(tables)
        query = query + " where " + self.db._get_filters_query(filters)

        query = query + " order by last_upload limit 100"

        data = self.db.ExecuteQuery(query)
        # TODO: Hardcoded creation of file
        createJSON(data, destdir + "/scr-oldest_changesets.json")

        return data


class MostActiveChangesetsWaiting4Reviewer(Analyses):
    """ This study provides the list of Gerrit changesets with the highest
        activity that are still waiting for a reviewer action. Thus they 
        were not marked with a Code-Review of -1 or -2.

        This provides the related repository, the patch title, the date
        of the last upload. This is also sorted by the total number of
        patchsets and in second place by the total number of comments.
    """

    id = "list_most_active_changesets_waiting4reviewer"
    name = "List of the most active changesets waiting for a reviewer action"
    desc = "List of the most active changesets waiting for a reviewer action"

    def create_report(self, data_source, destdir):
        if data_source != SCR: return
        self.result(data_source, destdir)

    def result(self, data_source = None, destdir = None):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        # Creation of a view to obtain the last patch date
        # This view contains information about the max date found in a list
        # of patchsets. This later allows to order by such date.
        # In addition, this list ignores abandoned or merged changesets
        # (so closed ones) and those changests whose Code Review is -2
        # at some point.
        query = """create or replace view last_patch as
                          select issue_id,
                                 max(old_value) as last_patch
                          from changes,
                               issues
                          where issues.status<>'Abandoned' and
                                issues.status<>'Merged' and
                                issues.id=changes.issue_id and
                                changes.issue_id not in
                                   (select issue_id from changes where new_value=-2)
                          group by issue_id"""

        self.db.ExecuteQuery(query)

        # Subquery that calculates for a given changeset (issue_id) the total 
        # number of patchsets (iterations in the review process) and the last
        # patchset upload date. This is based on the view "last_patch", so this
        # ignores changesets already abandoned, merged or in a code-review = -2
        # process.
        subquery = """ (select lp.issue_id,
                               max(changed_on) as last_upload,
                               count(distinct(old_value)) as patchsets
                        from changes,
                             last_patch lp
                        where lp.issue_id = changes.issue_id and
                              field='status' and
                              new_value='UPLOADED'
                        group by lp.issue_id
                        order by patchsets
                        desc limit 100) t"""

        # Subquey that calculates for a given changeset the total number of 
        # comments
        subquery_comments = """(select c.issue_id,
                                      count(distinct(c.id)) as comments
                                from comments c,
                                     issues i
                                where i.id = c.issue_id and
                                      i.status <> 'Abandoned' and
                                      i.status <> 'Merged'
                                group by c.issue_id) t2"""

        # Subquery that calculates last patchsets that are in a Code-Review -1/-2
        # status.
        subquery_status = """(select lp.issue_id
                              from last_patch lp,
                                   changes ch
                              where lp.issue_id = ch.issue_id and
                                    lp.last_patch=ch.old_value and
                                    ch.field='Code-Review' and
                                    (new_value=-1 or new_value=-2))"""

        fields.add("tr.url as project_name")
        fields.add("p.name as author_name")
        fields.add("i.issue as gerrit_issue_id")
        fields.add("i.summary as summary")
        fields.add("t.last_upload as last_upload")
        fields.add("i.submitted_on as first_upload")
        fields.add("t.patchsets as number_of_patchsets")
        # This field is removed due to time constraints
        #fields.add("t2.comments as number_of_comments")

        tables.add("issues i")
        tables.add("people p")
        tables.add("trackers tr")
        tables.add("last_patch lp")
        tables.add(subquery)
        # This table is removed due to time constraints
        #tables.add(subquery_comments)
        # If needed in the future, this would be the place to add
        # the GetSQLXXX generic tables to have this analysis adding extra
        # filters

        filters.add("t.issue_id = i.id")
        filters.add("tr.id = i.tracker_id")
        filters.add("i.id = lp.issue_id")
        # This filter is removed due to time constraints
        #filters.add("i.id = t2.issue_id")
        filters.add("i.submitted_by = p.id")
        filters.add("lp.issue_id not in " + subquery_status)
        # If needed, this would be the place to add the GetSQLXXX generic
        # filters to have this analysis adding extra filters

        query = " select " + self.db._get_fields_query(fields)
        query = query + " from " + self.db._get_tables_query(tables)
        query = query + " where " + self.db._get_filters_query(filters)
        query = query + " order by t.patchsets desc limit 100"

        data = self.db.ExecuteQuery(query)
        # TODO: Hardcoded creation of file
        createJSON(data, destdir + "/scr-most_active_changesets.json")

        return data


class OldestChangesetsByAffiliation(Analyses):

    """ This class provides the oldest changesets without activity
        ordered by affiliation and by the last upload.
    """

    id = "oldest_changesets_by_affiliation"
    name = "The oldest changesets by affiliation"
    desc = "The oldest changesets by affiliation and by the last upload"

    def create_report(self, data_source, destdir):
        if data_source != SCR: return
        self.result(data_source, destdir)

    def result(self, data_source = None, destdir = None):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("tr.url as project_name")
        fields.add("pro.name as author_name")
        fields.add("org.name as organization")
        fields.add("i.issue as gerrit_issue_id")
        fields.add("i.summary as summary")
        fields.add("i.submitted_on as first_upload")
        fields.add("t.last_upload as last_upload")

        tables.add("issues i")
        tables.add("trackers tr")
        tables.add("people_uidentities puid")
        tables.add(self.db.identities_db + ".enrollments enr")
        tables.add(self.db.identities_db + ".organizations org")
        tables.add(self.db.identities_db + ".profiles pro")
        tables.add("(select issue_id, max(changed_on) as last_upload from changes where field='status' and new_value='UPLOADED' group by issue_id) t")

        filters.add("t.issue_id = i.id")
        filters.add("i.id not in (select distinct(issue_id) from changes where field='Code-Review')")
        filters.add("i.status<>'Abandoned'")
        filters.add("i.status<>'Merged'")
        filters.add("tr.id=i.tracker_id")
        filters.add("i.submitted_by=puid.people_id")
        filters.add("puid.uuid = enr.uuid")
        filters.add("i.submitted_on >= enr.start")
        filters.add("i.submitted_on < enr.end")
        filters.add("enr.organization_id = org.id")
        filters.add("puid.uuid = pro.uuid")
        filters.add("i.summary not like '%WIP%'")

        query = " select " + self.db._get_fields_query(fields)
        query = query + " from " + self.db._get_tables_query(tables)
        query = query + " where " + self.db._get_filters_query(filters)
        query = query + " order by org.name, t.last_upload"

        data = self.db.ExecuteQuery(query)

        # TODO: Hardcoded creation of file
        createJSON(data, destdir + "/scr-oldest_changesets_by_affiliation.json")

        #Filtering the data to have only 10 entries per organization at most
        data_df = pandas.DataFrame(data, columns=["gerrit_issue_id", "project_name", "organization", "last_upload", "first_upload", "summary", "author_name"])
        organizations = pandas.unique(data_df.organization)
        dataframes = []
        for organization in organizations:
            dataframes.append(data_df[data_df["organization"]==organization][1:10])

        filter_orgs = pandas.concat(dataframes)
        filter_orgs = filter_orgs.to_dict(orient="list")
        createJSON(filter_orgs, destdir + "/scr-oldest_changesets_by_affiliation.json")

        return filter_orgs


if __name__ == '__main__':

    filters = MetricFilters("month", "'2011-04-01'", "'2016-01-01'")
    dbcon = SCRQuery("root", "", "dic_gerrit_mediawiki_5829", "acs_sortinghat_mediawiki_5879")

    oldest_changesets = OldestChangesets(dbcon, filters)
    print oldest_changesets.result()

    oldest_changesets = OldestChangesetsByAffiliation(dbcon, filters)
    oldest_changesets.result(destdir = "./")

    most_active = MostActiveChangesetsWaiting4Reviewer(dbcon, filters)
    most_active.result()

