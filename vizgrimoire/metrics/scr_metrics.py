## Copyright (C) 2014 Bitergia
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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>

""" Metrics for the source code review system """

from datetime import datetime
import logging
import MySQLdb
import numpy

from vizgrimoire.GrimoireUtils import completePeriodIds, checkListArray, medianAndAvgByPeriod, check_array_values
from vizgrimoire.metrics.query_builder import DSQuery

from vizgrimoire.metrics.metrics import Metrics

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.metrics.query_builder import SCRQuery

from vizgrimoire.metrics.query_builder import ITSQuery

from vizgrimoire.SCR import SCR

from sets import Set

class InitialActivity(Metrics):
    """ For the given dates of activity, this returns the first trace found
    """

    id = "first_date"
    name = "First activity date"
    desc = "First commit between the two provided dates"
    data_source = SCR

    def get_agg(self):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("DATE_FORMAT(MIN(submitted_on),'%Y-%m-%d') as first_date")

        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "i.submitted_on", fields,
                                   tables, filters, False,
                                   self.filters.type_analysis)
        return self.db.ExecuteQuery(query)

class EndOfActivity(Metrics):
    """ For the given dates of activity, this returns the last trace found
    """
    id = "last_date"
    name = "Last activity date"
    desc = "Last commit between the two provided dates"
    data_source = SCR

    def get_agg(self):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("DATE_FORMAT(MAX(changed_on),'%Y-%m-%d') as last_date")

        tables.add("changes c")
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))
        filters.add("c.issue_id = i.id")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "c.changed_on", fields,
                                   tables, filters, False,
                                   self.filters.type_analysis)

        return self.db.ExecuteQuery(query)

class Submitted(Metrics):
    id = "submitted"
    name = "Submitted reviews"
    desc = "Number of submitted code review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("submitted", self.filters, evolutionary)
        return q

class Merged(Metrics):
    id = "merged"
    name = "Merged changes"
    desc = "Number of changes merged into the source code"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("merged", self.filters, evolutionary)
        return q

    def _get_sqlchanges (self, evolutionary):
        q = self.db.GetReviewsChangesSQL("merged", self.filters, evolutionary)
        return q

    def get_ts_changes(self):
        query = self._get_sqlchanges(True)
        ts = self.db.ExecuteQuery(query)
        return completePeriodIds(ts, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)


class Mergers(Metrics):
    id = "mergers"
    name = "Successful submitters"
    desc = "Number of persons submitting changes that got accepted"
    data_source = SCR

    def _get_sql(self, evolutionary):
        pass

class Abandoned(Metrics):
    id = "abandoned"
    name = "Abandoned reviews"
    desc = "Number of abandoned review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("abandoned", self.filters, evolutionary)
        return q

    def _get_sqlchanges (self, evolutionary):
        q = self.db.GetReviewsChangesSQL("abandoned", self.filters, evolutionary)
        return q

    def get_ts_changes(self):
        query = self._get_sqlchanges(True)
        ts = self.db.ExecuteQuery(query)
        return completePeriodIds(ts, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)

class BMISCR(Metrics):
    """This class calculates the efficiency closing reviews

    This class is based on the Backlog Management Index that in issues, it is
    calculated as the number of closed issues out of the total number of opened
    ones in a period. (The other way around also provides an interesting view). 
    
    In terms of the code review system, this values is measured as the number
    of merged+abandoned reviews out of the total number of submitted ones.
    """

    id = "bmiscr"
    name = "BMI SCR"
    desc = "Efficiency reviewing: (merged+abandoned reviews)/(submitted reviews)"
    data_source = SCR

    def get_ts(self):
        abandoned_reviews = Abandoned(self.db, self.filters)
        merged_reviews = Merged(self.db, self.filters)
        submitted_reviews = Submitted(self.db, self.filters)

        abandoned = abandoned_reviews.get_ts()
        abandoned = completePeriodIds(abandoned, self.filters.period, self.filters.startdate,
                                      self.filters.enddate)
        # casting the type of the variable in order to use numpy
        # faster way to deal with datasets...
        abandoned_array = numpy.array(abandoned["abandoned"])

        merged = merged_reviews.get_ts()
        merged = completePeriodIds(merged, self.filters.period, self.filters.startdate,
                                      self.filters.enddate)
        merged_array = numpy.array(merged["merged"])

        submitted = submitted_reviews.get_ts()
        submitted = completePeriodIds(submitted, self.filters.period, self.filters.startdate,
                                      self.filters.enddate)
        submitted_array = numpy.array(submitted["submitted"])

        bmi_array = (abandoned_array.astype(float) + merged_array.astype(float)) / submitted_array.astype(float)

        bmi = abandoned
        bmi.pop("abandoned")
        bmi["bmiscr"] = list(bmi_array)

        return bmi

    def get_agg(self):
        abandoned_reviews = Abandoned(self.db, self.filters)
        merged_reviews = Merged(self.db, self.filters)
        submitted_reviews = Submitted(self.db, self.filters)

        abandoned = abandoned_reviews.get_agg()
        abandoned_data = abandoned["abandoned"]
        merged = merged_reviews.get_agg()
        merged_data = merged["merged"]
        submitted = submitted_reviews.get_agg()
        submitted_data = submitted["submitted"]

        if submitted_data == 0:
            # We should probably add a NaN value.
            bmi_data= 0
        else:
            bmi_data = float(merged_data + abandoned_data) / float(submitted_data)
        bmi = {"bmiscr":bmi_data}

        return bmi



class Pending(Metrics):
    id = "pending"
    name = "Pending reviews"
    desc = "Number of pending review processes"
    data_source = SCR

    def _get_metrics_for_pending(self):
        # We need to fix the same filter for all metrics
        metrics_for_pendig = {}

        metric = SCR.get_metrics("submitted", SCR)
        if metric is None:
            metric = Submitted(self.db, self.filters)
        metric.filters = self.filters
        metrics_for_pendig['submitted'] = metric

        metric = SCR.get_metrics("merged", SCR)
        if metric is None:
            metric = Merged(self.db, self.filters)
        metric.filters = self.filters
        metrics_for_pendig['merged'] = metric

        metric = SCR.get_metrics("abandoned", SCR)
        if metric is None:
            metric = Abandoned(self.db, self.filters)
        metric.filters = self.filters
        metrics_for_pendig['abandoned'] = metric

        return metrics_for_pendig

    def _get_metrics_for_pending_all(self, evol):
        """ Return the metric for all items normalized """
        metrics = self._get_metrics_for_pending()
        if evol is True:
            submitted = metrics['submitted'].get_ts()
            merged = metrics['merged'].get_ts()
            abandoned = metrics['abandoned'].get_ts()
        else:
            submitted = metrics['submitted'].get_agg()
            merged = metrics['merged'].get_agg()
            abandoned = metrics['abandoned'].get_agg()

        from vizgrimoire.report import Report
        filter = Report.get_filter(self.filters.type_analysis[0])
        items = SCR.get_filter_items(filter, self.filters.startdate,
                                     self.filters.enddate, self.db.identities_db)
        items = items.pop('name')

        from vizgrimoire.GrimoireUtils import fill_and_order_items
        id_field = SCRQuery.get_group_field_alias(self.filters.type_analysis[0])
        submitted = check_array_values(submitted)
        merged = check_array_values(merged)
        abandoned = check_array_values(abandoned)

        submitted = fill_and_order_items(items, submitted, id_field,
                                         evol, self.filters.period,
                                         self.filters.startdate, self.filters.enddate)
        merged = fill_and_order_items(items, merged, id_field,
                                         evol, self.filters.period,
                                         self.filters.startdate, self.filters.enddate)
        abandoned = fill_and_order_items(items, abandoned, id_field,
                                         evol, self.filters.period,
                                         self.filters.startdate, self.filters.enddate)

        metrics_for_pendig_all = {
          id_field: submitted[id_field],
          "submitted": submitted["submitted"],
          "merged": merged["merged"],
          "abandoned": abandoned["abandoned"]
        }
        if evol:
            metrics_for_pendig_all[self.filters.period] = submitted[self.filters.period]

        return metrics_for_pendig_all

    def get_agg_all(self):
        evol = False
        metrics = self._get_metrics_for_pending_all(evol)
        id_field = SCRQuery.get_group_field_alias(self.filters.type_analysis[0])
        data= \
            [metrics['submitted'][i]-metrics['merged'][i]-metrics['abandoned'][i] \
             for i in range(0, len(metrics['submitted']))]
        return {id_field:metrics[id_field], "pending":data}

    def get_ts_all(self):
        evol = True
        metrics = self._get_metrics_for_pending_all(evol)
        id_field = SCRQuery.get_group_field_alias(self.filters.type_analysis[0])
        pending = {"pending":[]}
        for i in range(0, len(metrics['submitted'])):
            pending["pending"].append([])
            for j in range(0, len(metrics['submitted'][i])):
                pending_val = metrics["submitted"][i][j] - metrics["merged"][i][j] - metrics["abandoned"][i][j]
                pending["pending"][i].append(pending_val)
        pending[self.filters.period] = metrics[self.filters.period]
        pending[id_field] = metrics[id_field]
        return pending

    def get_agg(self):
        metrics = self._get_metrics_for_pending()
        submitted = metrics['submitted'].get_agg()
        merged = metrics['merged'].get_agg()
        abandoned = metrics['abandoned'].get_agg()

        # GROUP BY queries
        if self.filters.type_analysis is not None and self.filters.type_analysis[1] is None:
            pending = self.get_agg_all()
        else:
            pending = submitted['submitted']-merged['merged']-abandoned['abandoned']
            pending = {"pending":pending}
        return pending

    def get_ts(self):
        metrics = self._get_metrics_for_pending()
        submitted = metrics["submitted"].get_ts()
        merged = metrics["merged"].get_ts()
        abandoned = metrics["abandoned"].get_ts()
        evol = dict(submitted.items() + merged.items() + abandoned.items())
        pending = {"pending":[]}
            # GROUP BY queries
        if self.filters.type_analysis is not None and self.filters.type_analysis[1] is None:
            pending = self.get_ts_all()
        else:
            for i in range(0, len(evol['submitted'])):
                pending_val = evol["submitted"][i] - evol["merged"][i] - evol["abandoned"][i]
                pending["pending"].append(pending_val)
            pending[self.filters.period] = evol[self.filters.period]
        return pending

class Opened(Metrics):
    id = "opened"
    name = "Opened reviews"
    desc = "Number of review processes opened"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("opened", self.filters, evolutionary)
        return q

class Closed(Metrics):
    id = "closed"
    name = "Closed reviews"
    desc = "Number of closed review processes (merged or abandoned)"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("closed", self.filters, evolutionary)
        return q

class InProgress(Metrics):
    id = "inprogress"
    name = "In progress reviews"
    desc = "Number review processes in progress"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("inprogress", self.filters, evolutionary)
        return q


class New(Metrics):
    id = "new"
    name = "New reviews"
    desc = "Number of new review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL("new", self.filters, evolutionary)
        return q

    def _get_sqlchanges (self, evolutionary):
        q = self.db.GetReviewsChangesSQL("new", self.filters, evolutionary)
        return q

    def get_ts_changes(self):
        query = self._get_sqlchanges(True)
        ts = self.db.ExecuteQuery(query)
        return completePeriodIds(ts, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)

class PatchesVerified(Metrics):
    id = "verified"
    name = "Verified patches reviews"
    desc = "Number of verified review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL ("verified", self.filters, evolutionary)
        return q

class PatchesApproved(Metrics):
    id = "approved"
    name = "Approved patches reviews"
    desc = "Number of approved review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL ("approved", self.filters, evolutionary)
        return q

class PatchesCodeReview(Metrics):
    id = "codereview"
    name = "Code review patches"
    desc = "Number of patches in review processes in code review state"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL ("codereview", self.filters, evolutionary)
        return q

class PatchesSent(Metrics):
    id = "sent"
    name = "Number of patches sent"
    desc = "Number of patches sent"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL ("sent", self.filters, evolutionary)
        return q

class PatchesPerReview(Metrics):
    """Class that returns the mean and median of patches per review

    The Submitted class or the PatchesSent class do not provide information about
    the mean and median number of iterations (patches) per review.
    Indeed, such information is not even available given that numbers
    are returned in each of those classes.

    Thus, this class returns two values per analyzed period: mean and median
    of patches per review.

    Finally, a review and its patches are part of a period, if such review
    has some activity during the period of analysis. Thus, if a patch
    was submitted before the period, but activity is registered (for instance
    a +1 review), the whole review with all of its iterations is counted.

    Having another approach would force to analyze only submitted reviews in
    a period, but if the period is too small, iterations may not appear
    as many as they are sometimes.

    """

    id = "iterations_per_review"
    name = "Number of patches per changeset"
    desc = "Number of patches (iterations till a patch is closed) per changeset (a whole review process)"
    data_source = SCR

    def get_agg(self):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(ch.old_value)) as patches")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        tables.add("changes ch")
        tables.add("issues i")
        tables.add("(select distinct(issue_id) as issue_id " +\
                   "  from changes " + \
                   "  where changed_on >= "+self.filters.startdate+" and " +\
                    "        changed_on < "+self.filters.enddate+" ) t")

        filters.add("ch.issue_id = t.issue_id")
        filters.add("ch.issue_id = i.id")
        filters.add("ch.old_value <> ''")

        query = "select " + self.db._get_fields_query(fields) 
        query = query + " from " + self.db._get_tables_query(tables)
        query = query + " where " + self.db._get_filters_query(filters)
        query = query + " group by i.id "

        patches_per_review = self.db.ExecuteQuery(query)

        agg_data={}
        agg_data["mean"] = round(numpy.mean(patches_per_review["patches"]), 2)
        agg_data["median"] = round(numpy.median(patches_per_review["patches"]), 2)

        return agg_data

    def get_ts(self):
        #Not implemented
        return None

class Participants(Metrics):
    """ A participant in SCR is a person with any trace in the system

    A trace is defined in the case of scr as a comment, a change or a new
    changeset/patchset.
    """

    id = "participants"
    name = "Participants in SCR"
    desc = "A participant is defined as any person with any type of activity in SCR"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])


        fields.add("count(distinct(u.uuid)) as participants")

        # issues table is needed given that this is used to
        # filter by extra conditions such as trackers
        tables.add("people_uidentities pup")
        tables.add(self.db.identities_db + ".uidentities u")
        tables.add("issues i")

        filters.add("t.submitted_by = pup.people_id")
        filters.add("pup.uuid = u.uuid")
        filters.add("i.id = t.issue_id")

        # Comments people
        fields_c = Set([])
        tables_c = Set([])
        filters_c = Set([])

        fields_c.add("comments.issue_id as issue_id")
        fields_c.add("comments.submitted_by as submitted_by")
        fields_c.add("comments.submitted_on as submitted_on")
        tables_c.add("comments")

        # Changes people
        fields_ch = Set([])
        tables_ch = Set([])
        filters_ch = Set([])

        fields_ch.add("ch.issue_id as issue_id")
        fields_ch.add("ch.changed_by as submitted_by")
        fields_ch.add("ch.changed_on as submitted_on")
        tables_ch.add("changes ch")

        # Issues people
        fields_i = Set([])
        tables_i = Set([])
        filters_i = Set([])

        fields_i.add("i.id as issue_id")
        fields_i.add("i.submitted_by as submitted_by")
        fields_i.add("i.submitted_on as submitted_on")
        tables_i.add("issues i")

        #Building queries
        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        evol = False

        comments_query = self.db.BuildQuery(period, startdate, enddate,
                                            "comments.submitted_on",
                                            fields_c, tables_c, filters_c,
                                            evol)
        changes_query = self.db.BuildQuery(period, startdate, enddate,
                                            "ch.changed_on",
                                            fields_ch, tables_ch, filters_ch,
                                            evol)
        issues_query = self.db.BuildQuery(period, startdate, enddate,
                                            "i.submitted_on",
                                            fields_i, tables_i, filters_i,
                                            evol)

        tables_query = "(" + comments_query + ") union (" + changes_query + ") union (" + issues_query + ")"
        tables.add("(" + tables_query + ") t")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))

        # We want participants in commetns, changes and issues, not just issues
        if "i.submitted_by = pup.people_id" in filters:
            filters.remove("i.submitted_by = pup.people_id")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "t.submitted_on",
                                   fields, tables, filters, evolutionary, self.filters.type_analysis)
        return query

    def get_list(self, metric_filters = None, days = 0):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        if days > 0:
            filters.add("DATEDIFF (%s, t.submitted_on) < %s " % (self.filters.enddate, days))

        fields.add("u.uuid as id")
        fields.add("pro.name as identifier")
        fields.add("count(*) as events")

        tables.add("people_uidentities pup")
        tables.add(self.db.identities_db + ".uidentities u")
        tables.add(self.db.identities_db + ".profiles pro")
        tables.add("issues i")

        filters.add("t.submitted_by = pup.people_id")
        filters.add("pup.uuid = u.uuid")
        filters.add("pup.uuid = pro.uuid")
        filters.add("i.id = t.issue_id")

        # Comments people
        fields_c = Set([])
        tables_c = Set([])
        filters_c = Set([])

        fields_c.add("comments.issue_id as issue_id")
        fields_c.add("comments.submitted_by as submitted_by")
        fields_c.add("comments.submitted_on as submitted_on")
        tables_c.add("comments")

        # Changes people
        fields_ch = Set([])
        tables_ch = Set([])
        filters_ch = Set([])

        fields_ch.add("ch.issue_id as issue_id")
        fields_ch.add("ch.changed_by as submitted_by")
        fields_ch.add("ch.changed_on as submitted_on")
        tables_ch.add("changes ch")

        # Issues people
        fields_i = Set([])
        tables_i = Set([])
        filters_i = Set([])

        fields_i.add("i.id as issue_id")
        fields_i.add("i.submitted_by as submitted_by")
        fields_i.add("i.submitted_on as submitted_on")
        tables_i.add("issues i")

        #Building queries
        period = self.filters.period
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        evol = False

        comments_query = self.db.BuildQuery(period, startdate, enddate,
                                            "comments.submitted_on",
                                            fields_c, tables_c, filters_c,
                                            evol)
        changes_query = self.db.BuildQuery(period, startdate, enddate,
                                            "ch.changed_on",
                                            fields_ch, tables_ch, filters_ch,
                                            evol)
        issues_query = self.db.BuildQuery(period, startdate, enddate,
                                            "i.submitted_on",
                                            fields_i, tables_i, filters_i,
                                            evol)

        tables_query = "(" + comments_query + ") union (" + changes_query + ") union (" + issues_query + ")"
        tables.add("(" + tables_query + ") t")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))
        # We want participants in commetns, changes and issues, not just issues
        if "i.submitted_by = pup.people_id" in filters:
            filters.remove("i.submitted_by = pup.people_id")

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, "t.submitted_on",
                                   fields, tables, filters, False)

        query = query + " group by pro.name "
        query = query + " order by count(*) desc "

        # Add orgs information
        q_orgs = """
            SELECT top.id, top.identifier as identifier, events, org.name as organization FROM (%s) top
            LEFT JOIN %s.enrollments enr ON top.id = enr.uuid
            LEFT JOIN %s.organizations org ON org.id = enr.organization_id;
            """ % (query, self.db.identities_db, self.db.identities_db)

        return self.db.ExecuteQuery(q_orgs)


class PatchesWaitingForReviewer(Metrics):
    id = "WaitingForReviewer"
    name = "Waiting for reviewer patches"
    desc = "Number of patches from review processes waiting for reviewer"
    data_source = SCR


    def _get_sql(self, evolutionary):
        q = self.db.GetWaiting4ReviewerSQL(self.filters, evolutionary)
        return q

class PatchesWaitingForSubmitter(Metrics):
    id = "WaitingForSubmitter"
    name = "Waiting for submitter patches"
    desc = "Number of patches from review processes waiting for submitter"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetWaiting4SubmitterSQL(self.filters, evolutionary)
        return q

class ReviewsWaitingForReviewerTS(Metrics):
    id = "ReviewsWaitingForReviewer_ts"
    name = "Reviews waiting for reviewer"
    desc = "Number of preview processes waiting for reviewer"
    data_source = SCR

    def _get_date_from_month(self, monthid):
        # month format: year*12+month
        year = (monthid-1) / 12
        month = monthid - year*12
        # We need the last day of the month
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        current = str(year)+"-"+str(month)+"-"+str(last_day)
        return (current)

    def _get_pending(self, month, reviewers = False):
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        identities_db = self.db.identities_db
        type_analysis =  self.filters.type_analysis

        current = self._get_date_from_month(month)

        sql_max_patchset = self.db.get_sql_max_patchset_for_reviews (current)
        sql_reviews_reviewed = self.db.get_sql_reviews_reviewed(self.filters.startdate, current)
        sql_reviews_closed = self.db.get_sql_reviews_closed(self.filters.startdate, current)

        fields = Set([])


        fields.add("COUNT(DISTINCT(i.id)) as pending")
        fields = self.db._get_fields_query(fields)

        tables = Set([])
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        tables = self.db._get_tables_query(tables)

        # Pending (NEW = submitted-merged-abandoned) REVIEWS
        filters = Set([])
        filters.add(" i.submitted_on <= '"+current+"'")
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))
        # remove closed reviews
        filters.add("i.id NOT IN ("+ sql_reviews_closed +")")

        if reviewers:
            filters.add("i.id NOT IN (%s)" % (sql_reviews_reviewed))

        filters = self.db._get_filters_query(filters)

        all_items = self.db.get_all_items(self.filters.type_analysis)

        q = self.db.GetSQLGlobal('i.submitted_on', fields, tables, filters,
                                 startdate, enddate, all_items)

        rs = self.db.ExecuteQuery(q)
        if all_items is not None:
            checkListArray(rs)
        else:
            rs = rs['pending']
        return rs


    def _get_ts_all(self):
        startdate = self.filters.startdate
        enddate = self.filters.enddate
        period = self.filters.period
        start = datetime.strptime(startdate, "'%Y-%m-%d'")
        end = datetime.strptime(enddate, "'%Y-%m-%d'")

        if (period != "month"):
            logging.error("Period not supported in " + self.id  + " " + period)
            return {}

        start_month = start.year*12 + start.month
        end_month = end.year*12 + end.month
        months = end_month - start_month

        # First, we need to group by the filter field the data
        all_items = self.db.get_all_items(self.filters.type_analysis)
        group_field = self.db.get_group_field(all_items)
        id_field = group_field.split('.')[1] # remove table name

        # First we get all the data from db
        pending_data = {'month':[],'pending':[],id_field:[]}
        pending_reviewers_data = {'month':[],'pending':[],id_field:[]}
        for i in range(0, months+1):
            # total pending this month
            pending_month = self._get_pending(start_month+i)
            pending_data['month'].append(start_month+i)
            pending_data[id_field].append(pending_month[id_field])
            pending_data['pending'].append(pending_month['pending'])
            # total pending waiting for reviewers this month
            pending_reviewers_month = self._get_pending(start_month+i, True)
            pending_reviewers_data['month'].append(start_month+i)
            pending_reviewers_data[id_field].append(pending_reviewers_month[id_field])
            pending_reviewers_data['pending'].append(pending_reviewers_month['pending'])
        # print pending_review_data
        # Get the complete list of items
        all_items = []
        for data in pending_data[id_field]:
            all_items = list(Set(all_items+data))
        # Build the final dict with format [[months],[itens],[[item1_ts],...]
        pending = {"month":[]}
        pending['month'] = pending_data['month']
        pending = completePeriodIds(pending, self.filters.period,
                                    self.filters.startdate, self.filters.enddate)
        pending["ReviewsWaiting_ts"] = []
        pending["ReviewsWaitingForReviewer_ts"] = []
        pending[id_field] = all_items
        # For each item build the tme series and append it
        for item in all_items:
            item_ts = []
            for i in range(0, months+1):
                if len(pending_data[id_field])>0 and \
                   item in pending_data[id_field][i]:
                    pos = pending_data[id_field][i].index(item)
                    item_ts.append(pending_data['pending'][i][pos])
                else:
                    # No pendings reviews for this item
                    item_ts.append(0)
            pending['ReviewsWaiting_ts'].append(item_ts)
            item_reviewers_ts = []
            for i in range(0, months+1):
                if len(pending_reviewers_data[id_field])>0 and \
                   item in pending_reviewers_data[id_field][i]:
                    pos = pending_reviewers_data[id_field][i].index(item)
                    item_reviewers_ts.append(pending_reviewers_data['pending'][i][pos])
                else:
                    # No pendings reviews for this item
                    item_reviewers_ts.append(0)
            pending['ReviewsWaitingForReviewer_ts'].append(item_reviewers_ts)
        return pending

    def get_ts(self):

        if self.filters.type_analysis and self.filters.type_analysis[1] is None:
            # Support for GROUP BY queries
            return self._get_ts_all()

        pending = {"month":[],
                   "ReviewsWaiting_ts":[],
                   "ReviewsWaitingForReviewer_ts":[]}

        startdate = self.filters.startdate
        enddate = self.filters.enddate
        period = self.filters.period
        start = datetime.strptime(startdate, "'%Y-%m-%d'")
        end = datetime.strptime(enddate, "'%Y-%m-%d'")

        if (period != "month"):
            logging.error("Period not supported in " + self.id  + " " + period)
            return {}

        start_month = start.year*12 + start.month
        end_month = end.year*12 + end.month
        months = end_month - start_month

        for i in range(0, months+1):
            pending['month'].append(start_month+i)
            pending_month = self._get_pending(start_month+i)
            pending_reviewers_month = self._get_pending(start_month+i, True)
            pending['ReviewsWaiting_ts'].append(pending_month)
            pending['ReviewsWaitingForReviewer_ts'].append(pending_reviewers_month)

        return pending


class ReviewsWaitingForReviewer(Metrics):
    id = "ReviewsWaitingForReviewer"
    name = "Reviews waiting for reviewer"
    desc = "Number of preview processes waiting for reviewer"
    data_source = SCR


    def _get_sql (self, evolutionary):

        sql_max_patchset = self.db.get_sql_max_patchset_for_reviews ()
        sql_reviews_reviewed = self.db.get_sql_reviews_reviewed(self.filters.startdate)

        fields = Set([])
        fields.add("COUNT(DISTINCT(i.id)) as ReviewsWaitingForReviewer")

        tables = Set([])
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters = Set([])
        filters.add("i.status = 'NEW'")
        filters.add("i.id NOT IN (%s) " % (sql_reviews_reviewed))
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, "i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return(q)

# Review this metrics according to ReviewsWaitingForReviewer
class ReviewsWaitingForSubmitter(Metrics):
    id = "ReviewsWaitingForSubmitter"
    name = "Reviews waiting for submitter"
    desc = "Number of review processes waiting for submitter"
    data_source = SCR


    def _get_sql(self, evolutionary):
        q_last_change = self.db.get_sql_last_change_for_reviews()

        fields = Set([])
        fields.add("COUNT(DISTINCT(i.id)) as ReviewsWaitingForSubmitter")

        tables = Set([])
        tables.add("issues i")
        tables.add("changes c")
        tables.add("(%s) t1 " % q_last_change)
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters = Set([])
        filters.add("i.id = c.issue_id")
        filters.add("t1.id = c.id")
        filters.add("(c.field='CRVW' or c.field='Code-Review' or c.field='Verified' or c.field='VRIF')")
        filters.add("(c.new_value=-1 or c.new_value=-2)")
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " c.changed_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

class Companies(Metrics):
    id = "organizations"
    name = "Organizations"
    desc = "Number of organizations (organizations, etc.) with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        #TODO: warning -> not using GetSQLReportFrom/Where to build queries
        fields.add("count(distinct(enr.organization_id)) as organizations")
        tables.add("issues i")
        tables.add("people_uidentities pup")
        tables.add(self.db.identities_db + ".enrollments enr")
        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.uuid = enr.uuid")

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list (self):
        q = "SELECT org.id as id, org.name as name, COUNT(DISTINCT(i.id)) AS total "+\
                   "FROM  "+self.db.identities_db+".organizations org, "+\
                           self.db.identities_db+".enrollments enr, "+\
                    "     people_uidentities pup, "+\
                    "     issues i "+\
                   "WHERE i.submitted_by = pup.people_id AND "+\
                   "  enr.uuid = pup.uuid AND "+\
                   "  org.id = enr.organization_id AND "+\
                   "  i.status = 'merged' AND "+\
                   "  i.submitted_on >="+  self.filters.startdate+ " AND "+\
                   "  i.submitted_on < "+ self.filters.enddate+ " "+\
                   "GROUP BY org.name "+\
                   "ORDER BY total DESC, org.name "
        return(self.db.ExecuteQuery(q))

class Countries(Metrics):
    id = "countries"
    name = "Countries"
    desc = "Number of countries with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        #TODO: warning -> not using GetSQLReportFrom/Where to build queries
        fields.add("count(distinct(pro.country_code)) as countries")
        tables.add("issues i")
        tables.add("people_uidentities pup")
        tables.add(self.db.identities_db + ".profiles pro")
        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.uuid = pro.uuid")

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list  (self):
        q = "SELECT cou.name as name, COUNT(DISTINCT(i.id)) AS issues "+\
               "FROM  "+self.db.identities_db+".countries cou, "+\
                       self.db.identities_db+".profiles pro, "+\
                "    people_uidentities pup, "+\
                "    issues i "+\
               "WHERE i.submitted_by = pup.people_id AND "+\
               "  pro.uuid = pup.uuid AND "+\
               "  cou.code = pro.country_code AND "+\
               "  i.status = 'merged' AND "+\
               "  i.submitted_on >="+  self.filters.startdate+ " AND "+\
               "  i.submitted_on < "+ self.filters.enddate+ " "+\
               "GROUP BY cou.name "+\
               "ORDER BY issues DESC "
        return(self.db.ExecuteQuery(q))



class Domains(Metrics):
    id = "domains"
    name = "Domains"
    desc = "Number of domains with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = "COUNT(DISTINCT(SUBSTR(email,LOCATE('@',email)+1))) AS domains"
        tables = "issues i, people p "
        filters = "i.submitted_by = p.id"
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " i.submitted_on ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list(self):
        from vizgrimoire.data_source import DataSource
        from vizgrimoire.filter import Filter
        startdate = self.filters.startdate
        enddate = self.filters.enddate

        fields = "DISTINCT(SUBSTR(email,LOCATE('@',email)+1)) AS domain"
        tables = "issues i, people p"
        filters = "i.submitted_by = p.id"

        q = """
            SELECT %s
            FROM %s
            WHERE %s AND i.submitted_on >= %s AND i.submitted_on < %s
            GROUP BY domain
            ORDER BY COUNT(DISTINCT(i.id)) DESC LIMIT %i
            """ % (fields, tables, filters, startdate, enddate,  + Metrics.domains_limit)

        data = self.db.ExecuteQuery(q)
        data['name'] = data.pop('domain')
        return (data)

class Projects(Metrics):
    id = "projects"
    name = "Projects"
    desc = "Number of projects in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        pass

    def get_list (self):
        # Projects activity needs to include subprojects also
        logging.info ("Getting projects list for SCR")

        # Get all projects list
        q = "SELECT p.id AS name FROM  %s.projects p" % (self.db.projects_db)
        projects = self.db.ExecuteQuery(q)
        data = []

        # Loop all projects getting reviews
        for project in projects['name']:
            type_analysis = ['project', project]

            metric = SCR.get_metrics("submitted", SCR)
            type_analysis_orig = metric.filters.type_analysis
            metric.filters.type_analysis = type_analysis
            reviews = metric.get_agg()
            metric.filters.type_analysis = type_analysis_orig

            reviews = reviews['submitted']
            if (reviews > 0):
                data.append([reviews,project])

        # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
        from operator import itemgetter
        data_sort = sorted(data, key=itemgetter(0),reverse=True)
        names = [name[1] for name in data_sort]

        return({"name":names})

class Repositories(Metrics):
    id = "repositories"
    name = "Repositories"
    desc = "Number of repositories with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(t.id)) as repositories")
        tables.add("issues i")
        tables.add("trackers t")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.add("i.tracker_id = t.id")
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))
        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list  (self):
        #TODO: warning -> not using GetSQLReportFrom/Where
        q = "SELECT t.url as name, COUNT(DISTINCT(i.id)) AS issues "+\
               " FROM  issues i, trackers t "+\
               " WHERE i.tracker_id = t.id AND "+\
               "  i.submitted_on >="+  self.filters.startdate+ " AND "+\
               "  i.submitted_on < "+ self.filters.enddate +\
               " GROUP BY t.url "+\
               " ORDER BY issues DESC "
        names = self.db.ExecuteQuery(q)
        if not isinstance(names['name'], (list)): names['name'] = [names['name']]
        return(names)


class People(Metrics):
    id = "people2"
    name = "People"
    desc = "Number of people active in code review activities"
    data_source = SCR

    def _get_sql(self, evolutionary):
        pass

    def _get_top_global (self, days = 0, metric_filters = None):
        """ Implemented using Submitters """
        top = None
        submitters = SCR.get_metrics("submitters", SCR)
        if submitters is None:
            #TODO: absolutely wrong: EmailsSenders???
            submitters = EmailsSenders(self.db, self.filters)
            top = submitters._get_top_global(days, metric_filters)
        else:
            afilters = submitters.filters
            submitters.filters = self.filters
            top = submitters._get_top_global(days, metric_filters)
            submitters.filters = afilters

        top['name'] = top.pop('openers')
        return top

class Reviewers(Metrics):
    id = "reviewers"
    name = "Reviewers"
    desc = "Number of persons reviewing code review activities"
    data_source = SCR
    action = "reviews"

    # Not sure if this top is right
    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        if filter_bots != "": filter_bots += " AND "
        date_limit = ""

        #TODO: warning -> not using GetSQLReportFrom/Where
        if (days != 0 ):
            q = "SELECT @maxdate:=max(changed_on) from changes limit 1"
            self.db.ExecuteQuery(q)
            date_limit = " AND DATEDIFF(@maxdate, changed_on)<" + str(days)

        q = "SELECT up.uuid as id, up.identifier as reviewers, "+\
            "               count(distinct(c.id)) as reviewed "+\
            "        FROM people_uidentities pup, changes c, "+ self.db.identities_db+".uidentities up "+\
            "        WHERE "+ filter_bots+ " "+\
            "            c.changed_by = pup.people_id and "+\
            "            pup.uuid = up.uuid and "+\
            "            c.changed_on >= "+ startdate + " and "+\
            "            c.changed_on < "+ enddate + " "+\
            "            "+ date_limit + " "+\
            "        GROUP BY up.identifier "+\
            "        ORDER BY reviewed desc, reviewers "+\
            "        LIMIT " + str(limit)

        # Add orgs information
        q_orgs = """
            SELECT top.id, reviewers, reviewed, org.name as organization FROM (%s) top
            LEFT JOIN %s.enrollments enr ON top.id = enr.uuid
            LEFT JOIN %s.organizations org ON org.id = enr.organization_id;
            """ % (q, self.db.identities_db, self.db.identities_db)

        return(self.db.ExecuteQuery(q_orgs))



    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(changed_by)) as reviewers")
        tables.add("changes ch")
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.add("ch.issue_id = i.id")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables.add("people_uidentities pup")
        filters.add("ch.changed_by  = pup.people_id")

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " ch.changed_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q


class ActiveCoreReviewers(Metrics):
    """Returns a list of core reviewers in Gerrit systems.
    
    A core reviewer is defined as a reviewer that can use the 
    +2 or -2 in the review system. However, given that there is not
    a public list of core reviewers per project, this class assumes
    that a developer is a(n active)  core reviewer the point in time when she
    uses a +2 or a -2.

    """

    id = "active_core_reviewers"
    name = "Active Core Reviewers"
    desc = "Number of developers reviewing code review activities that are allowed to use a +2 or -2"
    data_source = SCR

    def _get_sql(self, evolutionary):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(changed_by)) as active_core_reviewers")
        tables.add("changes ch")
        tables.add("issues_ext_gerrit ieg")
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.add("ch.issue_id = i.id")
        #TODO: warning -> at some point the filter for branches should be at
        #                 the GetSQLReportFrom/Where method.
        filters.add("ieg.branch like '%master%'")
        filters.add("ieg.issue_id = i.id")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables.add("people_uidentities pup")
        filters.add("ch.changed_by  = pup.people_id")
        filters.add("(ch.new_value = -2 or ch.new_value = 2)")
        filters.add("field = 'Code-Review'")

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " ch.changed_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list(self, metric_filters = None, days = 0):
        # TODO: missing calculation of last x days in the query

        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("up.uuid as id")
        if days > 0:
            filters.add("DATEDIFF (%s, ch.changed_on) < %s " % (self.filters.enddate, days))

        fields.add("up.identifier as identifier")
        fields.add("count(distinct(ch.id)) as reviews")

        tables.add("changes ch")
        tables.add("issues_ext_gerrit ieg")
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))

        filters.add("ch.issue_id = i.id")
        #TODO: warning -> at some point the filter for branches should be at
        #                 the GetSQLReportFrom/Where method.
        filters.add("ieg.branch like '%master%'")
        filters.add("ieg.issue_id = i.id")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables.add(self.db.identities_db + ".uidentities up")
        tables.add("people_uidentities pup")
        filters.add("ch.changed_by  = pup.people_id")
        filters.add("pup.uuid = up.uuid")
        filters.add("(ch.new_value = -2 or ch.new_value = 2)")
        filters.add("field = 'Code-Review'")
        #dates
        filters.add("changed_on >= " + self.filters.startdate)
        filters.add("changed_on < " + self.filters.enddate)

        select = self.db._get_fields_query(fields)
        from_ = self.db._get_tables_query(tables)
        filters = self.db._get_filters_query(filters)

        query = "select " + select + " from " + from_ + " where " + filters
        query = query + " group by up.uuid, up.identifier"
        query = query + " order by count(distinct(ch.id)) desc, up.uuid "

        # Add orgs information
        q_orgs = """
            SELECT top.id, identifier, reviews, org.name as organization FROM (%s) top
            LEFT JOIN %s.enrollments enr ON top.id = enr.uuid
            LEFT JOIN %s.organizations org ON org.id = enr.organization_id;
            """ % (query, self.db.identities_db, self.db.identities_db)

        return self.db.ExecuteQuery(q_orgs)


class Closers(Metrics):
    id = "closers"
    name = "Closers"
    desc = "Number of persons closing code review activities"
    data_source = SCR
    action = "closed"

    def _get_top_global (self, days = 0, metric_filters = None):

        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        if filter_bots != "": filter_bots += " AND "
        date_limit = ""

        if (days != 0 ):
            q = "SELECT @maxdate:=max(submitted_on) from issues limit 1"
            self.db.ExecuteQuery(q)
            date_limit = " AND DATEDIFF(@maxdate, submitted_on)<"+str(days)

        # TODO: warning-> not using GetSQLReportFrom/Where
        merged_sql = " AND status='MERGED' "
        rol = "mergers"
        action = "merged"

        q = "SELECT up.uuid as id, up.identifier as "+rol+", "+\
            "            count(distinct(i.id)) as "+action+" "+\
            "        FROM people_uidentities pup, issues i, "+self.db.identities_db+".uidentities up "+\
            "        WHERE "+ filter_bots+ " "+\
            "            i.submitted_by = pup.people_id and "+\
            "            pup.uuid = up.uuid and "+\
            "            i.submitted_on >= "+ startdate+ " and "+\
            "            i.submitted_on < "+ enddate+ " "+\
            "            "+date_limit+ merged_sql+ " "+\
            "        GROUP BY up.identifier "+\
            "        ORDER BY "+action+" desc, id "+\
            "        LIMIT "+ str(limit)

        # Add orgs information
        q_orgs = """
            SELECT top.id, %s, %s, org.name as organization FROM (%s) top
            LEFT JOIN %s.enrollments enr ON top.id = enr.uuid
            LEFT JOIN %s.organizations org ON org.id = enr.organization_id;
            """ % (rol, action, q, self.db.identities_db, self.db.identities_db)

        return(self.db.ExecuteQuery(q_orgs))

    def _get_sql(self, evolutionary):
        pass

# Pretty similar to ITS openers
class Submitters(Metrics):
    id = "submitters"
    name = "Submitters"
    desc = "Number of persons submitting code review processes"
    data_source = SCR
    action = "submitted"

    def __get_sql_trk_prj__(self, evolutionary):
        """ First we get the submitters then join with unique identities """

        tpeople_sql  = "SELECT  distinct(submitted_by) as submitted_by, submitted_on  "
        tables = self.db.GetSQLReportFrom(self.filters)
        tables.add("issues i")
        tpeople_sql += " FROM " + self.db._get_tables_query(tables)
        filters_ext = self.db._get_filters_query(self.db.GetSQLReportWhere(self.filters,"issues"))
        if (filters_ext != ""):
            tpeople_sql += " WHERE " + filters_ext

        fields = Set([])
        tables = self.db.GetSQLReportFrom(self.filters)
        filters = self.db.GetSQLReportWhere(self.filters,"issues")

        fields.add("count(distinct(pup.uuid)) as submitters")
        tables.add("people_uidentities pup")
        tables.add("issues i")
        tables.add("(%s) tpeople" % (tpeople_sql))
        filters.add("tpeople.submitted_by = pup.people_id")
        filters.add("i.submitted_by = pup.people_id")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " tpeople.submitted_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def __get_sql_default__(self, evolutionary):
        """ This function returns the evolution or agg number of people opening issues """
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(pup.uuid)) as submitters")
        tables.add("issues i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters,"issues"))

        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables.add("people_uidentities pup")
        filters.add("i.submitted_by = pup.people_id")

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " submitted_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def _get_sql(self, evolutionary):
#         if (self.filters.type_analysis is not None and (self.filters.type_analysis[0] in  ["repository","project"])):
        if (self.filters.type_analysis is not None and (self.filters.type_analysis[0] in  ["project"])):
            # repository filter does not work with prj SQL using GROUP BY queries
            return self.__get_sql_trk_prj__(evolutionary)
        else:
            return self.__get_sql_default__(evolutionary)

    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters
        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        if filter_bots != "": filter_bots += " AND "

        date_limit = ""
        rol = "openers"
        action = "opened"

        #TODO: warning -> not using GetSQLReportFrom/Where
        if (days != 0 ):
            q = "SELECT @maxdate:=max(submitted_on) from issues limit 1"
            self.db.ExecuteQuery(q)
            date_limit = " AND DATEDIFF(@maxdate, submitted_on)<"+str(days)

        q = "SELECT up.uuid as id, up.identifier as "+rol+", "+\
            "            count(distinct(i.id)) as "+action+" "+\
            "        FROM people_uidentities pup, issues i, "+self.db.identities_db+".uidentities up "+\
            "        WHERE "+ filter_bots+ " "+\
            "            i.submitted_by = pup.people_id and "+\
            "            pup.uuid = up.uuid and "+\
            "            i.submitted_on >= "+ startdate+ " and "+\
            "            i.submitted_on < "+ enddate+ " "+\
            "            "+date_limit +  " "+\
            "        GROUP BY up.identifier "+\
            "        ORDER BY "+action+" desc, id "+\
            "        LIMIT "+ str(limit)

        # Add orgs information
        q_orgs = """
            SELECT top.id, %s, %s, org.name as organization FROM (%s) top
            LEFT JOIN %s.enrollments enr ON top.id = enr.uuid
            LEFT JOIN %s.organizations org ON org.id = enr.organization_id;
            """ % (rol, action, q, self.db.identities_db, self.db.identities_db)

        return(self.db.ExecuteQuery(q_orgs))

class TimeToReview(Metrics):
    id = "review_time"
    name = "Review Time"
    desc = "Time to review"
    data_source = SCR

    def _get_sql(self):
        if self.filters.period != "month": return None
        bots = []
        q = self.db.GetTimeToReviewQuerySQL (self.filters, bots)
        return q

    def _get_agg_all(self, data):
        from numpy import median, average
        from vizgrimoire.GrimoireUtils import removeDecimals

        data_all = {}

        # First, we need to group by the filter field the data
        all_items = self.db.get_all_items(self.filters.type_analysis)
        id_field = self.db.get_group_field_alias(all_items)

        items =  list(Set(data[id_field]))
        data_all[id_field] = items
        for id in ["review_time_days_median", "review_time_days_avg"]:
            data_all[id] = []

        for item in items:
            # First, extract the data for the item
            data_item = {"changed_on":[],"revtime":[]}
            for i in range(0,len(data[id_field])):
                if data[id_field][i] == item:
                    data_item["changed_on"].append(data["changed_on"][i])
                    data_item["revtime"].append(data["revtime"][i])
            data_revtime = data_item['revtime']
            if (isinstance(data, list) == False): data_revtime = [data_revtime]
            # ttr_median = sorted(data)[len(data)//2]
            if (len(data_revtime) == 0):
                ttr_median = float("nan")
                ttr_avg = float("nan")
            else:
                ttr_median = float(median(removeDecimals(data_revtime)))
                ttr_avg = float(average(removeDecimals(data_revtime)))
            data_all["review_time_days_median"].append(ttr_median)
            data_all["review_time_days_avg"].append(ttr_avg)
        return data_all

    def get_agg(self):
        from numpy import median, average
        from vizgrimoire.GrimoireUtils import removeDecimals

        q = self._get_sql()
        if q is None: return {}
        data = self.db.ExecuteQuery(q)

        if self.filters.type_analysis and self.filters.type_analysis[1] is None:
            # Support for GROUP BY queries
            return self._get_agg_all(data)

        data = data['revtime']
        if (isinstance(data, list) == False): data = [data]
        # ttr_median = sorted(data)[len(data)//2]
        if (len(data) == 0):
            ttr_median = float("nan")
            ttr_avg = float("nan")
        else:
            ttr_median = float(median(removeDecimals(data)))
            ttr_avg = float(average(removeDecimals(data)))
        return {"review_time_days_median":ttr_median, "review_time_days_avg":ttr_avg}

    def _get_ts_all(self, data):
        from numpy import median, average
        from vizgrimoire.GrimoireUtils import removeDecimals

        data_all = {}

        # First, we need to group by the filter field the data
        all_items = self.db.get_all_items(self.filters.type_analysis)
        id_field = self.db.get_group_field_alias(all_items)

        items =  list(Set(data[id_field]))
        data_all[id_field] = items
        for id in ["review_time_days_median", "review_time_days_avg"]:
            data_all[id] = []

        for item in items:
            # First, extract the data for the item
            data_item = {"changed_on":[],"revtime":[]}
            for i in range(0,len(data[id_field])):
                if data[id_field][i] == item:
                    data_item["changed_on"].append(data["changed_on"][i])
                    data_item["revtime"].append(data["revtime"][i])

            med_avg_list = medianAndAvgByPeriod(self.filters.period,
                                                data_item['changed_on'], data_item['revtime'])
            metrics_list = {}
            if (med_avg_list != None):
                metrics_list['review_time_days_median'] = med_avg_list['median']
                metrics_list['review_time_days_avg'] = med_avg_list['avg']
                metrics_list['month'] = med_avg_list['month']
            else:
                metrics_list['review_time_days_median'] = []
                metrics_list['review_time_days_avg'] = []
                metrics_list['month'] = []

            metrics_list = completePeriodIds(metrics_list, self.filters.period,
                                             self.filters.startdate, self.filters.enddate)

            data_all['review_time_days_median'].append(metrics_list['review_time_days_median'])
            data_all['review_time_days_avg'].append(metrics_list['review_time_days_avg'])
            ts_fields = ['unixtime','date','month','id']
            for ts_field in ts_fields:
                data_all[ts_field] = metrics_list[ts_field]

        return data_all

    def get_ts(self):
        q = self._get_sql()
        if q is None: return {}
        review_list = self.db.ExecuteQuery(q)
        checkListArray(review_list)

        if self.filters.type_analysis and self.filters.type_analysis[1] is None:
            # Support for GROUP BY queries
            return self._get_ts_all(review_list)

        metrics_list = {}
        med_avg_list = medianAndAvgByPeriod(self.filters.period, review_list['changed_on'], review_list['revtime'])
        if (med_avg_list != None):
            metrics_list['review_time_days_median'] = med_avg_list['median']
            metrics_list['review_time_days_avg'] = med_avg_list['avg']
            metrics_list['month'] = med_avg_list['month']
        else:
            metrics_list['review_time_days_median'] = []
            metrics_list['review_time_days_avg'] = []
            metrics_list['month'] = []

        metrics_list = completePeriodIds(metrics_list, self.filters.period,
                          self.filters.startdate, self.filters.enddate)

        return metrics_list


class TimeToReviewPatch(Metrics):
    """ This class returns the time that a submitter or a reviewer has been waiting

    This class considers that a review process is a three states machine:
    - Waiting for a submitter action
    - Waiting for a reviewer action
    - End of review process

    A patch is considered as waiting for a submitter action in the
    following conditions:
    - A Code-Review action is detected, being this a -1 or -2
    - A Verified action is detected, being this a -1 or -2
    - A Workflow -1 is detected, after the started. An initial patchset with 
      Workflow -1 would not be considered in the machine states.

    A patch is considered as waiting for a reviewer action in the
    following conditions:
    - A new Patch upload is detected (eg: new patch, rebase or restore action)

    Existing limitations:
    - There are cases where the time of the review found in the db took place before
    the last upload. This takes places when trivial changes are requested to the
    patch submitter. Given this, those negative time-waiting-for-a-submitter-action
    are simply ignored from the final dataset. This takes place when using +2 or
    when using -2.
    - This analysis is based on the Changes table. There may appear issues whose part of their life
    is out of the timeframe limits provided in the self.filters variable. In this case,
    those changesets, would be included and added to the analysis.

    """

    id = "timewaiting_reviewer_n_submitter"
    name = "Time waiting for reviewer and submitter"
    desc = "Time waiting for reviewer and submitter"
    data_source = SCR

    def get_agg(self):
        # Building query
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("ch.issue_id")
        fields.add("ch.field")
        fields.add("ch.old_value")
        fields.add("ch.new_value")
        fields.add("UNIX_TIMESTAMP(ch.changed_on) as changed_on")
        fields.add("ch.changed_on as date")

        tables.add("changes ch")
        tables.add("issues i")
        subquery = "(select distinct(i.id) from changes ch, issues i "
        subquery = subquery + " where ch.changed_on >= " + self.filters.startdate + " and "
        subquery = subquery + " ch.changed_on < " + self.filters.enddate + " and "
        subquery = subquery + " ch.issue_id = i.id) ch2"
        tables.add(subquery)

        filters.add("""(field='Upload' or
                   (field='Verified' and (new_value=-1 or new_value=-2)) or
                   (field='Code-Review' and (new_value=-1 or new_value=-2 or new_value=2)) or
                   (field='Workflow' and new_value=-1))""")
        filters.add("ch.issue_id = i.id")
        filters.add("i.id = ch2.id")
        filters.add("ch.changed_on < " + self.filters.enddate)

        # Migrating those sets to strings
        fields_str = self.db._get_fields_query(fields)
        tables_str = self.db._get_tables_query(tables)
        filters_str = self.db._get_filters_query(filters)

        # Adding extra filters
        tables_str = tables_str + ", " + self.db._get_tables_query(self.db.GetSQLReportFrom(self.filters))
        filters_str = filters_str + " and " + self.db._get_filters_query(self.db.GetSQLReportWhere(self.filters))
        filters_str = filters_str + " order by issue_id, cast(old_value as DECIMAL) asc"

        query = "select " + fields_str + " from " + tables_str + " where " + filters_str
        changes = self.db.ExecuteQuery(query)

        # Starting the analysis of time waiting for reviewer or for submitter
        waiting4reviewer = []
        waiting4submitter = []

        query = """select UNIX_TIMESTAMP(min(changed_on)) as first_date,
                          min(changed_on) as first_date_str,
                          UNIX_TIMESTAMP(max(changed_on)) as end_date,
                          max(changed_on) as end_date_str
                   from changes
                   where changed_on >= %s and
                         changed_on < %s""" % (self.filters.startdate, self.filters.enddate)
        dates = self.db.ExecuteQuery(query)
        min_date = int(dates["first_date"])
        max_date = int(dates["end_date"])

        count = 0
        old_issue = 1 # assuming we always start with the first issue_id
        old_patchset = 1 # assuming we always start with the first patchset

        # This analysis is based on a two states machine.
        # A patch is either waiting for a reviewer action or by a submitter action.
        # In any of the two states there is an exit: this could be the end time of the
        # analysis or that the review was closed.
        # In any case, the initial state is always waiting for the reviewer.
        old_issue_id = -1
        old_field = ""
        old_patchset = -1
        old_new_value = ""
        old_changed_on = -1
        is_new_review = True
        current_state = 1
        for issue_id in changes["issue_id"]:
            field = changes["field"][count]
            patchset = int(changes["old_value"][count])
            new_value = changes["new_value"][count]
            if new_value <> '':
                new_value = int(new_value)
            changed_on = int(changes["changed_on"][count])
            date = changes["date"][count]

            if issue_id <> old_issue_id and old_issue_id > 0:
                is_new_review = True

            # The process always starts with a new upload. This finite
            # states machine assume that all of the reviews calculated
            # are full ones. Only in some cases, when a new changeset arrives
            # we should deal with those reviews
            #print "    \n\n    field: " + str(field)
            #print "    patchset: " + str(patchset)
            #print "    new_value: " + str(new_value)
            #print "    issue_id: " + str(issue_id)
            #print "    changed_on: " + str(changed_on)
            #print "    current_status: " + str(current_state)

            if current_state == 1:
                # Waiting for a reviewer action
                if is_new_review:
                    current_state = 1
                    is_new_review = False
                    if old_changed_on <> -1:
                        # first case
                        # we need to count the last time waiting for a reviewer action
                        waiting4reviewer.append(max_date - old_changed_on)
                    old_changed_on = changed_on

                elif (field == "Verified" and (new_value == -1 or new_value == -2)):
                    current_state = 2
                    # Automatic validation of the code, no time waiting for a reviewer
                    # action
                    old_changed_on = changed_on

                elif (field == "Code-Review" and (new_value == -1 or new_value == -2)):
                    current_state = 2
                    # we have to count time waiting for a reviewer action
                    waiting4reviewer.append(changed_on - old_changed_on)
                    old_changed_on = changed_on

                elif field == "Workflow" and new_value == -1:
                    current_state = 2
                    # we shouldn't count time waiting for a reviewer action
                    # Typically, developers upload a new patchset and after a while they 
                    # update the flag Workflow to -1. Thus, we may consider 
                    # TODO: an improvement to this state would be to take into account
                    # if the modification to Workflow=-1 was made by a reviewer
                    # or the developer
                    old_changed_on = changed_on

                elif field == "Code-Review" and new_value == 2:
                    current_state = 3
                    # we have to count time waiting for a reviewer action
                    waiting4reviewer.append(changed_on - old_changed_on)
                    old_changed_on = changed_on

            elif current_state == 2:
                # Waiting for a submitter action
                if is_new_review:
                    current_state = 1
                    is_new_review = False
                    # we need to count the last time waiting for a submitter action
                    waiting4submitter.append(max_date - old_changed_on)
                    old_changed_on = changed_on

                elif field == "Upload":
                    current_state = 1
                    # we have to count time waiting for a submitter action
                    waiting4submitter.append(changed_on - old_changed_on)
                    old_changed_on = changed_on

                elif field == "Code-Review" and new_value == 2:
                    current_state = 3
                    # we shouldn't count time waiting for a submitter action
                    old_changed_on = changed_on

            elif current_state == 3:
                # Final state
                if is_new_review:
                    is_new_review = False
                    current_state = 1
                    old_changed_on = changed_on

            else:
                print "ERROR, not existing state"

            old_field = field
            old_patchset = patchset
            old_issue_id = issue_id
            count = count + 1


        dataset = {}
        dataset["waitingtime4reviewer"] = waiting4reviewer
        dataset["waitingtime4submitter"] = waiting4submitter
        return dataset

if __name__ == '__main__':

    filters = MetricFilters("month", "'2014-09-01'", "'2014-10-01'", ["repository", "review.openstack.org_openstack/nova"])
    dbcon = SCRQuery("root", "", "dic_bicho_gerrit_openstack_3359_bis3", "dic_cvsanaly_openstack_4114")

    pending = Pending(dbcon, filters)
    print pending.get_agg()

    timewaiting = TimeToReviewPatch(dbcon, filters)
    from vizgrimoire.datahandlers.data_handler import DHESA
    values = timewaiting.get_agg()
    dhesa = DHESA(values["waitingtime4submitter"])
    print values["waitingtime4submitter"]
    print dhesa.data["median"]
    print dhesa.data["mean"]

    print "Submitted info:"
    submitted = Submitted(dbcon, filters)
    print submitted.get_ts()
    print submitted.get_agg()

    print "Merged info:"
    merged = Merged(dbcon, filters)
    print merged.get_ts()
    print merged.get_agg()

    print "Abandoned info:"
    abandoned = Abandoned(dbcon, filters)
    print abandoned.get_ts()
    print abandoned.get_agg()

    print "BMI"
    bmi = BMISCR(dbcon, filters)
    print bmi.get_ts()
    print bmi.get_agg()

    print "Patches per review"
    patches = PatchesPerReview(dbcon, filters)
    print patches.get_agg()

    patches.filters.type_analysis=["repository", "review.openstack.org_openstack/nova"]
    print patches.get_agg()

    patches.filters.type_analysis=["project", "integrated"]
    print patches.get_agg()

    patches.filters.type_analysis=["project", "Infrastructure"]
    print patches.get_agg()

    patches.filters.startdate = "'2014-01-01'"
    patches.filters.type_analysis=["project", "Infrastructure"]
    print patches.get_agg()
