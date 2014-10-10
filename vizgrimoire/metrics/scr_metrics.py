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

import logging
import MySQLdb
import numpy

from GrimoireUtils import completePeriodIds, checkListArray, medianAndAvgByPeriod

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import SCRQuery

from query_builder import ITSQuery

from SCR import SCR

from sets import Set

class Submitted(Metrics):
    id = "submitted"
    name = "Submitted reviews"
    desc = "Number of submitted code review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "submitted",
                                  self.filters.type_analysis, evolutionary)
        return q

class Merged(Metrics):
    id = "merged"
    name = "Merged changes"
    desc = "Number of changes merged into the source code"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "merged",
                                  self.filters.type_analysis, evolutionary)
        return q

    def _get_sqlchanges (self, evolutionary):
        q = self.db.GetReviewsChangesSQL(self.filters.period, self.filters.startdate,
                                         self.filters.enddate, "merged",
                                         self.filters.type_analysis, evolutionary)
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
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "abandoned",
                                  self.filters.type_analysis, evolutionary)
        return q

    def _get_sqlchanges (self, evolutionary):
        q = self.db.GetReviewsChangesSQL(self.filters.period, self.filters.startdate,
                                         self.filters.enddate, "abandoned",
                                         self.filters.type_analysis, evolutionary)
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
        metric.filters = self.filters
        metrics_for_pendig['submitted'] = metric

        metric = SCR.get_metrics("merged", SCR)
        metric.filters = self.filters
        metrics_for_pendig['merged'] = metric

        metric = SCR.get_metrics("abandoned", SCR)
        metric.filters = self.filters
        metrics_for_pendig['abandoned'] = metric

        return metrics_for_pendig


    def get_agg(self):
        metrics = self._get_metrics_for_pending()
        submitted = metrics['submitted'].get_agg()
        merged = metrics['merged'].get_agg()
        abandoned = metrics['abandoned'].get_agg()

        # GROUP BY queries not supported yet
        if isinstance(submitted['submitted'], list):  pending = None
        else: pending = submitted['submitted']-merged['merged']-abandoned['abandoned']
        return ({"pending":pending})

    def get_ts(self):
        metrics = self._get_metrics_for_pending()
        submitted = metrics["submitted"].get_ts()
        merged = metrics["merged"].get_ts()
        abandoned = metrics["abandoned"].get_ts()
        evol = dict(submitted.items() + merged.items() + abandoned.items())
        pending = {"pending":[]}
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
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "opened",
                                  self.filters.type_analysis, evolutionary)
        return q

class Closed(Metrics):
    id = "closed"
    name = "Closed reviews"
    desc = "Number of closed review processes (merged or abandoned)"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "closed",
                                  self.filters.type_analysis, evolutionary)
        return q

class InProgress(Metrics):
    id = "inprogress"
    name = "In progress reviews"
    desc = "Number review processes in progress"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "inprogress",
                                  self.filters.type_analysis, evolutionary)
        return q


class New(Metrics):
    id = "new"
    name = "New reviews"
    desc = "Number of new review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "new",
                                  self.filters.type_analysis, evolutionary)
        return q

    def _get_sqlchanges (self, evolutionary):
        q = self.db.GetReviewsChangesSQL(self.filters.period, self.filters.startdate,
                                         self.filters.enddate, "new",
                                         self.filters.type_analysis, evolutionary)
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
        q = self.db.GetEvaluationsSQL (self.filters.period, self.filters.startdate,
                                       self.filters.enddate, "verified",
                                       self.filters.type_analysis, evolutionary)
        return q

class PatchesApproved(Metrics):
    id = "approved"
    name = "Approved patches reviews"
    desc = "Number of approved review processes"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL (self.filters.period, self.filters.startdate,
                                       self.filters.enddate, "approved",
                                       self.filters.type_analysis, evolutionary)
        return q

class PatchesCodeReview(Metrics):
    id = "codereview"
    name = "Code review patches"
    desc = "Number of patches in review processes in code review state"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL (self.filters.period, self.filters.startdate,
                                       self.filters.enddate, "codereview",
                                       self.filters.type_analysis, evolutionary)
        return q

class PatchesSent(Metrics):
    id = "sent"
    name = "Number of patches sent"
    desc = "Number of patches sent"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetEvaluationsSQL (self.filters.period, self.filters.startdate,
                                       self.filters.enddate, "sent",
                                       self.filters.type_analysis, evolutionary)
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
        fields = "count(distinct(ch.old_value)) as patches"
        tables_from = self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters_where = self.db.GetSQLReportWhere(self.filters.type_analysis)

  
        tables = " changes ch, issues i, " +\
                 " (select distinct(issue_id) as issue_id " +\
                 "  from changes " + \
                 "  where changed_on >= "+self.filters.startdate+" and " +\
                 "        changed_on < "+self.filters.enddate+" ) t "
        if len(tables_from) > 0:
            tables = tables +  tables_from


        filters = " ch.issue_id = t.issue_id and "
        filters = filters + " ch.issue_id = i.id and "
        filters = filters + " ch.old_value <> '' "

        if len(filters_where) > 0:
            filters = filters + filters_where

        filters = filters + " group by i.id "

        query = "select " + fields + " from " + tables + " where " + filters

        patches_per_review = self.db.ExecuteQuery(query)

        agg_data={}
        agg_data["mean"] = round(numpy.mean(patches_per_review["patches"]), 2)
        agg_data["median"] = round(numpy.median(patches_per_review["patches"]), 2)

        return agg_data

    def get_ts(self):
        #Not implemented
        return None


class PatchesWaitingForReviewer(Metrics):
    id = "WaitingForReviewer"
    name = "Waiting for reviewer patches"
    desc = "Number of patches from review processes waiting for reviewer"
    data_source = SCR


    def _get_sql(self, evolutionary):
        q = self.db.GetWaiting4ReviewerSQL(self.filters.period, self.filters.startdate,
                                           self.filters.enddate,
                                           self.filters.type_analysis, evolutionary)
        return q

class PatchesWaitingForSubmitter(Metrics):
    id = "WaitingForSubmitter"
    name = "Waiting for submitter patches"
    desc = "Number of patches from review processes waiting for submitter"
    data_source = SCR

    def _get_sql(self, evolutionary):
        q = self.db.GetWaiting4SubmitterSQL(self.filters.period, self.filters.startdate,
                                            self.filters.enddate,
                                            self.filters.type_analysis, evolutionary)
        return q

class ReviewsWaitingForReviewerTS(Metrics):
    id = "ReviewsWaitingForReviewer_ts"
    name = "Reviews waiting for reviewer"
    desc = "Number of preview processes waiting for reviewer"
    data_source = SCR

    def get_ts(self):
        from datetime import datetime

        def get_date_from_month(monthid):
            # month format: year*12+month
            year = (monthid-1) / 12
            month = monthid - year*12
            # We need the last day of the month
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            current = str(year)+"-"+str(month)+"-"+str(last_day)
            return (current)


        def get_pending(month, reviewers = False):
            current = get_date_from_month(month)

            sql_max_patchset = self.db.get_sql_max_patchset_for_reviews (current)
            sql_reviews_reviewed = self.db.get_sql_reviews_reviewed(self.filters.startdate, current)
            sql_reviews_closed = self.db.get_sql_reviews_closed(self.filters.startdate, current)

            fields = "COUNT(DISTINCT(i.id)) as pending"

            tables = " issues i "
            tables = tables + self.db.GetSQLReportFrom(type_analysis)

            # Pending (NEW = submitted-merged-abandoned) REVIEWS
            filters = " i.submitted_on <= '"+current+"'"
            filters += self.db.GetSQLReportWhere(type_analysis)
            # remove closed reviews
            filters += " AND i.id NOT IN ("+ sql_reviews_closed +")"

            if reviewers:
                filters += """ AND i.id NOT IN (%s)
                """ % (sql_reviews_reviewed)

            q = self.db.GetSQLGlobal('i.submitted_on', fields, tables, filters,
                                     startdate, enddate)

            rs = self.db.ExecuteQuery(q)
            return rs['pending']

        pending = {"month":[],
                   "ReviewsWaiting_ts":[],
                   "ReviewsWaitingForReviewer_ts":[]}

        startdate = self.filters.startdate
        enddate = self.filters.enddate
        period = self.filters.period
        identities_db = self.db.identities_db
        type_analysis =  self.filters.type_analysis

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
            pending_month = get_pending(start_month+i)
            pending_reviewers_month = get_pending(start_month+i, True)
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

        fields = "COUNT(DISTINCT(i.id)) as ReviewsWaitingForReviewer"
        tables = "issues i "
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = " i.status = 'NEW' AND i.id NOT IN (%s) " % (sql_reviews_reviewed)

        filters = filters + self.db.GetSQLReportWhere(self.filters.type_analysis)

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

        fields = "COUNT(DISTINCT(i.id)) as ReviewsWaitingForSubmitter"
        tables = "changes c, issues i, (%s) t1" % q_last_change
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = """
            i.id = c.issue_id  AND t1.id = c.id
            AND (c.field='CRVW' or c.field='Code-Review' or c.field='Verified' or c.field='VRIF')
            AND (c.new_value=-1 or c.new_value=-2)
        """
        filters = filters + self.db.GetSQLReportWhere(self.filters.type_analysis)

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " c.changed_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

class Companies(Metrics):
    id = "companies"
    name = "Organizations"
    desc = "Number of organizations (companies, etc.) with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = "count(distinct(upc.company_id)) as companies"
        tables = "issues i, people_upeople pup, %s.upeople_companies upc" % (self.db.identities_db)
        filters = "i.submitted_by = pup.people_id and pup.upeople_id = upc.upeople_id"

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list (self):
        q = "SELECT c.id as id, c.name as name, COUNT(DISTINCT(i.id)) AS total "+\
                   "FROM  "+self.db.identities_db+".companies c, "+\
                           self.db.identities_db+".upeople_companies upc, "+\
                    "     people_upeople pup, "+\
                    "     issues i "+\
                   "WHERE i.submitted_by = pup.people_id AND "+\
                   "  upc.upeople_id = pup.upeople_id AND "+\
                   "  c.id = upc.company_id AND "+\
                   "  i.status = 'merged' AND "+\
                   "  i.submitted_on >="+  self.filters.startdate+ " AND "+\
                   "  i.submitted_on < "+ self.filters.enddate+ " "+\
                   "GROUP BY c.name "+\
                   "ORDER BY total DESC "
        return(self.db.ExecuteQuery(q))

class Countries(Metrics):
    id = "countries"
    name = "Countries"
    desc = "Number of countries with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        fields = "count(distinct(upc.country_id)) as countries"
        tables = "issues i, people_upeople pup, %s.upeople_countries upc" % (self.db.identities_db)
        filters = "i.submitted_by = pup.people_id and pup.upeople_id = upc.upeople_id"

        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list  (self):
        q = "SELECT c.name as name, COUNT(DISTINCT(i.id)) AS issues "+\
               "FROM  "+self.db.identities_db+".countries c, "+\
                       self.db.identities_db+".upeople_countries upc, "+\
                "    people_upeople pup, "+\
                "    issues i "+\
               "WHERE i.submitted_by = pup.people_id AND "+\
               "  upc.upeople_id = pup.upeople_id AND "+\
               "  c.id = upc.country_id AND "+\
               "  i.status = 'merged' AND "+\
               "  i.submitted_on >="+  self.filters.startdate+ " AND "+\
               "  i.submitted_on < "+ self.filters.enddate+ " "+\
               "GROUP BY c.name "+\
               "ORDER BY issues DESC "
        return(self.db.ExecuteQuery(q))

class Domains(Metrics):
    id = "domains"
    name = "Domains"
    desc = "Number of domains with persons active in code review"
    data_source = SCR

    def _get_sql(self, evolutionary):
        pass

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
        q = "SELECT p.id AS name FROM  %s.projects p" % (self.db.identities_db)
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
        fields = " count(distinct(t.id)) as repositories"
        tables = "issues i, trackers t"
        tables += self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = "i.tracker_id = t.id "
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis)
        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " i.submitted_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def get_list  (self):
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


        if (days != 0 ):
            q = "SELECT @maxdate:=max(changed_on) from changes limit 1"
            self.db.ExecuteQuery(q)
            date_limit = " AND DATEDIFF(@maxdate, changed_on)<" + str(days)

        q = "SELECT u.id as id, u.identifier as reviewers, "+\
            "               count(distinct(c.id)) as reviewed "+\
            "        FROM people_upeople pup, changes c, "+ self.db.identities_db+".upeople u "+\
            "        WHERE "+ filter_bots+ " "+\
            "            c.changed_by = pup.people_id and "+\
            "            pup.upeople_id = u.id and "+\
            "            c.changed_on >= "+ startdate + " and "+\
            "            c.changed_on < "+ enddate + " "+\
            "            "+ date_limit + " "+\
            "        GROUP BY u.identifier "+\
            "        ORDER BY reviewed desc, reviewers "+\
            "        LIMIT " + str(limit)

        return(self.db.ExecuteQuery(q))



    def _get_sql(self, evolutionary):
        fields = " count(distinct(changed_by)) as reviewers "
        tables = " changes ch, issues i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters  = "ch.issue_id = i.id "
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ", people_upeople pup"
            filters += " and ch.changed_by  = pup.people_id"
        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ", people_upeople pup"
            filters += " and ch.changed_by = pup.people_id "


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
        fields = " count(distinct(changed_by)) as core_reviewers "
        tables = " changes ch, issues_ext_gerrit ieg, issues i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters  = "ch.issue_id = i.id and ieg.branch like '%master%' and ieg.issue_id = i.id "
        filters += self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ", people_upeople pup"
            filters += " and ch.changed_by  = pup.people_id"
        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ", people_upeople pup"
            filters += " and ch.changed_by = pup.people_id "

        filters += " and (ch.new_value = -2 or ch.new_value = 2) "
        filters += " and field = 'Code-Review' "
        q = self.db.BuildQuery (self.filters.period, self.filters.startdate,
                                self.filters.enddate, " ch.changed_on",
                                fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q


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


        merged_sql = " AND status='MERGED' "
        rol = "mergers"
        action = "merged"

        q = "SELECT u.id as id, u.identifier as "+rol+", "+\
            "            count(distinct(i.id)) as "+action+" "+\
            "        FROM people_upeople pup, issues i, "+self.db.identities_db+".upeople u "+\
            "        WHERE "+ filter_bots+ " "+\
            "            i.submitted_by = pup.people_id and "+\
            "            pup.upeople_id = u.id and "+\
            "            i.submitted_on >= "+ startdate+ " and "+\
            "            i.submitted_on < "+ enddate+ " "+\
            "            "+date_limit+ merged_sql+ " "+\
            "        GROUP BY u.identifier "+\
            "        ORDER BY "+action+" desc, id "+\
            "        LIMIT "+ str(limit)
        return(self.db.ExecuteQuery(q))


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
        tpeople_sql += " FROM issues i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters_ext = self.db.GetSQLReportWhere(self.filters.type_analysis) 
        if (filters_ext != ""):
            # Hack: remove "and "
            filters_ext = filters_ext[4:]
            tpeople_sql += " WHERE " + filters_ext


        fields = " count(distinct(upeople_id)) as submitters "
        tables = " people_upeople pup, (%s) tpeople " % (tpeople_sql)
        filters = " tpeople.submitted_by = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " tpeople.submitted_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q


    def __get_sql_default__(self, evolutionary):
        """ This function returns the evolution or agg number of people opening issues """
        fields = " count(distinct(pup.upeople_id)) as submitters "
        tables = " issues i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis is None or len (self.filters.type_analysis) != 2) :
            #Specific case for the basic option where people_upeople table is needed
            #and not taken into account in the initial part of the query
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id"
        elif (self.filters.type_analysis[0] == "repository" or self.filters.type_analysis[0] == "project"):
            #Adding people_upeople table
            tables += ", people_upeople pup"
            filters += " and i.submitted_by = pup.people_id "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " submitted_on ",
                               fields, tables, filters, evolutionary, self.filters.type_analysis)
        return q

    def _get_sql(self, evolutionary):
        if (self.filters.type_analysis is not None and (self.filters.type_analysis[0] in  ["repository","project"])):
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

        if (days != 0 ):
            q = "SELECT @maxdate:=max(submitted_on) from issues limit 1"
            self.db.ExecuteQuery(q)
            date_limit = " AND DATEDIFF(@maxdate, submitted_on)<"+str(days)

        q = "SELECT u.id as id, u.identifier as "+rol+", "+\
            "            count(distinct(i.id)) as "+action+" "+\
            "        FROM people_upeople pup, issues i, "+self.db.identities_db+".upeople u "+\
            "        WHERE "+ filter_bots+ " "+\
            "            i.submitted_by = pup.people_id and "+\
            "            pup.upeople_id = u.id and "+\
            "            i.submitted_on >= "+ startdate+ " and "+\
            "            i.submitted_on < "+ enddate+ " "+\
            "            "+date_limit +  " "+\
            "        GROUP BY u.identifier "+\
            "        ORDER BY "+action+" desc, id "+\
            "        LIMIT "+ str(limit)
        return(self.db.ExecuteQuery(q))

class TimeToReview(Metrics):
    id = "review_time"
    name = "Review Time"
    desc = "Time to review"
    data_source = SCR

    def _get_sql(self):
        if self.filters.period != "month": return None
        bots = []
        q = self.db.GetTimeToReviewQuerySQL (self.filters.startdate, self.filters.enddate,
                                             self.filters.type_analysis, bots)
        return q

    def get_agg(self):
        from numpy import median, average
        from GrimoireUtils import removeDecimals

        q = self._get_sql()
        if q is None: return {}
        data = self.db.ExecuteQuery(q)
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

    def get_ts(self):
        q = self._get_sql()
        if q is None: return {}
        review_list = self.db.ExecuteQuery(q)
        checkListArray(review_list)
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

    This class considers that a review process is a two states machine:
    - Waiting for a submitter action
    - Waiting for a reviewer action

    A patch is considered as waiting for a submitter action in the
    following conditions:
    - A Code-Review action is detected, being this a -1 or -2
    - A Verified action is detected, being this a -1 or -2

    A patch is considered as waiting for a reviewer action in the
    following conditions:
    - A new Patch upload is detected (eg: new patch, rebase or restore action)

    Existing limitations:
    - There are cases where the time of the review found in the db took place before
    the last upload. This takes places when trivial changes are requested to the
    patch submitter. Given this, those negative time-waiting-for-a-submitter-action
    are simply ignored from the final dataset. This takes place when using +2 or
    when using -2.
    - This analysis is based on the Changes table. There may appear issues whose life
    is out of the timeframe limits provided in the self.filters variable.

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

        filters.add("""(field='Upload' or
                   (field='Verified' and (new_value=-1 or new_value=-2)) or
                   (field='Code-Review' and (new_value=-1 or new_value=-2 or new_value=2)))""")
        filters.add("ch.changed_on >= " + self.filters.startdate)
        filters.add("ch.changed_on < " + self.filters.enddate)
        filters.add("ch.issue_id = i.id")

        # Migrating those sets to strings
        fields_str = self.db._get_fields_query(fields)
        tables_str = self.db._get_tables_query(tables)
        filters_str = self.db._get_filters_query(filters)

        # Adding extra filters
        tables_str = tables_str + " " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters_str = filters_str + " " + self.db.GetSQLReportWhere(self.filters.type_analysis)
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

            if current_state == 1:
                # First state: Waiting for a reviewer response
                if field == "Upload" and not is_new_review:
                    current_state = 1
                    # Time waiting for a reviewer action. Upload -> Upload
                    waiting4reviewer.append(changed_on - old_changed_on)
                    old_changed_on = changed_on
                if field == "Upload" and is_new_review:
                    current_state = 1
                    is_new_review = False
                    # Time waiting for a reviewer action. Given that there is a ne
                    # review, the time goes from the old_changed_on till the end
                    # of the timeframe analysis (max_date)
                    waiting4reviewer.append(max_date - old_changed_on)
                    if old_changed_on == -1:
                        # first case
                        waiting4reviewer = []
                    old_changed_on = changed_on
                if (field == "Code-Review" or "Verified") and (new_value == -1 or new_value == -2) and not is_new_review:
                    current_state = 2
                    # In any of these cases the reviewer gives up reviewing.
                    # The submitter needs to upload a new version
                    waiting4reviewer.append(changed_on - old_changed_on)
                    old_changed_on = changed_on
                if (field == "Code-Review" or "Verified") and (new_value == -1 or new_value == -2) and is_new_review:
                    current_state = 2
                    is_new_review = False
                    # There are 2 actions taking place here:
                    # - There's a reviewer that needed to review (Case 1)
                    # - And there's a new review that at some point (before the time of this analysis) (Case 2)
                    #   was uploaded.
                    waiting4reviewer.append(max_date - old_changed_on) # Case 1
                    waiting4reviewer.append(changed_on - min_date) # Case 2
                    old_changed_on = changed_on
                if field == "Code-Review" and new_value == 2 and not is_new_review:
                    current_state = 3
                    # The review finishes at this point.
                    waiting4reviewer.append(changed_on - old_changed_on)
                if field == "Code-Review" and new_value == 2 and is_new_review:
                    current_state = 3
                    is_new_review = False
                    # There are 2 actions taking place here:
                    # - There's a reviewer that needed to review (Case 1)
                    # - And there's a new review that at some point (before the time of this analysis) (Case 2)
                    #   was uploaded
                    waiting4reviewer.append(max_date - old_changed_on) # Case 1
                    waiting4reviewer.append(changed_on - min_date) # Case 2
                    old_changed_on = changed_on
            elif current_state == 2:
                # Second state: Waiting for a submitter response
                if field == "Upload" and not is_new_review:
                    current_state = 1
                    # A new upload was updated, the time waiting for the submitter ends here
                    waiting4submitter.append(changed_on - old_changed_on)
                    old_changed_on = changed_on
                if field == "Upload" and is_new_review:
                    current_state = 1
                    is_new_review = False
                    # Two actions take place here:
                    # - A submitter is still required to act
                    # - A new review is submitted
                    waiting4submitter.append(max_date - old_changed_on)
                    old_changed_on = changed_on
                if (field == "Code-Review" or "Verified") and (new_value == -1 or new_value == -2) and not is_new_review:
                    current_state = 2
                    # This implies extra actions on the same changeset and patchset
                    # Thus, this is still time waiting for a submitter action
                    # The value of old_changed_on keeps the same.
                    pass
                if (field == "Code-Review" or "Verified") and (new_value == -1 or new_value == -2) and is_new_review:
                    current_state = 2
                    is_new_review = False
                    # Two actions take place in this case:
                    # - An action by the submitter is still required.
                    # - And a new review started before the period of this analysis.
                    #   Thus, there should be a previous Upload action
                    waiting4submitter.append(max_date - old_changed_on)
                    waiting4reviewer.append(changed_on - min_date)
                    old_changed_on = changed_on
                if field == "Code-Review" and new_value == 2 and not is_new_review:
                    current_state = 3
                    # In some cases, during the time waiting for a submitter action
                    # a +2 review may appear. Thus, the time waiting for a submitter ends
                    waiting4submitter.append(changed_on - old_changed_on)
                    old_changed_on = changed_on
                if field == "Code-Review" and new_value == 2 and is_new_review:
                    current_state = 3
                    is_new_review = False
                    # Two actions take place in this case:
                    # - A previous action by a submitter is required
                    # - A new review took place by another reviewer.
                    waiting4submitter.append(max_date - old_changed_on)
                    waiting4reviewer.append(changed_on - min_date)
                    old_changed_on = changed_on
            elif current_state == 3:
                # Third state: end of review. It is assumed that any action after the
                # Code-Review +2 does not matter.
                if field == "Upload" and not is_new_review:
                    # If this is not a new review, the review is closed as assumption
                    pass
                if field == "Upload" and is_new_review:
                    current_state = 1
                    is_new_review = False
                    old_changed_on = changed_on
                if (field == "Code-Review" or "Verified") and (new_value == -1 or new_value == -2) and not is_new_review:
                    # If this is not a new review, the review is closed as assumption
                    pass
                if (field == "Code-Review" or "Verified") and (new_value == -1 or new_value == -2) and is_new_review:
                    current_state = 2
                    is_new_review = False
                    # Two actions takes place:
                    # - the current review process finishes (case 1 nothing done)
                    # - a new one appears whose upload patch took place before the
                    #   period of this analysis (case 2)
                    waiting4reviewer.append(changed_on - min_date) # case 2
                    old_changed_on = changed_on
                if field == "Code-Review" and new_value == 2 and not is_new_review:
                    # If this is not a new review, the review is already closed
                    pass
                if field == "Code-Review" and new_value == 2 and is_new_review:
                    current_state = 3
                    is_new_review = False
                    # Two actions take place in this case:
                    # - The current review finishes (case 1, no action required)
                    # - A new review appears, and the upload took place
                    #   before the timeframe analysis (case 2)
                    waiting4reviewer.append(changed_on - min_date) # case 2
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

    filters = MetricFilters("month", "'2014-07-01'", "'2014-09-01'", None)
    dbcon = SCRQuery("root", "", "dic_bicho_gerrit_openstack_3359_bis3", "dic_cvsanaly_openstack_4114")

    timewaiting = TimeToReviewPatch(dbcon, filters)
    print timewaiting.get_agg()

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
