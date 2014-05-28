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

""" Metrics for the source code review system """

import logging
import MySQLdb

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import ITSQuery

from SCR import SCR

class Submitted(Metrics):
    id = "submitted"
    name = "Submitted reviews"
    desc = "Number of submitted code review processes"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "submitted",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q

class Merged(Metrics):
    id = "merged"
    name = "Merged changes"
    desc = "Number of changes merged into the source code"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "merged",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q

class Mergers(Metrics):
    id = "mergers"
    name = "Successful submitters"
    desc = "Number of persons submitting changes that got accepted"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Abandoned(Metrics):
    id = "abandoned"
    name = "Abandoned reviews"
    desc = "Number of abandoned review processes"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "abandoned",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q

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

        pending = submitted['submitted']-merged['merged']-abandoned['abandoned']
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

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "opened",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q

class Closed(Metrics):
    id = "closed"
    name = "Closed reviews"
    desc = "Number of closed review processes (merged or abandoned)"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "closed",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q

class InProgress(Metrics):
    id = "inprogress"
    name = "In progress reviews"
    desc = "Number review processes in progress"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "inprogress",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q


class New(Metrics):
    id = "new"
    name = "New reviews"
    desc = "Number of new review processes"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        q = self.db.GetReviewsSQL(self.filters.period, self.filters.startdate,
                                  self.filters.enddate, "new",
                                  self.filters.type_analysis, evolutionary, self.db.identities_db)
        return q

class Verified(Metrics):
    id = "verified"
    name = "Verified reviews"
    desc = "Number of verified review processes"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Approved(Metrics):
    id = "approved"
    name = "Approved reviews"
    desc = "Number of approved review processes"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class CodeReview(Metrics):
    id = "codereview"
    name = "Code review"
    desc = "Number of review processes in code review state"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class WaitingForReviewer(Metrics):
    id = "WaitingForReviewer"
    name = "Waiting for reviewer"
    desc = "Number of patches from review processes waiting for reviewer"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class WaitingForSubmitter(Metrics):
    id = "WaitingForSubmitter"
    name = "Waiting for submitter"
    desc = "Number of patches from review processes waiting for submitter"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class ReviewsWaitingForReviewer(Metrics):
    id = "ReviewsWaitingForReviewer"
    name = "Reviews waiting for reviewer"
    desc = "Number of preview processes waiting for reviewer"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class ReviewsWaitingForSubmitter(Metrics):
    id = "ReviewsWaitingForSubmitter"
    name = "Reviews waiting for submitter"
    desc = "Number of review processes waiting for submitter"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Companies(Metrics):
    id = "companies"
    name = "Organizations"
    desc = "Number of organizations (companies, etc.) with persons active in code review"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Countries(Metrics):
    id = "countries"
    name = "Countries"
    desc = "Number of countries with persons active in code review"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Domains(Metrics):
    id = "domains"
    name = "Domains"
    desc = "Number of domains with persons active in code review"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Repositories(Metrics):
    id = "repositories"
    name = "Repositories"
    desc = "Number of repositories with persons active in code review"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class People(Metrics):
    id = "people"
    name = "People"
    desc = "Number of persons active in code review activities"
    data_source = SCR

    def __get_sql__(self, evolutionary):
        pass

class Closers(Metrics):
    id = "closers"
    name = "Closers"
    desc = "Number of persons closing code review activities"
    data_source = SCR
    action = "closed"

    def __get_sql__(self, evolutionary):
        pass

class Openers(Metrics):
    id = "openers"
    name = "Openers"
    desc = "Number of persons opening code review activities"
    data_source = SCR
    action = "opened"

    def __get_sql__(self, evolutionary):
        pass