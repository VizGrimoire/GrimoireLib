## Copyright (C) 2013-2014 Bitergia
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
##
## Queries for source code review data analysis
##
##
## Authors:
##   Alvaro del Castillo San Felix <acs@bitergia.com>

import logging, os, sys
from numpy import median, average
import time
from datetime import datetime, timedelta

from vizgrimoire.GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from vizgrimoire.GrimoireSQL import ExecuteQuery
from vizgrimoire.GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds
from vizgrimoire.GrimoireUtils import checkListArray, removeDecimals, get_subprojects
from vizgrimoire.GrimoireUtils import getPeriod, createJSON, checkFloatArray, medianAndAvgByPeriod, check_array_values
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.metrics.query_builder import DSQuery


from vizgrimoire.data_source import DataSource
from vizgrimoire.filter import Filter


class Pullpo(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_pullpo"

    @staticmethod
    def get_name(): return "pullpo"

    @staticmethod
    def get_metrics_not_filters():
        metrics_not_filters =  ['repositories']
        return metrics_not_filters

    @staticmethod
    def get_date_init(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        q = " SELECT DATE_FORMAT (MIN(created_at), '%Y-%m-%d') as first_date FROM pull_requests"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        q = " SELECT DATE_FORMAT (MAX(updated_at), '%Y-%m-%d') as last_date FROM pull_requests"
        return(ExecuteQuery(q))

    @staticmethod
    def get_url():
        q = "select url,name from repositories limit 1"
        return (ExecuteQuery(q))

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        metrics = DataSource.get_metrics_data(Pullpo, period, startdate, enddate, identities_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(Pullpo, period, startdate, enddate, True)
        evol_data = dict(metrics.items()+studies.items())

        return evol_data

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  Pullpo.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = Pullpo().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_= None):
        metrics = DataSource.get_metrics_data(Pullpo, period, startdate, enddate, 
                                              identities_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(Pullpo, period, startdate, enddate, False)
        agg = dict(metrics.items()+studies.items())

        if (filter_ is None):
            static_url = Pullpo.get_url()
            agg = dict(agg.items() + static_url.items())

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_= None):
        data = Pullpo.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = Pullpo().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))


    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = Pullpo.get_bots()
        top_all = None
        mreviewers = DataSource.get_metrics("reviewers", Pullpo)
        mopeners = DataSource.get_metrics("submitters", Pullpo)
        mmergers = DataSource.get_metrics("closers", Pullpo)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top_reviewers = {}
            top_reviewers['reviewers'] = mreviewers.get_list(mfilter, 0)
            top_reviewers['reviewers.last month']= mreviewers.get_list(mfilter, 31)
            top_reviewers['reviewers.last year']= mreviewers.get_list(mfilter, 365)

            top_openers = {}
            top_openers['openers.'] = mopeners.get_list(mfilter, 0)
            top_openers['openers.last_month']= mopeners.get_list(mfilter, 31)
            top_openers['openers.last year'] = mopeners.get_list(mfilter, 365)

            top_mergers = {}
            top_mergers['mergers.'] = mmergers.get_list(mfilter, 0)
            top_mergers['mergers.last_month'] = mmergers.get_list(mfilter, 31)
            top_mergers['mergers.last year'] = mmergers.get_list(mfilter, 365)

            # The order of the list item change so we can not check it
            top_all = dict(top_reviewers.items() +  top_openers.items() + top_mergers.items())
        else:
            logging.info("Pullpo does not support yet top for filters.")

        return (top_all)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = Pullpo.get_top_data (startdate, enddate, i_db, None, npeople)
        createJSON (data, destdir+"/"+Pullpo().get_top_filename())

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            metric = DataSource.get_metrics("repositories", Pullpo)
        elif (filter_name == "company"):
            metric = DataSource.get_metrics("companies", Pullpo)
        elif (filter_name == "country"):
            metric = DataSource.get_metrics("countries", Pullpo)
        elif (filter_name == "project"):
            metric = DataSource.get_metrics("projects", Pullpo)
        elif (filter_name == "people2"):
            metric = DataSource.get_metrics("people2", Pullpo)
        else:
            logging.error("Pullpo " + filter_name + " not supported")
            return items

        items = metric.get_list()
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items = Pullpo.get_filter_items(filter_, startdate, enddate, identities_db)
            if (items == None): return
            items = items['name']

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        # For repos aggregated data. Include metrics to sort in javascript.
        if (filter_name == "repository"):
            items_list = {"name":[],"review_time_days_median":[],"submitted":[]}
        else:
            items_list = items

        for item in items :
            item_file = item.replace("/","_")
            if (filter_name == "repository"):
                items_list["name"].append(item_file)

            logging.info (item)
            filter_item = Filter(filter_name, item)

            evol = Pullpo.get_evolutionary_data(period, startdate, enddate, 
                                               identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(Pullpo()))
            createJSON(evol, fn)

            # Static
            agg = Pullpo.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(Pullpo()))
            createJSON(agg, fn)
            if (filter_name == "repository"):
                if 'submitted' in agg: 
                    items_list["submitted"].append(agg["submitted"])
                else: items_list["submitted"].append("NA")
                if 'review_time_days_median' in agg: 
                    items_list["review_time_days_median"].append(agg['review_time_days_median'])
                else: items_list["submitted"].append("NA")

        fn = os.path.join(destdir, filter_.get_filename(Pullpo()))
        createJSON(items_list, fn)

    @staticmethod
    def _check_report_all_data(data, filter_, startdate, enddate, idb,
                               evol = False, period = None):
        pass

    @staticmethod
    def create_filter_report_all(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        check = False # activate to debug pull_requests
        filter_name = filter_.get_name()

        if filter_name == "people2" or filter_name == "company":
            filter_all = Filter(filter_name, None)
            agg_all = Pullpo.get_agg_data(period, startdate, enddate,
                                       identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_static_filename_all(Pullpo()))
            createJSON(agg_all, fn)

            evol_all = Pullpo.get_evolutionary_data(period, startdate, enddate,
                                                 identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_evolutionary_filename_all(Pullpo()))
            createJSON(evol_all, fn)

            if check:
                Pullpo._check_report_all_data(evol_all, filter_, startdate, enddate,
                                           identities_db, True, period)
                Pullpo._check_report_all_data(agg_all, filter_, startdate, enddate,
                                           identities_db, False, period)
        else:
            raise Exception(filter_name +" does not support yet group by items sql queries")


    # Unify top format
    @staticmethod
    def _safeTopIds(top_data_period):
        if not isinstance(top_data_period['id'], (list)):
            for name in top_data_period:
                top_data_period[name] = [top_data_period[name]]
        return top_data_period['id']

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):

        top_data = Pullpo.get_top_data (startdate, enddate, identities_db, None, npeople)

        top  = Pullpo._safeTopIds(top_data['reviewers'])
        top += Pullpo._safeTopIds(top_data['reviewers.last year'])
        top += Pullpo._safeTopIds(top_data['reviewers.last month'])
        top += Pullpo._safeTopIds(top_data['openers.'])
        top += Pullpo._safeTopIds(top_data['openers.last year'])
        top += Pullpo._safeTopIds(top_data['openers.last_month'])
        top += Pullpo._safeTopIds(top_data['mergers.'])
        top += Pullpo._safeTopIds(top_data['mergers.last year'])
        top += Pullpo._safeTopIds(top_data['mergers.last_month'])
        # remove duplicates
        people = list(set(top)) 

        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        evol = Pullpo.get_people_query(upeople_id, startdate, enddate, True, period)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        agg = Pullpo.get_people_query(upeople_id, startdate, enddate)
        return agg

    # TODO: this should be done using people filter metrics
    @staticmethod
    def get_people_query(developer_id, startdate, enddate, evol = False, period = None):
        query_builder = Pullpo.get_query_builder()
        fields ='COUNT(distinct(pr.id)) AS submissions'
        tables = 'pull_requests pr, people_upeople pup'
        filters = 'pr.user_id = pup.people_id'
        filters +=" AND pup.upeople_id="+str(developer_id)
        if (evol) :
            q = GetSQLPeriod(period,'pr.created_at', fields, tables, filters,
                    startdate, enddate)
        else :
            fields += ",DATE_FORMAT (min(pr.created_at),'%Y-%m-%d') as first_date, "+\
                      "DATE_FORMAT (max(pr.created_at),'%Y-%m-%d') as last_date"
            q = GetSQLGlobal('pr.created_at', fields, tables, filters, 
                             startdate, enddate)

        data  = ExecuteQuery(q)
        return (data)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_query_builder():
        from vizgrimoire.metrics.query_builder import PullpoQuery
        return PullpoQuery

    @staticmethod
    def get_metrics_core_agg():
        m =  ['submitted','opened','closed','merged','abandoned','new','inprogress',
              'pending','review_time','repositories']
        # patches metrics
        m += ['verified','approved','codereview','sent','WaitingForReviewer','WaitingForSubmitter']
        m += ['submitters','reviewers']

        return m

    @staticmethod
    def get_metrics_core_ts():
        m  = ['submitted','opened','closed','merged','abandoned','new','pending','review_time','repositories']
        # Get metrics using changes table for more precise results
        m += ['merged','abandoned','new']
        m += ['verified','codereview','sent','WaitingForReviewer','WaitingForSubmitter']
        m += ['submitters','reviewers']

        return m

    @staticmethod
    def get_metrics_core_trends():
        return ['submitted','merged','pending','abandoned','closed','submitters']

