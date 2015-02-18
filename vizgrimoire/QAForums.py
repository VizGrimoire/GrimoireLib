#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>
#     Daniel Izquierdo <dizquierdo@bitergia.com>
#     Luis Cañas-Díaz <lcanas@bitergia.com>

import logging

import os, re

import datetime

from vizgrimoire.GrimoireSQL import GetSQLGlobal, GetSQLPeriod, ExecuteQuery, BuildQuery

from vizgrimoire.GrimoireUtils import GetPercentageDiff, GetDates, getPeriod, createJSON, completePeriodIds

from vizgrimoire.data_source import DataSource

from vizgrimoire.filter import Filter

from vizgrimoire.metrics.metrics_filter import MetricFilters


class QAForums(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        # return "db_qaforums"
        return "db_sibyl"

    @staticmethod
    def get_name():
        return "qaforums"

    @staticmethod
    def get_date_init(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        """Get the date of the first activity in the data source"""
        q = "SELECT DATE_FORMAT (MIN(added_at), '%Y-%m-%d') AS first_date FROM questions"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        """Get the date of the last activity in the data source"""
        q1 = "SELECT MAX(added_at) AS aq FROM questions"
        q2 = "SELECT MAX(submitted_on) AS sc FROM comments"
        q3 = "SELECT MAX(submitted_on) AS sa FROM answers"
        q = "SELECT DATE_FORMAT (GREATEST(aq, sc, sa), '%%Y-%%m-%%d') AS last_date FROM (%s) q, (%s) c, (%s) a" % (q1, q2, q3)
        return(ExecuteQuery(q))

    @staticmethod
    def __get_data (period, startdate, enddate, i_db, filter_, evol):
        metrics =  DataSource.get_metrics_data(QAForums, period, startdate, enddate, i_db, filter_, evol)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_studies_data(QAForums, period, startdate, enddate, evol)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        return QAForums.__get_data(period, startdate, enddate, identities_db, filter_, True)

    @staticmethod
    def create_evolutionary_report(period, startdate, enddate, destdir, identities_db, filter_ = None):
        data =  QAForums.get_evolutionary_data(period, startdate, enddate, identities_db, filter_)
        filename = QAForums().get_evolutionary_filename()
        createJSON(data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data(period, startdate, enddate, identities_db, filter_=None):
        return QAForums.__get_data(period, startdate, enddate, identities_db, filter_, False)

    @staticmethod
    def create_agg_report(period, startdate, enddate, destdir, i_db, filter_ = None):
        data = QAForums.get_agg_data(period, startdate, enddate, i_db, filter_)
        filename = QAForums().get_agg_filename()
        createJSON(data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_metrics ():
        return ["csenders","asenders","qsenders","participants"]

    @staticmethod
    def get_top_data(startdate, enddate, identities_db, filter_, npeople):
        top = {}
        mcsenders = DataSource.get_metrics("csenders", QAForums)
        masenders = DataSource.get_metrics("asenders", QAForums)
        mqsenders = DataSource.get_metrics("qsenders", QAForums)
        mparticipants = DataSource.get_metrics("participants", QAForums)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top['csenders.'] = mcsenders.get_list(mfilter, 0)
            top['csenders.last month'] = mcsenders.get_list(mfilter, 31)
            top['csenders.last year'] = mcsenders.get_list(mfilter, 365)

            top['asenders.'] = masenders.get_list(mfilter, 0)
            top['asenders.last month'] = masenders.get_list(mfilter, 31)
            top['asenders.last year'] = masenders.get_list(mfilter, 365)

            top['qsenders.'] = mqsenders.get_list(mfilter, 0)
            top['qsenders.last month'] = mqsenders.get_list(mfilter, 31)
            top['qsenders.last year'] = mqsenders.get_list(mfilter, 365)

            top['participants.'] = mparticipants.get_list(mfilter, 0)
            top['participants.last month'] = mparticipants.get_list(mfilter, 31)
            top['participants.last year'] = mparticipants.get_list(mfilter, 365)

        else:
            logging.info("QAForums does not support yet top for filters.")

        return top

    @staticmethod
    def create_top_report(startdate, enddate, destdir, npeople, i_db):
        data = QAForums.get_top_data(startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+QAForums().get_top_filename()
        createJSON(data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()
        #TODO: repository needs to be change to tag, once this is accepted as new
        #      data source in VizGrimoireJS-lib
        if (filter_name == "repository"):
            metric = DataSource.get_metrics("tags", QAForums)
            # items = QAForums.tags_name(startdate, enddate)
            items = metric.get_list()
        if (filter_name == "people2"):
            metric = DataSource.get_metrics("participants", QAForums)
            items = metric.get_list()
        else:
            logging.error("QAForums " + filter_name + " not supported")

        return items

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        return []

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        return []

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items =  QAForums.get_filter_items(filter_, startdate, enddate, identities_db)
            if items == None:
                return
            items = items['name']
  
        filter_name = filter_.get_name()

        if not isinstance(items, list):
            items = [items]

        file_items = []
        for item in items:
            if re.compile("^\..*").match(item) is not None: item = "_"+item
            file_items.append(item)

        fn = os.path.join(destdir, filter_.get_filename(QAForums()))
        createJSON(file_items, fn)
        for item in items:
            logging.info(item)
            filter_item = Filter(filter_.get_name(), item)

            evol_data = QAForums.get_evolutionary_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(QAForums()))
            createJSON(completePeriodIds(evol_data, period, startdate, enddate), fn)

            agg = QAForums.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(QAForums()))
            createJSON(agg, fn)

    @staticmethod
    def create_filter_report_all(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        filter_name = filter_.get_name()
        if filter_name == "people2" or filter_name == "company_off":
            filter_all = Filter(filter_name, None)
            agg_all = QAForums.get_agg_data(period, startdate, enddate,
                                            identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_static_filename_all(QAForums()))
            createJSON(agg_all, fn)

            evol_all = QAForums.get_evolutionary_data(period, startdate, enddate,
                                                 identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_evolutionary_filename_all(QAForums()))
            createJSON(evol_all, fn)
        else:
            logging.error(QAForums.get_name()+ " " + filter_name +" does not support yet group by items sql queries")


    @staticmethod
    def get_query_builder ():
        from vizgrimoire.metrics.query_builder import QAForumsQuery
        return QAForumsQuery

    @staticmethod
    def get_metrics_core_agg():
        return ['qsent','asent','csent','qsenders','asenders','csenders','participants']

    @staticmethod
    def get_metrics_core_ts():
        return ['qsent','asent','csent', 'unanswered', 'qsenders','asenders','csenders','participants']

    @staticmethod
    def get_metrics_core_trends():
        return ['qsent','asent','csent','qsenders','asenders','csenders','participants']
