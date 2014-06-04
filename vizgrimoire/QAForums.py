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

import os

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod, ExecuteQuery, BuildQuery

from GrimoireUtils import GetPercentageDiff, GetDates, getPeriod, createJSON, completePeriodIds

from data_source import DataSource

from filter import Filter

from metrics_filter import MetricFilters


class QAForums(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_qaforums"

    @staticmethod
    def get_name():
        return "qaforums"

    @staticmethod
    def get_date_init():
        """Get the date of the first activity in the data source"""
        q = "SELECT DATE_FORMAT (MIN(added_at), '%Y-%m-%d') AS date FROM questions"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end():
        """Get the date of the last activity in the data source"""
        q1 = "SELECT MAX(added_at) AS aq FROM questions"
        q2 = "SELECT MAX(submitted_on) AS sc FROM comments"
        q3 = "SELECT MAX(submitted_on) AS sa FROM answers"
        q = "SELECT DATE_FORMAT (GREATEST(aq, sc, sa), '%%Y-%%m-%%d') AS date FROM (%s) q, (%s) c, (%s) a" % (q1, q2, q3)
        return(ExecuteQuery(q))

    @staticmethod
    def __get_date_field(table_name):
        # the tables of the Sibyl tool are not coherent among them
        #so we have different fields for the date of the different posts
        if (table_name == "questions"):
            return "added_at"
        elif (table_name == "answers"):
            return "submitted_on"
        elif (table_name == "comments"):
            return "submitted_on"
        # FIXME add exceptions here

    @staticmethod
    def __get_author_field(table_name):
        # the tables of the Sibyl tool are not coherent among them
        #so we have different fields for the author ids of the different posts
        if (table_name == "questions"):
            return "author_identifier"
        elif (table_name == "answers"):
            return "user_identifier"
        elif (table_name == "comments"):
            return "user_identifier"
        # FIXME add exceptions here

    @staticmethod
    def __get_data (period, startdate, enddate, i_db, filter_, evol):
        data = {}

        type_analysis = None
        if (filter_ is not None):
            type_analysis = [filter_.get_name(), filter_.get_item()]

        metrics_on = ['qsent','asent','csent','qsenders','asenders','csenders']
        mfilter = MetricFilters(period, startdate, enddate, type_analysis)
        all_metrics = QAForums.get_metrics_set(QAForums)

        for item in all_metrics:
            if item.id not in metrics_on: continue
            item.filters = mfilter
            if evol is False:
                mvalue = item.get_agg()
            else:
                mvalue = item.get_ts()
            data = dict(data.items() + mvalue.items())

            if evol is False:
                # Tendencies
                metrics_trends = ['qsent','asent','csent','qsenders','asenders','csenders']

                for i in [7,30,365]:
                    for item in all_metrics:
                        if item.id not in metrics_trends: continue
                        period_data = item.get_agg_diff_days(enddate, i)
                        data = dict(data.items() +  period_data.items())

                if (filter_ is None):
                    data["init_date"] = QAForums.get_date_init()['date']
                    data["last_date"] = QAForums.get_date_end()['date']

        return data

    @staticmethod
    def get_top_senders(days, startdate, enddate, identities_db, bots, limit, type_post):
        # FIXME: neither using unique identities nor filtering bots
        table_name = type_post
        date_field = QAForums.__get_date_field(table_name)
        author_field = QAForums.__get_author_field(table_name)
        date_limit = ""

        if (days != 0):
            sql = "SELECT @maxdate:=max(%s) from %s limit 1" % (date_field, table_name)
            res = ExecuteQuery(sql)
            date_limit = " AND DATEDIFF(@maxdate, %s) < %s" % (date_field, str(days))
            #end if

        select = "SELECT %s AS id, p.username AS senders, COUNT(%s.id) AS sent" % \
          (author_field, table_name)
        fromtable = " FROM %s, people p" % (table_name)        
        filters = " WHERE %s = p.identifier AND %s >= %s AND %s < %s " % \
          (author_field, date_field, startdate, date_field, enddate)          

        tail = " GROUP BY senders ORDER BY sent DESC, senders LIMIT %s" % (limit)
        q = select + fromtable + filters + date_limit + tail
        return(ExecuteQuery(q))

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
    def get_top_data(startdate, enddate, identities_db, filter_, npeople):
        bots = QAForums.get_bots()

        top_senders = {}
        top_senders['csenders.'] = \
            QAForums.get_top_senders(0, startdate, enddate, identities_db, bots, npeople, "comments")
        top_senders['csenders.last year'] = \
            QAForums.get_top_senders(365, startdate, enddate, identities_db, bots, npeople, "comments")
        top_senders['csenders.last month'] = \
            QAForums.get_top_senders(31, startdate, enddate, identities_db, bots, npeople, "comments")

        top_senders['qsenders.'] = \
            QAForums.get_top_senders(0, startdate, enddate, identities_db, bots, npeople, "questions")
        top_senders['qsenders.last year'] = \
            QAForums.get_top_senders(365, startdate, enddate, identities_db, bots, npeople, "questions")
        top_senders['qsenders.last month'] = \
            QAForums.get_top_senders(31, startdate, enddate, identities_db, bots, npeople, "questions")

        top_senders['asenders.'] = \
            QAForums.get_top_senders(0, startdate, enddate, identities_db, bots, npeople, "answers")
        top_senders['asenders.last year'] = \
            QAForums.get_top_senders(365, startdate, enddate, identities_db, bots, npeople, "answers")
        top_senders['asenders.last month'] = \
            QAForums.get_top_senders(31, startdate, enddate, identities_db, bots, npeople, "answers")

        return(top_senders)

    @staticmethod
    def create_top_report(startdate, enddate, destdir, npeople, i_db):
        data = QAForums.get_top_data(startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+QAForums().get_top_filename()
        createJSON(data, top_file)

    @staticmethod
    def get_metrics_definition ():
        pass

    @staticmethod
    def tags_name(startdate, enddate):
        # Returns list of tags
        query = "select tag as name from tags"
        query = """select t.tag as name, 
                          count(distinct(qt.question_identifier)) as total 
                   from tags t, 
                        questionstags qt,
                        questions q 
                   where t.id=qt.tag_id and
                         qt.question_identifier = q.question_identifier and
                         q.added_at >= %s and
                         q.added_at < %s
                   group by t.tag 
                   having total > 20
                   order by total desc, name;""" % (startdate, enddate)
        data = ExecuteQuery(query)
        return data

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()
        #TODO: repository needs to be change to tag, once this is accepted as new
        #      data source in VizGrimoireJS-lib
        if (filter_name == "repository"):
            items = QAForums.tags_name(startdate, enddate)
        else:
            logging.error(filter_name + "not supported")

        return items

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        return []

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        return []

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items =  QAForums.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if items == None:
            return
        items = items['name']
  
        filter_name = filter_.get_name()

        if not isinstance(items, list):
            items = [items]

        fn = os.path.join(destdir, filter_.get_filename(QAForums()))
        createJSON(items, fn)
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
    def get_query_builder ():
        from query_builder import QAForumsQuery
        return QAForumsQuery