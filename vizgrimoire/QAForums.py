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
# This file is a part of the vizGrimoire.R package
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


class QAForums(DataSource):

    # @staticmethod
    # def get_db_name():
    #     return "db_qaforums"

    @staticmethod
    def get_name():
        return "qaforums"

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
            return "user_id"
        # FIXME add exceptions here

    @staticmethod
    def __get_metric_name(type_post, suffix):
        metric_str = ""
        if (type_post == "questions"):
            metric_str = "q"
        elif (type_post == "answers"):
            metric_str = "a"
        elif (type_post == "comments"):
            metric_str = "c"
        metric_str += suffix
        #else: raise UnexpectedParameter
        return metric_str
        
    @staticmethod
    def get_sent(period, startdate, enddate, identities_db, type_analysis, evolutionary,
                 type_post = "questions"):
        # type_post has to be "comment", "question", "answer"

        date_field = QAForums.__get_date_field(type_post)
        date_field = " " + date_field + " "
        tables = " " + str(type_post)
        fields = " COUNT(DISTINCT(id)) as sent"
        if ( type_post == "questions"):
            filters = GetSQLReportWhere(type_analysis, "author_identifier")
        elif ( type_post == "answers"):
            filters = GetSQLReportWhere(type_analysis, "user_identifier")
        else:
            filters = GetSQLReportWhere(type_analysis, "user_id")
        #end if
        q = BuildQuery(period, startdate, enddate, date_field, fields, tables, filters, evolutionary)
        return(ExecuteQuery(q))

    @staticmethod
    def get_senders(period, startdate, enddate, identities_db, type_analysis, evolutionary,
                    type_post = "questions"):
        table_name = type_post
        date_field = QAForums.__get_date_field(table_name)
        author_field = QAForums.__get_author_field(table_name)
        
        fields = " count(distinct(%s)) as senders " % (author_field)
        tables = " " + table_name + " " + GetSQLReportFrom(identities_db, type_analysis)
        filters = GetSQLReportWhere(type_analysis, author_field)
        q = BuildQuery(period, startdate, enddate, date_field, fields, tables, filters, evolutionary)
        return(ExecuteQuery(q))

    @staticmethod
    def static_num_sent(period, startdate, enddate, identities_db=None, type_analysis=[],
                        type_post = "questions"):
        table_name = type_post #type_post matches the name of the table
        date_field = QAForums.__get_date_field(table_name)
        
        fields = "SELECT count(id) as sent, \
        DATE_FORMAT (min(" + date_field + "), '%Y-%m-%d') as first_date, \
        DATE_FORMAT (max(" + date_field + "), '%Y-%m-%d') as last_date "
        tables = " FROM %s " % (table_name)
        filters = "WHERE %s >= %s AND %s < %s " % (date_field, startdate, date_field, enddate)
        q = fields + tables + filters
        return(ExecuteQuery(q))

    @staticmethod
    def static_num_senders(period, startdate, enddate, identities_db=None, type_analysis=[],
                           type_post = "questions"):
        table_name = type_post #type_post matches the name of the table
        date_field = QAForums.__get_date_field(table_name)
        author_field = QAForums.__get_author_field(table_name)
        
        fields = "SELECT count(distinct(%s)) as senders" % (author_field)
        tables = " FROM %s " % (table_name)
        filters = "WHERE %s >= %s AND %s < %s " % (date_field, startdate, date_field, enddate)
        q = fields + tables + filters
        return(ExecuteQuery(q))

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
    def evol_qsent(period, startdate, enddate, identities_db, type_analysis):
        return(QAForums.get_sent(period, startdate, enddate, identities_db, type_analysis, True, "questions"))

    @staticmethod
    def evol_asent(period, startdate, enddate, identities_db, type_analysis):
        return(QAForums.get_sent(period, startdate, enddate, identities_db, type_analysis, True, "answers"))

    @staticmethod
    def evol_csent(period, startdate, enddate, identities_db, type_analysis):
        return(QAForums.get_sent(period, startdate, enddate, identities_db, type_analysis, True, "comments"))

    ###
    @staticmethod
    def evol_csenders(period, startdate, enddate, identities_db, type_analysis):
        return(QAForums.get_senders(period, startdate, enddate, identities_db, type_analysis, True, "comments"))

    @staticmethod
    def evol_qsenders(period, startdate, enddate, identities_db, type_analysis):
        return(QAForums.get_senders(period, startdate, enddate, identities_db, type_analysis, True, "questions"))

    @staticmethod
    def evol_asenders(period, startdate, enddate, identities_db, type_analysis):
        return(QAForums.get_senders(period, startdate, enddate, identities_db, type_analysis, True, "answers"))

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        # get number ofquestions, answers and comments over time
        asent = QAForums.evol_asent(period, startdate, enddate, identities_db, None)
        qsent = QAForums.evol_qsent(period, startdate, enddate, identities_db, None)        
        csent = QAForums.evol_csent(period, startdate, enddate, identities_db, None)
        asenders = QAForums.evol_asenders(period, startdate, enddate, identities_db, None)
        qsenders = QAForums.evol_qsenders(period, startdate, enddate, identities_db, None)        
        csenders = QAForums.evol_csenders(period, startdate, enddate, identities_db, None)        

        # we rename the keys of the dicts
        asent['asent'] = asent.pop('sent')
        qsent['qsent'] = qsent.pop('sent')
        csent['csent'] = csent.pop('sent')
        asenders['asenders'] = asenders.pop('senders')
        qsenders['qsenders'] = qsenders.pop('senders')
        csenders['csenders'] = csenders.pop('senders')

        asent = completePeriodIds(asent, period, startdate, enddate)
        qsent = completePeriodIds(qsent, period, startdate, enddate)
        csent = completePeriodIds(csent, period, startdate, enddate)
        asenders = completePeriodIds(asenders, period, startdate, enddate)
        qsenders = completePeriodIds(qsenders, period, startdate, enddate)
        csenders = completePeriodIds(csenders, period, startdate, enddate)

        evol_data = dict(asent.items() + qsent.items() + csent.items() +
                         asenders.items() + qsenders.items() + csenders.items())

        return (evol_data)

    @staticmethod
    def create_evolutionary_report(period, startdate, enddate, destdir, identities_db, filter_ = None):
        data =  QAForums.get_evolutionary_data(period, startdate, enddate, identities_db, filter_)
        filename = QAForums().get_evolutionary_filename()
        createJSON(data, os.path.join(destdir, filename))

    @staticmethod
    def get_diff_sent_days(period, init_date, days, type_post="questions"):
        # This function provides the percentage in activity between two periods.
        #
        # The netvalue indicates if this is an increment (positive value) or decrement (negative value)

        chardates = GetDates(init_date, days)
        lastmessages = QAForums.static_num_sent(period, chardates[1], chardates[0], None, None, type_post)
        prevmessages = QAForums.static_num_sent(period, chardates[2], chardates[1], None, None, type_post)
        lastmessages = int(lastmessages['sent'])
        prevmessages = int(prevmessages['sent'])

        metric_str = QAForums.__get_metric_name(type_post, "sent")

        name_diff_metric = 'diff_net' + metric_str + '_'+str(days)
        name_perc_metric = 'percentage_' + metric_str + '_'+str(days)
        name_days_metric = metric_str + '_'+str(days)
        data = {}         
        data[name_diff_metric] = lastmessages - prevmessages
        data[name_perc_metric] = GetPercentageDiff(prevmessages, lastmessages)
        data[name_days_metric] = lastmessages
        return data

    @staticmethod
    def get_diff_senders_days(period, init_date, identities_db=None, days=None, type_post="questions"):
        # This function provides the percentage in activity between two periods:
        # Fixme: equal to GetDiffAuthorsDays

        chardates = GetDates(init_date, days)
        lastsenders = QAForums.static_num_senders(period, chardates[1], chardates[0], identities_db,
                                                  type_post)
        prevsenders = QAForums.static_num_senders(period, chardates[2], chardates[1], identities_db,
                                                  type_post)
        lastsenders = int(lastsenders['senders'])
        prevsenders = int(prevsenders['senders'])

        metric_str = QAForums.__get_metric_name(type_post, "senders")

        name_diff_metric = 'diff_net' + metric_str + '_'+str(days)
        name_perc_metric = 'percentage_' + metric_str + '_'+str(days)
        name_days_metric = metric_str + '_'+str(days)
        data = {}        
        data[name_diff_metric] = lastsenders - prevsenders
        data[name_perc_metric] = GetPercentageDiff(prevsenders, lastsenders)
        data[name_days_metric] = lastsenders
        return data

    @staticmethod
    def get_static_data(period, startdate, enddate, i_db, type_analysis):
        # 1- Retrieving information
        qsent = QAForums.static_num_sent(period, startdate, enddate, i_db, type_analysis, "questions")
        asent = QAForums.static_num_sent(period, startdate, enddate, i_db, type_analysis, "answers")
        csent = QAForums.static_num_sent(period, startdate, enddate, i_db, type_analysis, "comments")
        qsenders = QAForums.static_num_senders(period, startdate, enddate, i_db, type_analysis, "questions")
        asenders = QAForums.static_num_senders(period, startdate, enddate, i_db, type_analysis, "answers")
        csenders = QAForums.static_num_senders(period, startdate, enddate, i_db, type_analysis, "comments")
        # rename the keys of the dict in order to print them in the JSON file
        qsent["qsent"] = qsent.pop("sent")
        asent["asent"] = asent.pop("sent")
        csent["csent"] = csent.pop("sent")
        qsenders["qsenders"] = qsenders.pop("senders")
        asenders["asenders"] = asenders.pop("senders")
        csenders["csenders"] = csenders.pop("senders")

        # 2- Merging information
        static_data = dict(csent.items() + qsent.items() + asent.items() +
                           csenders.items() + qsenders.items() + asenders.items())

        return (static_data)

    @staticmethod
    def get_agg_data(period, startdate, enddate, identities_db, filter_=None):
        agg_data = {}

        type_messages = ['questions','comments','answers']

        # Tendencies
        for i in [7, 30, 365]:
            for tm in type_messages:
                period_data = QAForums.get_diff_sent_days(period, enddate, i, tm)
                agg_data = dict(agg_data.items() + period_data.items())
                period_data = QAForums.get_diff_senders_days(period, enddate, identities_db, i, tm)
                agg_data = dict(agg_data.items() + period_data.items())
                # end for
        static_data = QAForums.get_static_data(period, startdate, enddate, identities_db, None)
        agg_data = dict(agg_data.items() + static_data.items())

        return agg_data

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
    def GetSQLReportFrom(identities_db, type_analysis):
        # generic function to generate "from" clauses
        # type_analysis contains two values: type of analysis (company, country...)
        # and the value itself
       
        #TODO: to be implemented.
        print "WARNING: QAForums.GetSQLReportFrom to be implemented"
        tables = ""
        return tables


    @staticmethod
    def GetSQLReportWhere(type_analysis, role):
        # generic function to generate "from" clauses
        # type_analysis contains two values: type of analysis (company, country...)
        # and the value itself

        #TODO: to be implemented
        print "WARNING: QAForums.GetSQLReportWhere to be implemented"
        where = ""
        return where

