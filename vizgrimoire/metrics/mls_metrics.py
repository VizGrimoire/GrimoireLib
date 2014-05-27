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
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##


import logging
import MySQLdb

import re, sys

from GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff

from metrics import Metrics

from metrics_filter import MetricFilters

from query_builder import MLSQuery

from MLS import MLS

class EmailsSent(Metrics):
    """ Emails metric class for mailing lists analysis """

    id = "sent"
    name = "Emails Sent"
    desc = "Emails sent to mailing lists"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        if (evolutionary):
            fields = " count(distinct(m.message_ID)) as sent "
        else:
            fields = " count(distinct(m.message_ID)) as sent, "+\
                      " DATE_FORMAT (min(m.first_date), '%Y-%m-%d') as first_date, "+\
                     " DATE_FORMAT (max(m.first_date), '%Y-%m-%d') as last_date "

        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class EmailsSenders(Metrics):
    """ Emails Senders class for mailing list analysis """

    id = "senders"
    name = "Email Senders"
    desc = "People sending emails"
    data_source = MLS

    def __get_sql__ (self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables = tables + ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            #not sure if this line is useful anymore...
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       "mp.email_address = pup.people_id and "+\
                       "mp.type_of_recipient=\'From\' "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class SendersResponse(Metrics):
    """ People answering in a thread """
    # Threads class is not needed here. This is thanks to the use of the
    # field is_reponse_of.

    id = "senders_response"
    name = "Senders Response"
    desc = "People answering in a thread"

    def __get_sql__ (self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders_response "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables += ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       "mp.email_address = pup.people_id and "+\
                       "mp.type_of_recipient=\'From\' "
        filters += " and m.is_response_of is not null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class SendersInit(Metrics):
    """ People initiating threads """

    id = "senders_init"
    name = "SendersInit"
    desc = "People initiating threads"
    data_source = MLS

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(pup.upeople_id)) as senders_init "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        if (tables == " messages m "):
            # basic case: it's needed to add unique ids filters
            tables += ", messages_people mp, people_upeople pup "
            filters = self.db.GetFiltersOwnUniqueIds()
        else:
            filters = self.db.GetSQLReportWhere(self.filters.type_analysis)

        if (self.filters.type_analysis and self.filters.type_analysis[0] in ("repository", "project")):
            #Adding people_upeople table
            tables += ",  messages_people mp, people_upeople pup "
            filters += " and m.message_ID = mp.message_id and "+\
                       " mp.email_address = pup.people_id and "+\
                       " mp.type_of_recipient=\'From\' "
        filters += " and m.is_response_of is null "

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query


class Repositories(Metrics):
    """ Mailing lists repositories """

    id = "repositories"
    name = "Mailing Lists"
    desc = "Mailing lists with activity"
    data_source = MLS

    def __get_sql__(self, evolutionary):
   
        #fields = " COUNT(DISTINCT(m."+rfield+")) AS repositories  "
        fields = " COUNT(DISTINCT(m.mailing_list_url)) AS repositories "
        tables = " messages m " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)
     
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " m.first_date ", fields,
                                   tables, filters, evolutionary)
        return query



