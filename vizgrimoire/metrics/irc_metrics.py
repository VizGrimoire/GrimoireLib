# -*- coding: utf-8 -*-
#
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
# This file is a part of GrimoireLib
#  (an Python library for the MetricsGrimoire and vizGrimoire systems)
#
#
# Authors:
#   Alvaro del Castillo <acs@bitergia.com>
#   Daniel Izquierdo <dizquierdo@bitergia.com>
#   Santiago Dueñas <sduenas@bitergia.com>
#

""" Opened metric for the IRC systems """


from metrics import Metrics
from metrics_filter import MetricFilters
from query_builder import IRCQuery
from IRC import IRC


class Sent(Metrics):
    """Messages sent metric class for IRC channels"""
    id = "sent"
    name = "Sent messages"
    desc = "Number of messages sent to IRC channels"
    data_source = IRC

    def __get_sql__(self, evolutionary):
        fields = " COUNT(message) AS sent "
        tables = " irclog i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)
        filters += " and type='COMMENT' "
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " date ", fields,
                               tables, filters, evolutionary)
        return q


class Senders(Metrics):
    """Messages senders class for IRC channels"""
    id = "senders"
    name = "Message senders"
    desc = "Number of message senders to IRC channels"
    data_source = IRC


    def _get_top_global (self, days = 0, metric_filters = None):
        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.get_bots_filter_sql(metric_filters)
        if filter_bots != "": filter_bots += " AND "
        date_limit = ""

        if (days != 0 ):
            sql = "SELECT @maxdate:=max(date) from irclog limit 1"
            res = self.db.ExecuteQuery(sql)
            date_limit = " AND DATEDIFF(@maxdate, date)<"+str(days)
        q = "SELECT u.id as id, u.identifier as senders,"+\
            "       COUNT(irclog.id) as sent "+\
            " FROM irclog, people_upeople pup, "+self.db.identities_db+".upeople u "+\
            " WHERE "+ filter_bots +\
            "            irclog.type = 'COMMENT' and "+\
            "            irclog.nick = pup.people_id and "+\
            "            pup.upeople_id = u.id and "+\
            "            date >= "+ startdate+ " and "+\
            "            date  < "+ enddate+ " "+ date_limit +\
            "            GROUP BY senders "+\
            "            ORDER BY sent desc, senders "+\
            "            LIMIT " + str(limit)
        return(self.db.ExecuteQuery(q))

    def __get_sql__(self, evolutionary):
        fields = " COUNT(DISTINCT(nick)) AS senders "
        tables = " irclog i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)
        filters += " and type='COMMENT' "
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " date ", fields,
                               tables, filters, evolutionary)
        return q


class Repositories(Metrics):
    """Repositories metric class for IRC channels"""
    id = "repositories"
    name = "Repositories"
    desc = "Number of active repositories"
    data_source = IRC

    def __get_sql__(self, evolutionary):
        fields = " COUNT(DISTINCT(channel_id)) AS repositories "
        tables = " irclog i " + self.db.GetSQLReportFrom(self.filters.type_analysis)
        filters = self.db.GetSQLReportWhere(self.filters.type_analysis)
        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " date ", fields,
                               tables, filters, evolutionary)
        return q

    def get_list (self):
        q = "SELECT name, count(i.id) AS total "+\
            "  FROM irclog i, channels c "+\
            "  WHERE i.channel_id=c.id "+\
            "  GROUP BY name ORDER BY total DESC"
        return(self.db.ExecuteQuery(q)['name'])

# Examples of use
if __name__ == '__main__':
    filters = MetricFilters("week", "'2010-01-01'", "'2014-01-01'", ["company", "'Red Hat'"])
    dbcon = IRCQuery("root", "", "cp_irc_SingleProject", "cp_irc_SingleProject",)
    redhat = Sent(dbcon, filters)
    all = Sent(dbcon)
    # print redhat.get_ts()
    print redhat.get_agg()
    print all.get_agg()
