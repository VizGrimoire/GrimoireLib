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

from vizgrimoire.metrics.metrics import Metrics
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.metrics.query_builder import IRCQuery
from vizgrimoire.IRC import IRC

from sets import Set

class Sent(Metrics):
    """Messages sent metric class for IRC channels"""
    id = "sent"
    name = "Sent messages"
    desc = "Number of messages sent to IRC channels"
    data_source = IRC

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("COUNT(i.message) AS sent")
        tables.add("irclog i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.add("i.type = 'COMMENT'")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " i.date ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        return query


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
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        if filter_bots != "": filter_bots += " AND "
        date_limit = ""

        if (days != 0 ):
            sql = "SELECT @maxdate:=max(date) from irclog limit 1"
            res = self.db.ExecuteQuery(sql)
            date_limit = " AND DATEDIFF(@maxdate, date)<"+str(days)
        q = "SELECT up.uuid as id, up.identifier as senders,"+\
            "       COUNT(irclog.id) as sent "+\
            " FROM irclog, people_uidentities pup, "+self.db.identities_db+".uidentities up "+\
            " WHERE "+ filter_bots +\
            "            irclog.type = 'COMMENT' and "+\
            "            irclog.nick = pup.people_id and "+\
            "            pup.uuid = up.uuid and "+\
            "            date >= "+ startdate+ " and "+\
            "            date  < "+ enddate+ " "+ date_limit +\
            "            GROUP BY senders "+\
            "            ORDER BY sent desc, senders "+\
            "            LIMIT " + str(limit)
        return(self.db.ExecuteQuery(q))

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("COUNT(DISTINCT(i.nick)) AS senders")
        tables.add("irclog i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.add("type = 'COMMENT'")
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " i.date ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        return query


class Repositories(Metrics):
    """Repositories metric class for IRC channels"""
    id = "repositories"
    name = "Repositories"
    desc = "Number of active repositories"
    data_source = IRC

    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("COUNT(DISTINCT(i.channel_id)) AS repositories")
        tables.add("irclog i")
        tables.union_update(self.db.GetSQLReportFrom(self.filters))
        filters.union_update(self.db.GetSQLReportWhere(self.filters))

        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " i.date ", fields,
                               tables, filters, evolutionary, self.filters.type_analysis)
        return query

    def get_list (self):
        tables_set = Set([])
        tables_set.add("irclog i")
        tables_set.add("channels chan")
        filters_set = Set([])
        filters_set.add("i.channel_id = chan.id")

        tables_set.union_update(self.db.GetSQLReportFrom(self.filters))
        filters_set.union_update(self.db.GetSQLReportWhere(self.filters))

        tables = self.db._get_fields_query(tables_set)
        filters = self.db._get_filters_query(filters_set)

        q = "SELECT name, count(i.id) AS total "+\
            "  FROM " + tables +\
            "  WHERE " + filters +\
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
