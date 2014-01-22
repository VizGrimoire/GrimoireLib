#!/usr/bin/env python

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

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod, GetSQLReportFrom
from GrimoireSQL import GetSQLReportWhere, ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates
import GrimoireUtils

# SQL Metaqueries
def GetTablesOwnUniqueIdsIRC():
    tables = 'irclog, people_upeople pup'
    return (tables)


def GetFiltersOwnUniqueIdsIRC():
    filters = 'pup.people_id = irclog.nick'
    return (filters)

def StaticNumSentIRC (period, startdate, enddate, identities_db=None, type_analysis=[]):
    fields = "SELECT count(message) as sent, \
              DATE_FORMAT (min(date), '%Y-%m-%d') as first_date, \
              DATE_FORMAT (max(date), '%Y-%m-%d') as last_date "
    tables = " FROM irclog "
    filters = "WHERE date >=" + startdate + " and date < " + enddate
    q = fields + tables + filters
    return(ExecuteQuery(q))

def StaticNumSendersIRC (period, startdate, enddate, identities_db=None, type_analysis=[]):
    fields = "SELECT count(distinct(nick)) as senders"
    tables = " FROM irclog "
    filters = "WHERE date >=" + startdate + " and date < " + enddate
    q = fields + tables + filters
    return(ExecuteQuery(q))

def StaticNumRepositoriesIRC (period, startdate, enddate, identities_db=None, type_analysis=[]):
    fields = "SELECT COUNT(DISTINCT(channel_id)) AS repositories "
    tables = "FROM irclog "
    filters = "WHERE date >=" + startdate + " AND date < " + enddate
    q = fields + tables + filters
    return(ExecuteQuery(q))


def GetStaticDataIRC(period, startdate, enddate, idb = None, type_analysis=[]):
    agg_data = "GetStaticDataIRC"

    sent = StaticNumSentIRC(period, startdate, enddate, idb, type_analysis)
    senders = StaticNumSendersIRC(period, startdate, enddate, idb, type_analysis)
    repositories = StaticNumRepositoriesIRC(period, startdate, enddate, idb, type_analysis)
    agg_data = dict(sent.items() + senders.items() + repositories.items())

    return (agg_data)

def GetSentIRC (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    fields = " count(distinct(message)) as sent "
    tables = " irclog " + GetSQLReportFrom(identities_db, type_analysis)
    filters = GetSQLReportWhere(type_analysis, "author")
    q = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolSentIRC (period, startdate, enddate, identities_db=None, type_analysis = []):
    return(GetSentIRC(period, startdate, enddate, identities_db, type_analysis, True))

def GetSendersIRC (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    fields = " count(distinct(nick)) as senders "
    tables = " irclog " + GetSQLReportFrom(identities_db, type_analysis)
    filters = GetSQLReportWhere(type_analysis, "author")
    q = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolSendersIRC (period, startdate, enddate, identities_db=None, type_analysis = []):
    return(GetSendersIRC(period, startdate, enddate, identities_db, type_analysis, True))

def GetRepositoriesIRC (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    fields = " COUNT(DISTINCT(channel_id)) AS repositories "
    tables = " irclog " + GetSQLReportFrom(identities_db, type_analysis)
    filters = GetSQLReportWhere(type_analysis, "author")
    q = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))

def EvolRepositoriesIRC (period, startdate, enddate, identities_db=None, type_analysis = []):
    return(GetRepositoriesIRC(period, startdate, enddate, identities_db, type_analysis, True))

def GetEvolDataIRC(period, startdate, enddate, idb=None, type_analysis=[]):

    # 1- Retrieving information
    sent = EvolSentIRC(period, startdate, enddate, idb, type_analysis)
    senders = EvolSendersIRC(period, startdate, enddate, idb, type_analysis)
    repositories = EvolRepositoriesIRC(period, startdate, enddate, idb, type_analysis)

    # 2- Merging information
    evol_data = dict(sent.items() + senders.items() + repositories.items())
    return (evol_data)

def GetTablesReposIRC():
    return(GetTablesOwnUniqueIdsIRC()+",channels c")

def GetFiltersReposIRC():
    return(GetFiltersOwnUniqueIdsIRC()+" AND c.id = irclog.channel_id ")

# TODO: this function does not use the official procedure
def GetRepoEvolSentSendersIRC (repo, period, startdate, enddate):
    fields = 'COUNT(irclog.id) AS sent, COUNT(DISTINCT(pup.upeople_id)) AS senders'
    tables= GetTablesReposIRC()
    filters = GetFiltersReposIRC() + " AND c.name='"+repo+"'"
    q = GetSQLPeriod(period,'date', fields, tables, filters, startdate, enddate)
    return(ExecuteQuery(q))

def GetRepoStaticSentSendersIRC (repo, startdate, enddate):
    fields = 'COUNT(irclog.id) AS sent,'+\
            'COUNT(DISTINCT(pup.upeople_id)) AS senders'
    tables = GetTablesReposIRC()
    filters = GetFiltersReposIRC()+" AND c.name='"+repo+"'"
    q = GetSQLGlobal('date',fields, tables, filters, startdate, enddate)
    return(ExecuteQuery(q))

##############
# Microstudies
##############

def GetIRCDiffSentDays (period, init_date, days):
    # This function provides the percentage in activity between two periods.
    #
    # The netvalue indicates if this is an increment (positive value) or decrement (negative value)

    chardates = GetDates(init_date, days)
    lastmessages = StaticNumSentIRC(period, chardates[1], chardates[0])
    lastmessages = int(lastmessages['sent'])
    prevmessages = StaticNumSentIRC(period, chardates[2], chardates[1])
    prevmessages = int(prevmessages['sent'])

    data = {}
    data['diff_netsent_'+str(days)] = lastmessages - prevmessages
    data['percentage_sent_'+str(days)] = GetPercentageDiff(prevmessages, lastmessages)
    data['sent_'+str(days)] = lastmessages

    return data

def GetIRCDiffSendersDays (period, init_date, identities_db=None, days = None):
    # This function provides the percentage in activity between two periods:
    # Fixme: equal to GetDiffAuthorsDays

    chardates = GetDates(init_date, days)
    lastsenders = StaticNumSendersIRC(period, chardates[1], chardates[0], identities_db)
    lastsenders = int(lastsenders['senders'])
    prevsenders = StaticNumSendersIRC(period, chardates[2], chardates[1], identities_db)
    prevsenders = int(prevsenders['senders'])

    data = {}
    data['diff_netsenders_'+str(days)] = lastsenders - prevsenders
    data['percentage_senders_'+str(days)] = GetPercentageDiff(prevsenders, lastsenders)
    data['senders_'+str(days)] = lastsenders

    return data

def GetTopSendersIRC (days, startdate, enddate, identities_db, bots):
    date_limit = ""
    filter_bots = ''
    for bot in bots:
        filter_bots += " nick<>'"+bot+"' and "
    if (days != 0 ):
        sql = "SELECT @maxdate:=max(date) from irclog limit 1"
        res = ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate, date)<"+str(days)
    q = "SELECT up.id as id, up.identifier as senders,"+\
        "       COUNT(irclog.id) as sent "+\
        " FROM irclog, people_upeople pup, "+identities_db+".upeople up "+\
        " WHERE "+ filter_bots + \
        "            irclog.nick = pup.people_id and "+\
        "            pup.upeople_id = up.id and "+\
        "            date >= "+ startdate+ " and "+\
        "            date  < "+ enddate+ " "+ date_limit +\
        "            GROUP BY senders "+\
        "            ORDER BY sent desc "+\
        "            LIMIT 10 "
    return(ExecuteQuery(q))

#########
# PEOPLE
#########
def GetListPeopleIRC (startdate, enddate):
    fields = "DISTINCT(pup.upeople_id) as id, count(irclog.id) total"
    tables = GetTablesOwnUniqueIdsIRC()
    filters = GetFiltersOwnUniqueIdsIRC()
    filters += " GROUP BY nick ORDER BY total DESC"
    q = GetSQLGlobal('date', fields, tables, filters, startdate, enddate)
    return(ExecuteQuery(q))

def GetQueryPeopleIRC (developer_id, period, startdate, enddate, evol):
    fields = "COUNT(irclog.id) AS sent"
    tables = GetTablesOwnUniqueIdsIRC()
    filters = GetFiltersOwnUniqueIdsIRC() + " AND pup.upeople_id = " + str(developer_id)

    if (evol) :
        q = GetSQLPeriod(period,'date', fields, tables, filters,
                startdate, enddate)
    else:
        fields = fields + \
                ",DATE_FORMAT (min(date),'%Y-%m-%d') as first_date,"+\
                " DATE_FORMAT (max(date),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('date', fields, tables, filters,
                startdate, enddate)
    return (q)

def GetEvolPeopleIRC (developer_id, period, startdate, enddate):
    q = GetQueryPeopleIRC(developer_id, period, startdate, enddate, True)
    return(ExecuteQuery(q))

def GetStaticPeopleIRC (developer_id, startdate, enddate):
    q = GetQueryPeopleIRC(developer_id, None, startdate, enddate, False)
    return(ExecuteQuery(q))