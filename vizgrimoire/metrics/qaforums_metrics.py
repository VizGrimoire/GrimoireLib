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

import datetime
import time

from sets import Set

from vizgrimoire.GrimoireUtils import completePeriodIds, createTimeSeries
from vizgrimoire.metrics.metrics import Metrics
from vizgrimoire.QAForums import QAForums


class Questions(Metrics):
    """Questions class"""

    id = "qsent"
    name = "Questions"
    desc = "Questions found in the QA platform"
    data_source = QAForums
  
    def _get_sql(self, evolutionary):
        return self.db.get_sent(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "questions")

class Answers(Metrics):
    """Answers class"""

    id = "asent"
    name = "Answers"
    desc = "Answers found in the QA platform"
    data_source = QAForums

    def _get_sql(self, evolutionary):
        return self.db.get_sent(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "answers")
class Comments(Metrics):
    """Comments class"""

    id = "csent"
    name = "Comments"
    desc = "Comments found in the QA platform"
    data_source = QAForums

    def _get_sql(self, evolutionary):
        return self.db.get_sent(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "comments")
class QuestionSenders(Metrics):
    """QuestionsSenders class"""

    id = "qsenders"
    name = "Question Senders"
    desc = "People asking questions in the QA platform"
    data_source = QAForums

    def _get_sql(self, evolutionary):
        return self.db.get_senders(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "questions")

    def _get_top_global(self, days = 0, metric_filters = 0):

        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople

        return self.db.get_top_senders(days, startdate, enddate, limit, "questions")

class UnansweredQuestions(Metrics):
    """UnansweredQuestions class"""

    id = "unanswered"
    name = "Unanswered Questions"
    desc = "Questions without any answer"
    data_source = QAForums

    def _get_sql_unaswered(self, enddate):
        sql = """SELECT COUNT(DISTINCT(q.question_identifier)) unanswered
                 FROM questions q
                 WHERE q.added_at < '%s'
                 AND q.question_identifier NOT IN
                     (SELECT a.question_identifier
                      FROM answers a
                      WHERE a.submitted_on < '%s');
              """ % (enddate, enddate)
        return sql

    def __gen_dates(self, period, startdate, enddate):
        dates = createTimeSeries({})
        dates.pop('id')
        dates[period] = []

        dates = completePeriodIds(dates, period, startdate, enddate)

        # Remove zeros
        dates['date'] = [d for d in dates['date'] if d != 0]
        dates['unixtime'] = [d for d in dates['unixtime'] if d != 0]

        return dates

    def get_ts(self):
        data = self.__gen_dates(self.filters.period,
                                self.filters.startdate,
                                self.filters.enddate)

        # Generate periods
        last_date = int(time.mktime(datetime.datetime.strptime(
                        self.filters.enddate, "'%Y-%m-%d'").timetuple()))

        periods = list(data['unixtime'][1:])
        periods.append(last_date)

        data['unanswered'] = []

        for p in periods:
            enddate = datetime.datetime.fromtimestamp(int(p)).strftime('%Y-%m-%d %H:%M:%S')
            query = self._get_sql_unaswered(enddate)
            res = self.db.ExecuteQuery(query)

            data['unanswered'].append(res['unanswered'])

        return data

class AnswerSenders(Metrics):
    """AnswerSenders class"""

    id = "asenders"
    name = "Answer Senders"
    desc = "People sending answers in the QA platform"
    data_source = QAForums

    def _get_sql(self, evolutionary):
        return self.db.get_senders(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "answers")

    def _get_top_global(self, days = 0, metric_filters = 0):

        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople

        return self.db.get_top_senders(days, startdate, enddate, limit, "answers")

class CommentSenders(Metrics):
    """CommentSenders class"""

    id = "csenders"
    name = "Comment Senders"
    desc = "People commenting in questions or answers in the QA platform"
    data_source = QAForums

    def _get_sql(self, evolutionary):
        return self.db.get_senders(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "comments")

    def _get_top_global(self, days = 0, metric_filters = 0):
        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople

        return self.db.get_top_senders(days, startdate, enddate, limit, "comments")

class Participants(Metrics):
    """All participants included in this metric, those commenting, asking and answering"""

    id = "participants"
    name = "Participants"
    desc = "All participants included in this metric, those commenting, asking and answering"
    data_source = QAForums

    #TODO: missing getsqlreportfrom/where methods
    def _get_sql(self, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])
        fields.add("count(distinct(t.identifier)) as participants")
        tables.add("""
                  (
                     (select p.identifier as identifier, 
                             q.added_at as date 
                      from questions q, 
                           people p 
                      where q.author_identifier=p.identifier) 
                     union 
                     (select p.identifier as identifier, 
                             a.submitted_on as date  
                      from answers a, 
                           people p 
                      where a.user_identifier=p.identifier) 
                     union 
                     (select p.identifier as identifier, 
                             c.submitted_on as date 
                      from comments c, 
                           people p 
                      where c.user_identifier=p.identifier)) t
                 """)
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " t.date ", fields,
                                   tables, filters, evolutionary, self.filters.type_analysis)
        return query

    def _get_top_global(self, days = 0, metric_filters = 0):

        if metric_filters == None:
            metric_filters = self.filters

        startdate = metric_filters.startdate
        enddate = metric_filters.enddate
        limit = metric_filters.npeople
        filter_bots = self.db.get_bots_filter_sql(self.data_source, metric_filters)
        if filter_bots != "": filter_bots += " AND "
        date_limit = ""

        # TODO: Last date to be reviewed using answers and comments
        if (days != 0 ):
            sql = "SELECT @maxdate:= max(last_activity_at) from questions limit 1"
            res = self.db.ExecuteQuery(sql)
            date_limit = " AND DATEDIFF(@maxdate, t.date)<"+str(days)

        # TODO: Missing use of GetSQLReportFrom/Where
        query = """
             select pup.uuid as id, 
                    p.username as name, 
                    count(*) as messages_sent from 
              (
                 (select p.identifier as identifier, 
                         q.added_at as date 
                  from questions q, 
                       people p 
                  where q.author_identifier=p.identifier) 
                 union 
                 (select p.identifier as identifier, 
                         a.submitted_on as date  
                  from answers a, 
                       people p 
                  where a.user_identifier=p.identifier) 
                 union 
                 (select p.identifier as identifier, 
                         c.submitted_on as date 
                  from comments c, 
                       people p 
                  where c.user_identifier=p.identifier)) t,
              people p,
              people_uidentities pup
             where p.identifier = t.identifier and
                   p.id = pup.people_id and 
                   t.date >= %s and
                   t.date < %s 
                   %s
             group by p.username order by count(*) desc, p.username limit %s
             """ % (startdate, enddate, date_limit, limit)

        return self.db.ExecuteQuery(query)


class Tags(Metrics):
    """Tags used in QAForum """

    id = "tags"
    name = "Tags"
    desc = "Tags used in QAForums"
    data_source = QAForums

    def _get_sql(self, evolutionary):
        pass

    #TODO: Missing use of GetSQLReportFrom methods
    def get_list(self):
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
                   having total > %s
                   order by total desc, name;""" % (self.filters.startdate, self.filters.enddate, Metrics.min_item_per_tag)
        data = self.db.ExecuteQuery(query)
        return data
