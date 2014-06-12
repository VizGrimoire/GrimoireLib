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

from query_builder import QAForumsQuery

from QAForums import QAForums


class Questions(Metrics):
    """Questions class"""

    id = "qsent"
    name = "Questions"
    desc = "Questions found in the QA platform"
    data_source = QAForums
  
    def __get_sql__(self, evolutionary):
        return self.db.get_sent(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "questions")


class Answers(Metrics):
    """Answers class"""

    id = "asent"
    name = "Answers"
    desc = "Answers found in the QA platform"
    data_source = QAForums

    def __get_sql__(self, evolutionary):
        return self.db.get_sent(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "answers")


class Comments(Metrics):
    """Comments class"""

    id = "csent"
    name = "Comments"
    desc = "Comments found in the QA platform"
    data_source = QAForums

    def __get_sql__(self, evolutionary):
        return self.db.get_sent(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "comments")


class QuestionSenders(Metrics):
    """QuestionsSenders class"""

    id = "qsenders"
    name = "Question Senders"
    desc = "People asking questions in the QA platform"
    data_source = QAForums

    def __get_sql__(self, evolutionary):
        return self.db.get_senders(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "questions")
       

class AnswerSenders(Metrics):
    """AnswerSenders class"""

    id = "asenders"
    name = "Answer Senders"
    desc = "People sending answers in the QA platform"
    data_source = QAForums

    def __get_sql__(self, evolutionary):
        return self.db.get_senders(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "answers")


class CommentSenders(Metrics):
    """CommentSenders class"""

    id = "csenders"
    name = "Comment Senders"
    desc = "People commenting in questions or answers in the QA platform"
    data_source = QAForums

    def __get_sql__(self, evolutionary):
        return self.db.get_senders(self.filters.period, self.filters.startdate, self.filters.enddate,
                                 self.filters.type_analysis, evolutionary, "comments")


class Participants(Metrics):
    """All participants included in this metric, those commenting, asking and answering"""

    id = "participants"
    name = "Participants"
    desc = "All participants included in this metric, those commenting, asking and answering"
    data_source = QAForums

    def __get_sql__(self, evolutionary):
        fields = " count(distinct(t.identifier)) as participants"
        tables = """
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
                 """
        filters = "" 
        query = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                                   self.filters.enddate, " t.date ", fields,
                                   tables, filters, evolutionary)
        return query

