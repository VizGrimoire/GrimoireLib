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
#
# Authors:
#     Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#

from query_builder import DSQuery

from metrics_filter import MetricFilters

class TopQuestions(object):
    # this class provides a list of top questions filtered by 
    # different requirements:
    # * Number of people participating 
    # * Number of comments
    # * Number of visits
    # * Others
    #
    # Based on the QAForums data source
    # This class is a general class and not specific studies. 


    def __init__(self, dbcon, filters):
        self.db = dbcon
        self.filters = filters
        
    def top_commented(self):
        # The top commented questions are those with the highets
        # number of comments in the question and ignoring the comments
        # in the answer of such question.
        # This query is based on the submission date of the question, and
        # not on the submission date of the comment. It may make sense both 
        # approaches

        query = """
                select q.url as url, 
                       q.question_identifier as question_identifier,
                       count(distinct(c.id)) as comments
                from questions q, 
                     comments c
                where q.question_identifier = c.question_identifier and
                      q.added_at >= %s and
                      q.added_at < %s
                group by q.question_identifier
                order by comments desc limit %s
                """ % (self.filters.startdate, self.filters.enddate, self.filters.npeople)
        return self.db.ExecuteQuery(query)

    def top_visited(self):
        # The top visited questions are those questions with the 
        # highest number of visits. The date of each of the visit
        # is not registered (due to the Askbot API), so if the 
        # startdate and enddate filters are required, those are
        # applied on the question submission date.

        query = """
                select url, 
                       question_identifier, 
                       view_count as visits 
                from questions 
                where questions.added_at >= %s and
                      q.added_at < %s
                order by view_count desc limit %s
                """ % (self.filters.startdate, self.filters.enddate, self.filters.npeople)
        return self.db.ExecuteQuery(query)
        

    def top_crowded(self):
        # The top crowded questions are those with the highest number
        # of different people participating in such questions, aggregating
        # the number of people replying the question, comments in questions,
        # comments in answers and the init of the question.

        query = """
                select q.url as url,
                       q.question_identifier as question_identifier,
                       count(distinct(user)) as people
                from(
                     (select question_identifier, 
                             author_identifier as user
                      from questions)
                     union
                     (select question_identifier,
                             user_identifier as user
                      from answers)
                     union
                     (select question_identifier,
                             user_identifier as user
                      from comments
                      where question_identifier is not null)
                     union
                     (select distinct a.question_identifier as question_identifier,
                             c.user_identifier as user
                      from answers a,
                           comments c
                      where c.answer_identifier = a.identifier) ) t,
                     questions q
                where q.question_identifier = t.question_identifier and
                      q.added_at >= %s and
                      q.added_at < %s
                group by q.question_identifier
                order by people desc limit %s
                """ % (self.filters.startdate, self.filters.enddate, self.filters.npeople)



