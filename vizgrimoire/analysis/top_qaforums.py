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

from analyses import Analyses

from query_builder import DSQuery

from metrics_filter import MetricFilters

class TopQAForums(Analyses):
    # this class provides a list of top contributors
    # A top contributor is defined in this function as the aggregation
    # of questions, comments and answers.

    id = "topqaforums_messagessent"
    name = "Top Messages Sent QAForums"
    desc = "Top people sending messages in the Q&A system. A message is defined as a question, a comment or an answer."

    def __get_sql__(self):
   
        query = """
             select p.username as name, count(*) as messages_sent from 
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
              people p
             where p.identifier = t.identifier and
                   t.date >= %s and
                   t.date < %s
             group by p.username order by count(*) desc limit %s
             """ % (self.filters.startdate, self.filters.enddate, self.filters.npeople)

        return query

    def result(self):
        return self.db.ExecuteQuery(self.__get_sql__())


if __name__ == '__main__':
    filters = MetricFilters("week", "'2013-01-01'", "'2014-01-01'", ["repository", "'nova.git'"])
    dbcon = DSQuery("root", "", "sibyl_openstack_20120412_updated", "sibyl_openstack_20120412_updated")
    top_messages = TopQAForums(dbcon, filters)
    print top_messages.result()

