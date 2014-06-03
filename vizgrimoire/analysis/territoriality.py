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
# Territoriality analysis of a module
# This analysis makes sense only when focusing on the activity
# undertaken by developers in the source code.
# Territoriality is measured as the percentage of files only
# touched by one developer along the life of each of the files.
# 

from analyses import Analyses

from query_builder import DSQuery

from metrics_filter import MetricFilters

class Territoriality(Analyses):
    # TODO:
    #  - Add date filters (start and end date)
    #  - Add repository filters (s.repository_id= xxx)
    #  - Add type of file filter (so far 'code' is harcoded in the query)
    #  - Add evolutionary analysis of territoriality

    id = "territoriality"
    name = "Territoriality"
    desc = "Percentage of files 'touched' by just one developer"

    def __get_sql__(self):
        query = """
                select (
                    select count(*) from(
                    select sum(a.file_id) as territorial_files       
                    from actions a, 
                         scmlog s, 
                         people_upeople pup, 
                         file_types ft 
                    where a.commit_id=s.id and 
                          s.author_id=pup.people_id  and 
                          a.file_id=ft.file_id and 
                          ft.type='code'  and 
                          a.file_id not in
                              (select file_id
                               from actions
                               where type='D')
                    group by a.file_id
                    having count(distinct(pup.upeople_id)) = 1 ) as t )
                /
                    (
                    select count(distinct(a.file_id)) as total_files 
                    from actions a, 
                         scmlog s, 
                         file_types ft 
                    where a.commit_id=s.id  and 
                          a.file_id=ft.file_id and 
                          ft.type='code' and 
                          a.file_id not in  
                              (select file_id
                               from actions
                               where type='D')
                    ) as territoriality
             """
        return query

    def result(self):
        return self.db.ExecuteQuery(self.__get_sql__())


