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
#
# This function provides information about the general structure of the community.
# This is divided into core, regular and ocassional authors
#  - Core developers are defined as those doing up to a 80% of the total commits
#  - Regular developers are defind as those doing from the 80% to a 99% of the total commits
#  - Occasional developers are defined as those doing from the 99% to the 100% of the commits
# Analysis based on the study by Crowston and Howison, 
#          'The social structure of free and open source software'
# http://firstmonday.org/ojs/index.php/fm/rt/printerFriendly/1207/1127


from analyses import Analyses

from query_builder import DSQuery

from metrics_filter import MetricFilters

class CommunityStructure(Analyses):
    # TODO:
    #  - Add date filters (start and end date)
    #  - Add repository filters (s.repository_id= xxx)
    #  - Add type of file filter (so far 'code' is harcoded in the query)
    #  - Add evolutionary analysis of territoriality

    id = "onion"
    name = "Onion Model"
    desc = "Community structure of developers: core, regular and occasional"

    def get_agg (self, data_source):
        """ Returns aggregated data for a data source. """
        data = {}
        if data_source.get_name() == "scm":
            data = self.result()
        return data

    def result(self, data_source):
        if data_source.get_name() != "scm": return

        # Init of structure to be returned
        community = {}
        community['core'] = None
        community['regular'] = None
        community['occasional'] = None

        q = "select count(distinct(s.id)) as total "+\
             "from scmlog s, people p, actions a "+\
             "where s.author_id = p.id and "+\
             "      p.email <> '%gerrit@%' and "+\
             "      p.email <> '%jenkins@%' and "+\
             "      s.id = a.commit_id and "+\
             "      s.date>="+self.filters.startdate+" and "+\
             "      s.date<="+self.filters.enddate+";"

        total = self.db.ExecuteQuery(q)
        total_commits = float(total['total'])

        # Database access: developer, %commits
        q = " select pup.upeople_id, "+\
            "        (count(distinct(s.id))) as commits "+\
            " from scmlog s, "+\
            "      actions a, "+\
            "      people_upeople pup, "+\
            "      people p "+\
            " where s.id = a.commit_id and "+\
            "       s.date>="+self.filters.startdate+" and "+\
            "       s.date<="+self.filters.enddate+" and "+\
            "       s.author_id = pup.people_id and "+\
            "       s.author_id = p.id and "+\
            "       p.email <> '%gerrit@%' and "+\
            "       p.email <> '%jenkins@%' "+\
            " group by pup.upeople_id "+\
            " order by commits desc; "

        people = self.db.ExecuteQuery(q)
        if not isinstance(people['commits'], list):
            people['commits'] = [people['commits']]
        # this is a list. Operate over the list
        people['commits'] = [((commits / total_commits) * 100) for commits in people['commits']]
        # people['commits'] = (people['commits'] / total_commits) * 100


        # Calculating number of core, regular and occasional developers
        cont = 0
        core = 0
        core_f = True # flag
        regular = 0
        regular_f = True  # flag
        occasional = 0
        devs = 0

        for value in people['commits']:
            cont = cont + value
            devs = devs + 1

            if (core_f and cont >= 80):
                #core developers number reached
                core = devs
                core_f = False

            if (regular_f and cont >= 95):
                regular = devs
                regular_f = False

        occasional = devs - regular
        regular = regular - core

        # inserting values in variable
        community['core'] = core
        community['regular'] = regular
        community['occasional'] = occasional

        return(community)


