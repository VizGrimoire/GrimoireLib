#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
#     Luis Cañas-Díaz <lcanas@bitergia.com>
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

class OnionTransitions(Analyses):
    # TODO:
    #  - Add date filters (start and end date)
    #  - Add repository filters (s.repository_id= xxx)
    #  - Add type of file filter (so far 'code' is harcoded in the query)
    #  - Add evolutionary analysis of territoriality

    id = "onion_migrations"
    name = "Migrations in the Onion Model"
    desc = "Migration in the Community structure of developers: core, regular and occasional"

    def get_agg (self, data_source = None):
        """ Returns aggregated data for a data source. """
        data = {}
        if data_source.get_name() == "scm":
            data = self.result(data_source)
        return data

    def _get_person_info(self, upeople_id, from_date, to_date, data_source = None):
        if (data_source.get_name() == "scm"):
            # q = " select pup.upeople_id as uid, p.name, p.email, "+\
            #     "        (count(distinct(s.id))) as commits "+\
            #     " from scmlog s, "+\
            #     "      actions a, "+\
            #     "      people_upeople pup, "+\
            #     "      people p "+\
            #     " where s.id = a.commit_id and "+\
            #     "       s.date>="+ from_date+" and "+\
            #     "       s.date<="+ to_date+" and "+\
            #     "       s.author_id = pup.people_id and "+\
            #     "       s.author_id = p.id and "+\
            #     "       p.email <> '%gerrit@%' and "+\
            #     "       p.email <> '%jenkins@%' and "+\
            #     "       pup.upeople_id = " + str(upeople_id) +\
            #     " group by pup.upeople_id "+\
            #     " order by commits desc; "
            q = " select pup.upeople_id as uid, p.name, p.email, "+\
                "        (count(distinct(s.id))) as commits "+\
                " from scmlog s, "+\
                "      people_upeople pup, "+\
                "      people p "+\
                " where s.date>="+ from_date+" and "+\
                "       s.date<="+ to_date+" and "+\
                "       s.author_id = pup.people_id and "+\
                "       s.author_id = p.id and "+\
                "       p.email <> '%gerrit@%' and "+\
                "       p.email <> '%jenkins@%' and "+\
                "       pup.upeople_id = " + str(upeople_id) +\
                " group by pup.upeople_id "+\
                " order by commits desc; "

            #print(q)
                
            people_data = self.db.ExecuteQuery(q)
            return(people_data)

    def result(self, data_source = None):
        if data_source.get_name() != "scm" \
            and data_source.get_name() != "qaforums": return None

        current_groups = self._activity_groups(self.filters.startdate, self.filters.enddate, data_source)
        cur_core = current_groups["core"]        
        cur_regular = current_groups["regular"]
        cur_occasional = current_groups["occasional"]

        from dateutil import parser
        dt_start = parser.parse(self.filters.startdate)
        dt_end = parser.parse(self.filters.enddate)
        timedelta = dt_end - dt_start

        new_startdate = dt_start - timedelta
        new_startdate = "'" + new_startdate.strftime("%Y-%m-%d") + "'"
        
        past_groups = self._activity_groups(new_startdate, self.filters.startdate, data_source)
        past_core = past_groups["core"]
        past_regular = past_groups["regular"]
        past_occasional = past_groups["occasional"]

        # going up
        up_core = past_regular.intersection(cur_core)
        up_reg = past_occasional.intersection(cur_regular)

        # going down
        down_reg = past_core.intersection(cur_regular)
        down_occ = past_regular.intersection(cur_occasional)

        groups = {"up_core":up_core, "up_reg":up_reg, "down_reg":down_reg, "down_occ":down_occ}        

        result = {}
        for g in groups:
            result[g] = {"name":[],"email":[],"commits":[]}
            for person in groups[g]:
                #print(person)
                user_data = self._get_person_info(person, self.filters.startdate, self.filters.enddate, data_source)
                result[g]["name"].append(user_data["name"])
                result[g]["email"].append(user_data["email"])
                result[g]["commits"].append(user_data["commits"])
                #result[g]["uid"].append(user_data["uid"])
        return (result)
        # for r in result:            
        #     print(r)
        #     for k in result[r]:
        #         print k

        # ## below the print
        
        # ##
        # print("core VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
        # c_set = []
        # a_set = []
        # for c in cur_core:
        #     aux = self.get_person_info(c, self.filters.startdate, self.filters.enddate, data_source)
        #     c_set.append(aux["commits"])
        # c_set.sort()
        # print c_set
        # print("regular ***********************************************")
        # for a in cur_regular:
        #     aux2 = self.get_person_info(a, self.filters.startdate, self.filters.enddate, data_source)
        #     a_set.append(aux2["commits"])
        # a_set.sort()
        # print a_set


        # print("PAST core VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
        # c_set = []
        # a_set = []
        # for c in past_core:
        #     aux = self.get_person_info(c, new_startdate, self.filters.startdate, data_source)
        #     c_set.append(aux["commits"])
        # c_set.sort()
        # print c_set
        # print("PAST regular ***********************************************")
        # for a in past_regular:
        #     aux2 = self.get_person_info(a, new_startdate, self.filters.startdate, data_source)
        #     a_set.append(aux2["commits"])
        # a_set.sort()
        # print a_set




    def _activity_groups(self, from_date, to_date, data_source = None):
        if data_source.get_name() != "scm": return None

        groups = {}

        # Init of structure to be returned
        community = {}
        community['core'] = None
        community['regular'] = None
        community['occasional'] = None

        # q = "select count(distinct(s.id)) as total "+\
        #      "from scmlog s, people p, actions a "+\
        #      "where s.author_id = p.id and "+\
        #      "      p.email <> '%gerrit@%' and "+\
        #      "      p.email <> '%jenkins@%' and "+\
        #      "      s.id = a.commit_id and "+\
        #      "      s.date>="+ from_date +" and "+\
        #      "      s.date<="+ to_date+";"

        q = "select count(distinct(s.id)) as total "+\
             "from scmlog s, people p "+\
             "where s.author_id = p.id and "+\
             "      p.email <> '%gerrit@%' and "+\
             "      p.email <> '%jenkins@%' and "+\
             "      s.date>="+ from_date +" and "+\
             "      s.date<="+ to_date+";"

        total = self.db.ExecuteQuery(q)
        total_commits = float(total['total'])

        # # Database access: developer, %commits
        # q = " select pup.upeople_id as uid, p.name, p.email, "+\
        #     "        (count(distinct(s.id))) as commits "+\
        #     " from scmlog s, "+\
        #     "      actions a, "+\
        #     "      people_upeople pup, "+\
        #     "      people p "+\
        #     " where s.id = a.commit_id and "+\
        #     "       s.date>="+ from_date+" and "+\
        #     "       s.date<="+ to_date+" and "+\
        #     "       s.author_id = pup.people_id and "+\
        #     "       s.author_id = p.id and "+\
        #     "       p.email <> '%gerrit@%' and "+\
        #     "       p.email <> '%jenkins@%' "+\
        #     " group by pup.upeople_id "+\
        #     " order by commits desc; "
        # Database access: developer, %commits
        q = " select pup.upeople_id as uid, p.name, p.email, "+\
            "        (count(distinct(s.id))) as commits "+\
            " from scmlog s, "+\
            "      people_upeople pup, "+\
            "      people p "+\
            " where s.date>="+ from_date+" and "+\
            "       s.date<="+ to_date+" and "+\
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

        #print(people)

        # Calculating number of core, regular and occasional developers
        cont = 0
        ncore = 0
        core_f = True # flag
        nregular = 0
        regular_f = True  # flag
        noccasional = 0
        ndevs = 0
        core_group = set()
        reg_group = set()
        occ_group = set()
        array_cont = 0        

        #for p in people:
        for value in people['commits']:
            cont = cont + value
            ndevs = ndevs + 1
            dev_email = people['uid'][array_cont]

            if (core_f):
                core_group.add(dev_email)
                if (cont >= 80):
                    core_f = False
                    ncore = ndevs
            elif (regular_f):
                reg_group.add(dev_email)
                if (cont >= 95):
                    regular_f = False
                    nregular = ndevs
            else:
                occ_group.add(dev_email)
                

            array_cont += 1

        noccasional = ndevs - nregular
        nregular = nregular - ncore

        # print("core_group len = %s" % len(core_group))
        # print("reg_group len = %s" % len(reg_group))
        # print("occ_group len = %s" % len(occ_group))

        # inserting values in variable
        community['core'] = ncore
        community['regular'] = nregular
        community['occasional'] = noccasional

        #print(community)

        groups["core"]= core_group
        groups["regular"]=reg_group
        groups["occasional"]=occ_group
        return(groups)
