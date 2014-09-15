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


import logging
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
        # gets the person info in two queries, first it obtains the name and later
        # the number of ocurrences if any. If not, it sets ocurrences = 0
        person_data = {}
        if (data_source.get_name() == "scm"):
            logging.info("Warning: current queries are counting merges")
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
            q0 = "select name, email from people, people_upeople pup "+\
                "where people.id = pup.people_id and pup.upeople_id = " + str(upeople_id) + " "+\
                " LIMIT 1"
            
            q1 = " select pup.upeople_id as uid, p.name, p.email, "+\
                "        (count(distinct(s.id))) as commits "+\
                " from scmlog s, "+\
                "      people_upeople pup, "+\
                "      people p "+\
                " where s.date>="+ from_date+" and "+\
                "       s.date<"+ to_date+" and "+\
                "       s.author_id = pup.people_id and "+\
                "       s.author_id = p.id and "+\
                "       p.email <> '%gerrit@%' and "+\
                "       p.email <> '%jenkins@%' and "+\
                "       pup.upeople_id = " + str(upeople_id) +\
                " group by pup.upeople_id "+\
                " order by commits desc; "

            aux = self.db.ExecuteQuery(q0)
            person_data["name"] = aux["name"]
            person_data["email"] = aux["email"]
            aux = self.db.ExecuteQuery(q1)
            if (type(aux["commits"]) == list):
                person_data["commits"] = 0
            else:
                person_data["commits"] = aux["commits"]

        elif (data_source.get_name() == "qaforums"):
            logging.info("Warning: qaforums is not using matched identities")
            q0 = "SELECT username as name from people where identifier = " + str(upeople_id)+ " "+\
                " LIMIT 1"
            
            q1 = "SELECT identifier, username as name, COUNT(*) as messages from ("+\
                "(select p.identifier as identifier, p.username, q.added_at as date"+\
                "  from questions q, people p"+\
                "  where q.author_identifier=p.identifier)"+\
                "union"+\
                "(select p.identifier as identifier, p.username, a.submitted_on as date"+\
                "  from answers a, people p"+\
                "  where a.user_identifier=p.identifier)"+\
                "union"+\
                "(select p.identifier as identifier, p.username, c.submitted_on as date"+\
                "  from comments c, people p"+\
                "  where c.user_identifier=p.identifier)) t "+\
                "WHERE date>="+ from_date +" AND date<" + to_date +" "+\
                " AND identifier = "+ str(upeople_id) + " "+\
                "group by identifier"
            aux = self.db.ExecuteQuery(q0)
            person_data["name"] = aux["name"]
            aux = self.db.ExecuteQuery(q1)
            if (type(aux["messages"]) == list):
                person_data["messages"] = 0
            else:
                person_data["messages"] = aux["messages"]
        return(person_data)

    def result(self, data_source = None, offset_days = None):
        if data_source.get_name() != "scm" \
            and data_source.get_name() != "qaforums": return None

        from dateutil import parser
        import datetime
            
        # calculation of the date ranges
        cur_to_date = self.filters.enddate
        if offset_days:
            dt_end = parser.parse(self.filters.enddate)
            aux = dt_end - datetime.timedelta(offset_days)
            cur_from_date = "'" + aux.strftime("%Y-%m-%d") + "'"
        else:
            cur_from_date = self.filters.startdate

        past_to_date = self.filters.startdate
        if offset_days:
            dt_end = parser.parse(self.filters.startdate)
            aux = dt_end - datetime.timedelta(offset_days)
            past_from_date = "'" + aux.strftime("%Y-%m-%d") + "'"
        else:
            dt_start = parser.parse(self.filters.startdate)
            dt_end = parser.parse(self.filters.enddate)
            timedelta = dt_end - dt_start
            new_startdate = dt_start - timedelta
            past_from_date = "'" + new_startdate.strftime("%Y-%m-%d") + "'"
        
        # getting the data
        #print("current %s - %s" % (cur_from_date, cur_to_date))
        current_groups = self._activity_groups(cur_from_date, cur_to_date, data_source)
        cur_core = current_groups["core"]        
        cur_regular = current_groups["regular"]
        cur_occasional = current_groups["occasional"]
        #print("past %s - %s" % (past_from_date, past_to_date))
        past_groups = self._activity_groups(past_from_date, past_to_date, data_source)
        past_core = past_groups["core"]
        past_regular = past_groups["regular"]
        past_occasional = past_groups["occasional"]

        # going up
        up_core = cur_core - past_core
        # we substract past_core cause we want people going up!
        up_reg = cur_regular - past_core - past_regular
        
        # going down
        down_reg = past_core.intersection(cur_regular)
        down_occ = past_regular.intersection(cur_occasional) | past_core.intersection(cur_occasional)

        groups = {"core":cur_core, "up_core":up_core, "up_reg":up_reg,
                  "down_reg":down_reg, "down_occ":down_occ}

        result = {}
        for g in groups:
            #here we define what we export in each group
            if (data_source.get_name() == "scm"):
                result[g] = {"name":[],"email":[],"commits":[]}
            elif (data_source.get_name() == "qaforums"):
                result[g] = {"name":[],"messages":[]}

            # and .. we get the data
            for person in groups[g]:
                user_data = self._get_person_info(person, self.filters.startdate, self.filters.enddate, data_source)
                result[g]["name"].append(user_data["name"])
                if (data_source.get_name() == "scm"):
                    result[g]["email"].append(user_data["email"])
                    result[g]["commits"].append(user_data["commits"])
                elif (data_source.get_name() == "qaforums"):
                    #print(user_data)
                    result[g]["messages"].append(user_data["messages"])
                #print user_data
                #print " --> "+ str(person)
                #result[g]["uid"].append(user_data["uid"])
        
        # print("core VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
        # c_set = []
        # a_set = []
        # for c in cur_core:
        #      aux = self._get_person_info(c, cur_from_date, cur_to_date, data_source)
        #      c_set.append(aux["commits"])
        # c_set.sort()
        # print c_set
        # print("regular ***********************************************")
        # for a in cur_regular:
        #      aux2 = self._get_person_info(a, cur_from_date, cur_to_date, data_source)
        #      a_set.append(aux2["commits"])
        # a_set.sort()
        # print a_set

        # print("core 22222222222222 VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
        # c_set = []
        # a_set = []
        # for c in cur_core:
        #      aux = self._get_person_info(c, self.filters.startdate, self.filters.enddate, data_source)
        #      c_set.append(aux["commits"])
        # c_set.sort()
        # print c_set
        # print("regular 2222222222222 ***********************************************")
        # for a in cur_regular:
        #      aux2 = self._get_person_info(a, self.filters.startdate, self.filters.enddate, data_source)
        #      a_set.append(aux2["commits"])
        # a_set.sort()
        # print a_set
        

        return (result)

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

    def _get_total_messages_query(self, from_date, to_date):
        q = "SELECT "+\
        "(select COUNT(*) from questions WHERE "+\
        " added_at>="+ from_date +" AND added_at<" + to_date + ") + "+\
        "(select COUNT(*) from comments WHERE "+\
        " submitted_on>="+ from_date +" AND submitted_on<" + to_date + ") + "+\
        "(SELECT COUNT(*) FROM answers WHERE "+\
        " submitted_on>="+ from_date +" AND submitted_on<" + to_date + ")"+\
        "as total;"
        return(q)
        
    def _get_personal_messages_query(self, from_date, to_date):
        q = "SELECT identifier, COUNT(*) as messages from ("+\
        "(select p.identifier as identifier, q.added_at as date"+\
        "  from questions q, people p"+\
        "  where q.author_identifier=p.identifier)"+\
        "union"+\
        "(select p.identifier as identifier, a.submitted_on as date"+\
        "  from answers a, people p"+\
        "  where a.user_identifier=p.identifier)"+\
        "union"+\
        "(select p.identifier as identifier, c.submitted_on as date"+\
        "  from comments c, people p"+\
        "  where c.user_identifier=p.identifier)) t "+\
        "WHERE date>="+ from_date +" AND date<" + to_date +" "+\
        "group by identifier order by messages desc"
        return(q)
    
    def _get_total_commits_query(self, from_date, to_date):
        logging.info("Warning: current queries are counting merges")
        # uncomment this query in order to remove the queries
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
             "      s.date<"+ to_date+";"
        return(q)

    def _get_personal_commits_query(self, from_date, to_date):
        logging.info("Warning: current queries are counting merges")
        # uncomment this query in order to remove the queries
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
            "       s.date<"+ to_date+" and "+\
            "       s.author_id = pup.people_id and "+\
            "       s.author_id = p.id and "+\
            "       p.email <> '%gerrit@%' and "+\
            "       p.email <> '%jenkins@%' "+\
            " group by pup.upeople_id "+\
            " order by commits desc; "
        return(q)

    def _activity_groups(self, from_date, to_date, data_source = None):
        if data_source.get_name() != "scm" and \
          data_source.get_name() != "qaforums": return None

        groups = {}

        # Init of structure to be returned
        community = {}
        community['core'] = None
        community['regular'] = None
        community['occasional'] = None

        if data_source.get_name() == "scm":        
            q = self._get_total_commits_query(from_date, to_date)
            total = self.db.ExecuteQuery(q)
            total_commits = float(total['total'])
            q = self._get_personal_commits_query(from_date, to_date)
            people = self.db.ExecuteQuery(q)
            if not isinstance(people['commits'], list):
                people['commits'] = [people['commits']]
            # this is a list. Operate over the list
            people['commits'] = [((commits / total_commits) * 100) for commits in people['commits']]
        elif data_source.get_name() == "qaforums":
            q = self._get_total_messages_query(from_date, to_date)
            total = self.db.ExecuteQuery(q)
            total_messages = float(total['total'])
            q = self._get_personal_messages_query(from_date, to_date)
            people = self.db.ExecuteQuery(q)
            if not isinstance(people['messages'], list):
                people['messages'] = [people['messages']]
            # this is a list. Operate over the list
            people['messages'] = [((messages / total_messages) * 100) for messages in people['messages']]

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

        # field used to browse people dict()
        if data_source.get_name() == "scm":
            field = 'commits'
            ident = 'uid'
        elif data_source.get_name() == "qaforums":
            field = 'messages'
            ident = 'identifier'
        
        #for p in people:
        for value in people[field]:
            cont = cont + value
            ndevs = ndevs + 1
            dev_email = people[ident][array_cont]

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
