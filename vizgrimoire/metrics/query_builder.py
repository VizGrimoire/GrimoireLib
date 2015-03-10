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
##   Alvaro del Castillo <acs@bitergia.com>

import logging
import MySQLdb
import re
import sys
from sets import Set

from vizgrimoire.metrics.metrics_filter import MetricFilters

class DSQuery(object):
    """ Generic methods to control access to db """

    db_conn_pool = {} # one connection per database

    def __init__(self, user, password, database, identities_db = None, host="127.0.0.1", port=3306, group=None):
        self.identities_db = identities_db
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.group = group
        if database in DSQuery.db_conn_pool:
            db = DSQuery.db_conn_pool[database]
        else:
            db = self.__SetDBChannel__(user, password, database, host, port, group)
            DSQuery.db_conn_pool[database] = db
        self.cursor = db.cursor()
        self.cursor.execute("SET NAMES 'utf8'")

        db = self.__SetDBChannel__(user, password, database, host, port, group)

        self.create_indexes()

    def create_indexes(self):
        """ Basic indexes used in each data source """
        pass

    def GetSQLGlobal(self, date, fields, tables, filters, start, end, all_items = None):
        group_field = None
        count_field = None
        if all_items:
            group_field = self.get_group_field(all_items, self)
            # Expected format: "count(distinct(pup.upeople_id)) AS authors"
            count_field = fields.split(" ")[2]
            fields = group_field + ", " + fields

        sql = 'SELECT '+ fields
        sql += ' FROM '+ tables
        sql += ' WHERE '+date+'>='+start+' AND '+date+'<'+end
        reg_and = re.compile("^[ ]*and", re.IGNORECASE)
        if (filters != ""):
            if (reg_and.match (filters.lower())) is not None: sql += " " + filters
            else: sql += ' AND '+filters

        if all_items:
            sql += " GROUP BY " + group_field
            sql += " ORDER BY " + count_field + " DESC," + group_field

        return(sql)

    def GetSQLPeriod(self, period, date, fields, tables, filters, start, end,
                     all_items = None):
        group_field = None
        if all_items :
            group_field = self.get_group_field(all_items, self)
            fields = group_field + ", " + fields

        iso_8601_mode = 3
        if (period == 'day'):
            # Remove time so unix timestamp is start of day    
            sql = 'SELECT UNIX_TIMESTAMP(DATE('+date+')) AS unixtime, '
        elif (period == 'week'):
            sql = 'SELECT YEARWEEK('+date+','+str(iso_8601_mode)+') AS week, '
        elif (period == 'month'):
            sql = 'SELECT YEAR('+date+')*12+MONTH('+date+') AS month, '
        elif (period == 'year'):
            sql = 'SELECT YEAR('+date+')*12 AS year, '
        else:
            logging.error("PERIOD: "+period+" not supported")
            raise Exception
        # sql = paste(sql, 'DATE_FORMAT (',date,', \'%d %b %Y\') AS date, ')
        sql += fields
        if all_items: fields + ", " + group_field
        sql += ' FROM ' + tables
        sql = sql + ' WHERE '+date+'>='+start+' AND '+date+'<'+end
        reg_and = re.compile("^[ ]*and", re.IGNORECASE)

        if (filters != ""):
            if (reg_and.match (filters.lower())) is not None: sql += " " + filters
            else: sql += ' AND ' + filters

        group_by = " GROUP BY "

        if all_items: group_by += group_field + ", "

        if (period == 'year'):
            sql += group_by + ' YEAR('+date+')'
            sql += ' ORDER BY YEAR('+date+')'
        elif (period == 'month'):
            sql += group_by + ' YEAR('+date+'),MONTH('+date+')'
            sql += ' ORDER BY YEAR('+date+'),MONTH('+date+')'
        elif (period == 'week'):
            sql += group_by + ' YEARWEEK('+date+','+str(iso_8601_mode)+')'
            sql += ' ORDER BY YEARWEEK('+date+','+str(iso_8601_mode)+')'
        elif (period == 'day'):
            sql += group_by + ' YEAR('+date+'),DAYOFYEAR('+date+')'
            sql += ' ORDER BY YEAR('+date+'),DAYOFYEAR('+date+')'
        else:
            logging.error("PERIOD: "+period+" not supported")
            sys.exit(1)

        if all_items:
            sql += "," + group_field
            # logging.info("GROUP sql")
            # print sql

        return(sql)

    def _get_fields_query(self, fields):
        # Returns a string with fields separated by ","
        fields_str = ""

        if len(fields) > 0:
            fields_str = str(fields.pop())
            for i in range(len(fields)):
                fields_str += " , " + str(fields.pop())

        return fields_str

    def _get_tables_query(self, tables):
        # Returns a string with tables separated by ","
        tables_str = ""

        if len(tables) > 0:
            tables_str = str(tables.pop())
            for i in range(len(tables)):
                tables_str += " , " + str(tables.pop())

        return tables_str

    def _get_filters_query(self, filters):
        # Returns a string with filters separated by "and"
        filters_str = ""

        if len(filters) > 0:
            filters_str = str(filters.pop())
            for i in range(len(filters)):
                filters_str += " and " + str(filters.pop())
        return filters_str

    def BuildQuery (self, period, startdate, enddate, date_field, fields,
                    tables, filters, evolutionary, type_analysis = None):
        # Select the way to evolutionary or aggregated dataset
        # filter_all: get data for all items in a filter
        q = ""

        if isinstance(fields, Set):
            # Special case where query fields are sets.
            # TODO: The "if" should be removed after the migration given that
            # all of the queries will use this.
            fields = self._get_fields_query(fields)
            tables = self._get_tables_query(tables)
            filters = self._get_filters_query(filters)

        # if all_items build a query for getting all items in one query
        all_items = None
        if type_analysis:
            all_items = type_analysis[0]
            if type_analysis[1] is not None:
                # all_items only used for global filter, not for items filter
                all_items = None 

        if (evolutionary):
            q = self.GetSQLPeriod(period, date_field, fields, tables, filters,
                                  startdate, enddate, all_items)
        else:
            q = self.GetSQLGlobal(date_field, fields, tables, filters,
                                  startdate, enddate, all_items)
        return(q)

    def __SetDBChannel__ (self, user=None, password=None, database=None,
                      host="127.0.0.1", port=3306, group=None):
        if (group == None):
            db = MySQLdb.connect(user=user, passwd=password,
                                 db=database, host=host, port=port)
        else:
            db = MySQLdb.connect(read_default_group=group, db=database)

        return db

    def ExecuteQuery (self, sql):
        if sql is None: return {}
        result = {}
        self.cursor.execute(sql)
        rows = self.cursor.rowcount
        columns = self.cursor.description

        if columns is None: return result

        for column in columns:
            result[column[0]] = []
        if rows > 1:
            for value in self.cursor.fetchall():
                for (index,column) in enumerate(value):
                    result[columns[index][0]].append(column)
        elif rows == 1:
            value = self.cursor.fetchone()
            for i in range (0, len(columns)):
                result[columns[i][0]] = value[i]
        return result

    def ExecuteViewQuery(self, sql):
        self.cursor.execute(sql)

    def get_subprojects(self, project):
        """ Return all subprojects ids for a project in a string join by comma """

        q = "SELECT project_id from %s.projects WHERE id='%s'" % (self.identities_db, project)
        project_id = self.ExecuteQuery(q)['project_id']

        q = """
            SELECT subproject_id from %s.project_children pc where pc.project_id = '%s'
        """ % (self.identities_db, project_id)

        subprojects = self.ExecuteQuery(q)

        if not isinstance(subprojects['subproject_id'], list):
            subprojects['subproject_id'] = [subprojects['subproject_id']]

        project_with_children = subprojects['subproject_id'] + [project_id]
        project_with_children_str = ','.join(str(x) for x in project_with_children)

        if (project_with_children_str == "[]"): project_with_children_str = "NULL"

        return  project_with_children_str

    @staticmethod
    def get_group_field (filter_type, ds_query = None):
        """ Return the name of the field to group by in filter all queries """
        field = None
        supported = ['people2','company','country','domain','project','repository','company,country']

        analysis = filter_type

        if analysis not in supported:
            raise Exception("Can't get_group_field for " +  filter_type)
        if analysis == 'people2': field = "up.identifier"
        elif analysis == "company": field = "com.name"
        elif analysis == "country": field = "cou.name"
        elif analysis == "domain": field = "d.name"
        elif analysis == "repository":
            field = "r.name"
            if type(ds_query) == ITSQuery: field = "t.url"
        elif analysis == "company,country": field = "CONCAT(com.name,'_',cou.name)"

        return field

    @staticmethod
    def get_bots_filter_sql (data_source, metric_filters = None):
        bots = data_source.get_bots()
        if metric_filters is not None:
            if metric_filters.people_out is not None:
                bots = metric_filters.people_out
        filter_bots = ''
        for bot in bots:
            filter_bots = filter_bots + " up.identifier<>'"+bot+"' AND "
        if filter_bots != '': filter_bots = filter_bots[:-4]
        return filter_bots

class SCMQuery(DSQuery):
    """ Specific query builders for source code management system data source """

    def GetSQLRepositoriesFrom (self):
        #tables necessaries for repositories
        tables = Set([])
        tables.add("repositories r")

        return tables

    def GetSQLRepositoriesWhere (self, repository):
        #fields necessaries to match info among tables
        fields = Set([])
        fields.add("r.id = s.repository_id")
        if repository is not None: fields.add("r.name ="+ repository)

        return fields

    def GetSQLProjectFrom (self):
        #tables necessaries for repositories
        tables = Set([])
        tables.add("repositories r")

        return tables

    def GetSQLProjectWhere (self, project):
        # include all repositories for a project and its subprojects
        # Remove '' from project name
        if (project[0] == "'" and project[-1] == "'"):
            project = project[1:-1]

        fields = Set([])

        repos = """r.uri IN (
               SELECT repository_name
               FROM   %s.projects p, %s.project_repositories pr
               WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
                     AND pr.data_source='scm'
               )""" % (self.identities_db, self.identities_db, self.get_subprojects(project))
        fields.add(repos)
        fields.add("r.id = s.repository_id")

        return fields

    def GetSQLCompaniesFrom (self):
        #tables necessaries for companies
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople_companies upcom")
        tables.add(self.identities_db + ".companies com")

        return tables

    def GetSQLCompaniesWhere (self, company, role):
         #fields necessaries to match info among tables
         fields = Set([])
         fields.add("s."+role+"_id = pup.people_id")
         fields.add("pup.upeople_id = upcom.upeople_id")
         fields.add("s.author_date >= upcom.init")
         fields.add("s.author_date < upcom.end")
         fields.add("upcom.company_id = com.id")
         if company is not None: fields.add("com.name =" + company)

         return fields

    def GetSQLCountriesFrom (self):
        #tables necessaries for companies
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople_countries upcou")
        tables.add(self.identities_db + ".countries cou")

        return tables

    def GetSQLCountriesWhere (self, country, role):
         #fields necessaries to match info among tables
        fields = Set([])
        fields.add("s."+role+"_id = pup.people_id")
        fields.add("pup.upeople_id = upcou.upeople_id")
        fields.add("upcou.country_id = cou.id")
        if country is not None: fields.add("cou.name ="+ country)

        return fields

    def GetSQLDomainsFrom (self) :
        #tables necessaries for domains
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople_domains upd")
        tables.add(self.identities_db + ".domains d")

        return tables

    def GetSQLDomainsWhere (self, domain, role) :
        #fields necessaries to match info among tables
        fields = Set([])
        fields.add("s."+role+"_id = pup.people_id")
        fields.add("pup.upeople_id = upd.upeople_id")
        fields.add("upd.domain_id = d.id")
        if domain is not None: fields.add("d.name ="+ domain)

        return fields

    def GetSQLBranchFrom(self):
        # tables necessary to limit the analysis to certain branches
        tables = Set([])
        tables.add("actions a")
        tables.add("branches b")

        return tables

    def GetSQLBranchWhere(self, branch):
        # filters necessary to limit the analysis to certain branches
        fields = Set([])
        fields.add("a.commit_id = s.id")
        fields.add("a.branch_id = b.id")
        fields.add("b.name =" + branch)

        return fields

    def GetSQLModuleFrom(self):
        # tables necessary to limit the analysis to specific directories path
        tables = Set([])
        tables.add("file_links fl")

        return tables 

    def GetSQLModuleWhere(self, module):
        # filters necessary to limit the analysis to specific directories path
        filters = Set([])
        filters.add("fl.commit_id = s.id")
        module = module.replace("'", "")
        filters.add("fl.file_path like '" + module + "%'")

        return filters

    def GetSQLFileTypeFrom(self):
        # tables necessary to filter by type of file
        tables = Set([])
        tables.add("actions a")
        tables.add("file_types ft")

        return tables

    def GetSQLFileTypeWhere(self, filetype):
        """ Filters necessary to filter by type of file.

        As specified by CVSAnalY, there are 9 different types:
        unknown, devel-doc, build, code, i18n, documentation,
        image, ui, package.

        Those strings are the one expected in the value of filetype
        """
        filters = Set([])
        filters.add("a.commit_id = s.id")
        filters.add("a.file_id = ft.file_id")
        filters.add("ft.type =" + filetype)

        return filters

    def GetSQLLogMessageFrom(self):
        # tables necessary to filter by message left by developers
        tables = Set([])
        tables.add("scmlog s")

        return tables

    def GetSQLLogMessageWhere(self, message):
        # filters necessary to filter by message left by developers
        filters = Set([])
        message = message.replace("'", "")
        filters.add("s.message like '%"+message+"%'")

        return filters

    def GetSQLNotLogMessageFrom(self):
        # tables necessary to filter by message left by developers
        tables = Set([])
        tables.add("scmlog s")

        return tables

    def GetSQLNotLogMessageWhere(self, message):
        # filters necessary to filter by message left by developers
        filters = Set([])
        if type(message) is str:
            filters.add("s.message not like '%"+message+"%'")
        if type(message) is list:
            val = ""
            for item in message:
                val += "s.message not like '%"+item+"%' AND "
            val = "("+val [:-4]+")" # remove last OR and add ()
            filters.add(val)
        return filters

    def GetSQLPeopleFrom (self):
        #tables necessaries for companies
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople up")

        return tables

    def GetSQLPeopleWhere (self, name):
        #fields necessaries to match info among tables
        fields = Set([])
        fields.add("s.author_id = pup.people_id")
        fields.add("up.id = pup.upeople_id")
        if name is not None: fields.add("up.identifier = "+name)

        return fields

    def GetSQLBotFrom(self):
        # Bots are removed from the upeople table, using the upeople.identifier table.
        # Another option is to remove those bots directly in the people table.
        tables = Set([])
        #tables.add("scmlog s")
        tables.add("people p")
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople u")

        return tables

    def GetSQLBotWhere(self, bots_str):
        # Based on the tables provided in GetSQLBotFrom method, 
        # this method provides the clauses to join the several tables

        bots = bots_str
        if not isinstance(bots_str, list):
            bots = bots_str.split(",")
        where = Set([])
        where.add("s.author_id = p.id")
        where.add("p.id = pup.people_id")
        where.add("pup.upeople_id = u.id")
        for bot in bots:
            # This code only ignores bots provided in raw_bots.
            # This should add the other way around, u.identifier = 'xxx'
            where.add("u.identifier <> '" + bot + "'")

        return where

    def _get_from_type_analysis_set(self, type_analysis):

        From = Set([])

        #if (type_analysis is None or len(type_analysis) != 2): return from_str
        # the type_analysis length !=2 error should be raised in the MetricFilter instance
        if type_analysis is not None:
            # To be improved... not a very smart way of doing this
            list_analysis = type_analysis[0].split(",")

            analysis = type_analysis[0]

            # Retrieving tables based on the required type of analysis.
            for analysis in list_analysis:
                if analysis == 'repository': From.union_update(self.GetSQLRepositoriesFrom())
                elif analysis == 'company': From.union_update(self.GetSQLCompaniesFrom())
                elif analysis == 'country': From.union_update(self.GetSQLCountriesFrom())
                elif analysis == 'domain': From.union_update(self.GetSQLDomainsFrom())
                elif analysis == 'project': From.union_update(self.GetSQLProjectFrom())
                elif analysis == 'branch': From.union_update(self.GetSQLBranchFrom())
                elif analysis == 'module': From.union_update(self.GetSQLModuleFrom())
                elif analysis == 'filetype': From.union_update(self.GetSQLFileTypeFrom())
                elif analysis == 'logmessage': From.union_update(self.GetSQLLogMessageFrom())
                elif analysis == 'notlogmessage': From.union_update(self.GetSQLNotLogMessageFrom())
                elif analysis == 'people': From.union_update(self.GetSQLPeopleFrom())
                elif analysis == 'people2': From.union_update(self.GetSQLPeopleFrom())
                else: raise Exception( analysis + " not supported in From")

        return From

    def GetSQLReportFrom (self, filters):
        # generic function to generate "from" clauses
        # "filters" is an instance of MetricsFilter that contains all of the
        # information needed to build the where clauses.

        From = self._get_from_type_analysis_set(filters.type_analysis)


        # Adding tables (if) needed when filtering bots.
        if filters.people_out is not None:
            From.union_update(self.GetSQLBotFrom())
        if filters.global_filter is not None:
            From.union_update(self._get_from_type_analysis_set(filters.global_filter))


        return (From)

    # TODO: share between all data sources
    def _get_where_global_filter_set(self, global_filter):
        if len(global_filter) == 2:
            fields = global_filter[0].split(",")
            values = global_filter[1].split(",")
            if len(fields) > 1:
                if fields.count(fields[0]) == len(fields):
                    # Same fields, different values, use OR
                    global_filter = [fields[0],values]

        global_where = self._get_where_type_analysis_set(global_filter)

        return global_where


    def _get_where_type_analysis_set(self, type_analysis, role = None):
        where = Set([])

        #if (type_analysis is None or len(type_analysis) != 2): return where_str
        # the type_analysis !=2 error should be raised when building a new instance 
        # of the class MetricFilter
        if type_analysis is not None and len(type_analysis)>1:
            analysis = type_analysis[0]
            values = type_analysis[1]

            if values is not None:
                if type(values) is str:
                    # To be improved... not a very smart way of doing this...
                    list_values = values.split(",")
                elif type(values) is list:
                    # On item or list of lists. Unify to list of lists
                    list_values = values
                    if list_values[0] is not list:
                        list_values = [list_values]
            else:
                list_values = None

            list_analysis = type_analysis[0].split(",")

            pos = 0
            for analysis in list_analysis:
                if list_values is not None:
                    value = list_values[pos]
                else:
                    value = None

                if analysis == 'repository': where.union_update(self.GetSQLRepositoriesWhere(value))
                elif analysis == 'company': where.union_update(self.GetSQLCompaniesWhere(value, role))
                elif analysis == 'country': where.union_update(self.GetSQLCountriesWhere(value, role))
                elif analysis == 'domain': where.union_update(self.GetSQLDomainsWhere(value, role))
                elif analysis == 'project': where.union_update(self.GetSQLProjectWhere(value))
                elif analysis == 'branch': where.union_update(self.GetSQLBranchWhere(value))
                elif analysis == 'module': where.union_update(self.GetSQLModuleWhere(value))
                elif analysis == 'filetype': where.union_update(self.GetSQLFileTypeWhere(value))
                elif analysis == 'logmessage': where.union_update(self.GetSQLLogMessageWhere(value))
                elif analysis == 'notlogmessage': where.union_update(self.GetSQLNotLogMessageWhere(value))
                elif analysis == 'people': where.union_update(self.GetSQLPeopleWhere(value))
                elif analysis == 'people2': where.union_update(self.GetSQLPeopleWhere(value))
                else: raise Exception( analysis + " not supported in Where")

                pos = pos + 1
        return where


    def GetSQLReportWhere (self, filters, role = "author"):
        # Generic function to generate 'where' clauses
        # 'filters' is an instance of MetricsFilter class with all of the
        # conditions needed to build the where clauses

        where = self._get_where_type_analysis_set(filters.type_analysis, role)

        # Adding conditions (if) needed when filtering bots
        if filters.people_out is not None:
            where.union_update(self.GetSQLBotWhere(filters.people_out))
        if filters.global_filter is not None:
            where.union_update(self._get_where_global_filter_set(filters.global_filter))

        return where

    # To be used in the future for apply a generic filter to all queries
    def GetCommitsFiltered(self):
        filters = ""
        return filters

    def GetPeopleQuerySCM (self, developer_id, period, startdate, enddate, evol) :
        fields ='COUNT(distinct(s.id)) AS commits'
        tables = " actions a, scmlog s, people_upeople pup "
        filters = "pup.people_id = s.author_id and s.id = a.commit_id "
        filters +=" AND pup.upeople_id="+str(developer_id)
        if (evol) :
            q = self.GetSQLPeriod(period,'s.author_date', fields, tables, filters,
                    startdate, enddate)
        else :
            fields += ",DATE_FORMAT (min(s.author_date),'%Y-%m-%d') as first_date, "+\
                      "DATE_FORMAT (max(s.author_date),'%Y-%m-%d') as last_date"
            q = self.GetSQLGlobal('s.author_date', fields, tables, filters, 
                    startdate, enddate)

        return (q)

    def GetEvolPeopleSCM (self, developer_id, period, startdate, enddate) :
        q = self.GetPeopleQuerySCM (developer_id, period, startdate, enddate, True)

        data = self.ExecuteQuery(q)
        return (data)

    def GetStaticPeopleSCM (self, developer_id, startdate, enddate) :
        q = self.GetPeopleQuerySCM (developer_id, None, startdate, enddate, False)

        data = self.ExecuteQuery(q)
        return (data)

    def GetPeopleIntake(self, min, max):
        filters = self.GetCommitsFiltered()
        if (filters != ""): filters  = " WHERE " + filters
        filters = ""

        q_people_num_commits_evol = """
            SELECT COUNT(*) AS total, author_id,
                YEAR(date) as year, MONTH(date) as monthid
            FROM scmlog
            %s
            GROUP BY author_id, year, monthid
            HAVING total > %i AND total <= %i
            ORDER BY date DESC
            """ % (filters, min, max)

        q_people_num_evol = """
            SELECT COUNT(*) as people, year*12+monthid AS month
            FROM (%s) t
            GROUP BY year, monthid
            """ % (q_people_num_commits_evol)

        return self.ExecuteQuery(q_people_num_evol)

class ITSQuery(DSQuery):
    """ Specific query builders for issue tracking system data source """
    def GetSQLRepositoriesFrom (self):
        # tables necessary for repositories 
        tables = Set([])
        tables.add("trackers t")

        return tables

    def GetSQLRepositoriesWhere (self, repository):
        # fields necessary to match info among tables
        filters = Set([])
        filters.add("i.tracker_id = t.id")
        if repository is not None: filters.add("t.url = "+repository)

        return filters

    def GetSQLProjectsFrom (self):
        # tables necessary for repositories
        tables = Set([])
        tables.add("trackers t")

        return tables

    def GetSQLProjectsWhere (self, project):
        # include all repositories for a project and its subprojects
        # Remove '' from project name
        filters = Set([])
        if len(project) > 1 :
            if (project[0] == "'" and project[-1] == "'"):
                project = project[1:-1]

        subprojects = self.get_subprojects(project)

        repos = """t.url IN (
               SELECT repository_name
               FROM   %s.projects p, %s.project_repositories pr
               WHERE  p.project_id = pr.project_id AND pr.data_source='its'
        """ % (self.identities_db, self.identities_db)

        if subprojects != "[]":
            repos += " AND p.project_id IN (%s) " % subprojects

        filters.add(repos + ")")
        filters.add("t.id = i.tracker_id")

        return filters

    def GetSQLCompaniesFrom (self):
        # fields necessary for the companies analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".companies com")
        tables.add(self.identities_db + ".upeople_companies upcom")

        return tables

    def GetSQLCompaniesWhere (self, name):
        # filters for the companies analysis
        filters = Set([])
        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.upeople_id = upcom.upeople_id")
        filters.add("upcom.company_id = com.id")
        filters.add("i.submitted_on >= upcom.init")
        filters.add("i.submitted_on < upcom.end")
        if name is not None:
            if type(name) is str:
                filters.add("com.name = "+name)
            if type(name) is list:
                val = ""
                for iname in name:
                    val += "com.name = "+ iname + " OR "
                val = "("+val [:-4]+")" # remove last OR and add ()
                filters.add(val)

        return filters

    def GetSQLCountriesFrom (self):
        # fields necessary for the countries analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".countries cou")
        tables.add(self.identities_db + ".upeople_countries upcou")

        return tables

    def GetSQLCountriesWhere (self, name):
        # filters for the countries analysis
        filters = Set([])
        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.upeople_id = upcou.upeople_id")
        filters.add("upcou.country_id = cou.id")
        if name is not None: filters.add("cou.name = "+name)

        return filters

    def GetSQLDomainsFrom (self):
        # fields necessary for the domains analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".domains d")
        tables.add(self.identities_db + ".upeople_domains upd")

        return tables

    def GetSQLDomainsWhere (self, name):
        # filters for the domains analysis
        filters = Set([])
        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.upeople_id = upd.upeople_id")
        filters.add("upd.domain_id = d.id")
        if name is not None: filters.add("d.name = " + name)

        return filters

    def GetSQLPeopleFrom (self):
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople up")

        return tables

    def GetSQLPeopleWhere (self, name, table = "changes"):
        filters = Set([])

        field = "ch.changed_by"

        if table == "issues": field = "i.submitted_by"

        filters.add(field + " = pup.people_id")
        filters.add("up.id = pup.upeople_id")

        if name is not None: filters.add("up.identifier = " + name)

        return filters

    def GetSQLTicketTypeFrom (self):
        tables = Set([])

        return tables

    def GetSQLTicketTypeWhere (self, ticket_type):
        filters = Set([])
        if type(ticket_type) is str:
            filters.add("i.type = " + ticket_type)
        if type(ticket_type) is list:
            val = ""
            for item in ticket_type:
                val += "i.type = "+ item + " OR "
            val = "("+val [:-4]+")" # remove last OR and add ()
            filters.add(val)

        return filters

    def _get_from_type_analysis_set(self, type_analysis):

        From = Set([])

        if type_analysis is not None and len(type_analysis)>1:
            # To be improved... not a very smart way of doing this
            list_analysis = type_analysis[0].split(",")

            # Retrieving tables based on the required type of analysis.
            for analysis in list_analysis:
                if analysis == 'repository': From.union_update(self.GetSQLRepositoriesFrom())
                elif analysis == 'company': From.union_update(self.GetSQLCompaniesFrom())
                elif analysis == 'country': From.union_update(self.GetSQLCountriesFrom())
                elif analysis == 'domain': From.union_update(self.GetSQLDomainsFrom())
                elif analysis == 'project': From.union_update(self.GetSQLProjectsFrom())
                elif analysis == 'people2': From.union_update(self.GetSQLPeopleFrom())
                elif analysis == 'ticket_type': From.union_update(self.GetSQLTicketTypeFrom())
                else: raise Exception( analysis + " not supported")

        return From


    def GetSQLReportFrom (self, filters):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of
        #such analysis

        From = self._get_from_type_analysis_set(filters.type_analysis)

        if filters.global_filter is not None:
            From.union_update(self._get_from_type_analysis_set(filters.global_filter))

        return From

    def _get_where_global_filter_set(self, global_filter):
        if len(global_filter) == 2:
            fields = global_filter[0].split(",")
            values = global_filter[1].split(",")
            if len(fields) > 1:
                if fields.count(fields[0]) == len(fields):
                    # Same fields, different values, use OR
                    global_filter = [fields[0],values]

        global_where = self._get_where_type_analysis_set(global_filter)

        return global_where


    def _get_where_type_analysis_set(self, type_analysis):
        #"type" is a list of two values: type of analysis and value of
        #such analysis
        where = Set([])

        if type_analysis is not None and len(type_analysis)>1:
            analysis = type_analysis[0]
            values = type_analysis[1]

            if values is not None:
                if type(values) is str:
                    # To be improved... not a very smart way of doing this...
                    list_values = values.split(",")
                elif type(values) is list:
                    # On item or list of lists. Unify to list of lists
                    list_values = values
                    if list_values[0] is not list:
                        list_values = [list_values]
            else:
                list_values = None

            list_analysis = type_analysis[0].split(",")

            pos = 0
            for analysis in list_analysis:
                if list_values is not None:
                    value = list_values[pos]
                else:
                    value = None

                if analysis == 'repository': where.union_update(self.GetSQLRepositoriesWhere(value))
                elif analysis == 'company': where.union_update(self.GetSQLCompaniesWhere(value))
                elif analysis == 'country': where.union_update(self.GetSQLCountriesWhere(value))
                elif analysis == 'domain': where.union_update(self.GetSQLDomainsWhere(value))
                elif analysis == 'project': where.union_update(self.GetSQLProjectsWhere(value))
                elif analysis == 'people2': where.union_update(self.GetSQLPeopleWhere(value, table))
                elif analysis == 'ticket_type': where.union_update(self.GetSQLTicketTypeWhere(value))
                else: raise Exception( analysis + " not supported")
                pos += 1
        return where

    def GetSQLReportWhere (self, filters, table = "changes"):
        #generic function to generate 'where' clauses

        where = self._get_where_type_analysis_set(filters.type_analysis)

        if filters.global_filter is not None:
            where.union_update(self._get_where_global_filter_set(filters.global_filter))

        return where

    def GetSQLIssuesStudies (self, mfilters, type_analysis, evolutionary, study):

        period = mfilters.period
        startdate = mfilters.startdate
        enddate = mfilters.enddate
        identities_db = self.identities_db

        # Generic function that counts evolution/agg number of specific studies with similar
        # database schema such as domains, companies and countries
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(name)) as " + study)
        tables.add("issues i")
        mtype_analysis = mfilters.type_analysis
        mfilters.type_analysis = type_analysis
        tables.union_update(self.GetSQLReportFrom(mfilters))
        filters.union_update(self.GetSQLReportWhere(mfilters))
        mfilters.type_analysis = mtype_analysis

        #Filtering last part of the query, not used in this case
        #filters = gsub("and\n( )+(d|c|cou|com).name =.*$", "", filters)

        filters_ext = Set([])
        for i in range(0, len(filters)):
            filter_str = filters.pop()
            if not re.match("(d|c|cou|com).name.*=", filter_str):
                filters_ext.add(filter_str)

        query = self.BuildQuery(period, startdate, enddate, " i.submitted_on ", fields, tables, filters_ext, evolutionary)

        return query

    # TODO: use this queries en GetSQLReportWhere and From
    def GetTablesOwnUniqueIds (self, table='') :
        tables = Set([])
        # TODO: The acronym "c" is already used for companies
        tables.add("changes c")
        tables.add("people_upeople pup")
        if (table == "issues"):
            tables = Set([])
            tables.add("issues i")
            tables.add("people_upeople pup")

        return (tables)

    def GetFiltersOwnUniqueIds (self, table='') :
        filters = Set([])
        filters.add("pup.people_id = c.changed_by")
        if (table == "issues"):
            filters = Set([])
            filters.add("pup.people_id = i.submitted_by")

        return (filters)

    # TODO: Companies using unique ids
    def GetTablesCompanies (self, table='') :
        tables = Set([])
        tables.union_update(self.GetTablesOwnUniqueIds(table))
        tables.add(self.identities_db + ".upeople_companies upc")

        return (tables)

    def GetFiltersCompanies (self, table='') :
        filters = Set([])
        filters.union_update(self.GetFiltersOwnUniqueIds(table))
        filters.add("pup.upeople_id = upc.upeople_id")
        if (table == 'issues') :
            filters.add("submitted_on >= upc.init")
            filters.add("submitted_on < upc.end")
        else :
             filters.add("changed_on >= upc.init")
             filters.add("changed_on < upc.end")

        return (filters)

    def GetTablesDomains (self, table='') :
        tables = Set([])

        tables.union_update(self.GetTablesOwnUniqueIds(table))
        tables.add(self.identities_db + ".upeople_domains upd")

        return(tables)

    def GetFiltersDomains (self, table='') :
        filters = Set([])

        filters.union_update(self.GetFiltersOwnUniqueIds(table))
        filters.add("pup.upeople_id = upd.upeople_id")

        return(filters)

class MLSQuery(DSQuery):
    """ Specific query builders for mailing lists data source """
    def GetSQLRepositoriesFrom (self):
        # tables necessary for repositories
        #return (" messages m ") 
        tables = Set([])
        return tables

    def GetSQLRepositoriesWhere (self, repository):
        # fields necessary to match info among tables
        filters = Set([])
        filters.add("m.mailing_list_url = " + repository)

        return filters

    def GetSQLCompaniesFrom (self):
        # fields necessary for the companies analysis
        tables = Set([])
        tables.add("messages_people mp")
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople_companies upc")
        tables.add(self.identities_db + ".companies c")

        return tables

    def GetSQLCompaniesWhere (self, name):
        # filters for the companies analysis
        filters = Set([])
        filters.add("m.message_ID = mp.message_id")
        filters.add("mp.email_address = pup.people_id")
        filters.add("mp.type_of_recipient = \'From\'")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.company_id = c.id")
        filters.add("m.first_date >= upc.init")
        filters.add("m.first_date < upc.end")
        if name <> "" and name is not None:
            filters.add("c.name = "+name)

        return filters

    def GetSQLCountriesFrom (self):
        # fields necessary for the countries analysis
        tables = Set([])
        tables.add("messages_people mp")
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".countries c")
        tables.add(self.identities_db + ".upeople_countries upc")

        return tables

    def GetSQLCountriesWhere (self, name):
        # filters necessary for the countries analysis
        filters = Set([])
        filters.add("m.message_ID = mp.message_id")
        filters.add("mp.email_address = pup.people_id")
        filters.add("mp.type_of_recipient = \'From\'")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.country_id = c.id")
        if name <> "":
            filters.add("c.name = " + name)

        return filters

    def GetSQLDomainsFrom (self):
        tables = Set([])
        tables.add("messages_people mp")
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".domains d")
        tables.add(self.identities_db + ".upeople_domains upd")

        return tables

    def GetSQLDomainsWhere (self, name):
        filters = Set([])
        filters.add("m.message_ID = mp.message_id")
        filters.add("mp.email_address = pup.people_id")
        filters.add("mp.type_of_recipient = \'From\'")
        filters.add("pup.upeople_id = upd.upeople_id")
        filters.add("upd.domain_id = d.id")
        filters.add("m.first_date >= upd.init")
        filters.add("m.first_date < upd.end")
        if name <> "":
            filters.add("d.name = " + name)

        return filters

    def GetSQLProjectsFrom(self):
        tables = Set([])
        tables.add("mailing_lists ml")

        return tables

    def GetSQLProjectsWhere(self, project):
        # include all repositories for a project and its subprojects
        p = project.replace("'", "") # FIXME: why is "'" needed in the name?

        repos = Set([])

        repos_str = """ml.mailing_list_url IN (
               SELECT repository_name
               FROM   %s.projects p, %s.project_repositories pr
               WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
                   AND pr.data_source='mls'
        )""" % (self.identities_db, self.identities_db, self.get_subprojects(p))
        repos.add(repos_str)
        repos.add("ml.mailing_list_url = m.mailing_list_url")

        return repos

    def GetSQLPeopleFrom (self):
        tables = Set([])
        tables.add("messages_people mp")
        tables.add("people_upeople pup")
        #TODO: change "upeople up" by "upeople u" accordingly to the
        #      rest of GrimoireLib
        tables.add(self.identities_db + ".upeople up")

        return tables

    def GetSQLPeopleWhere (self, name):
        #fields necessaries to match info among tables
        filters = Set([])
        filters.add("m.message_ID = mp.message_id")
        filters.add("mp.email_address = pup.people_id")
        filters.add("mp.type_of_recipient = \'From\'")
        filters.add("up.id = pup.upeople_id")

        if name is not None: 
            filters.add("up.identifier = " + name)

        return filters

    # Using senders only here!
    def GetFiltersOwnUniqueIds  (self):
        filters = Set([])
        filters.add("m.message_ID = mp.message_id")
        filters.add("mp.email_address = pup.people_id")
        filters.add("mp.type_of_recipient = \'From\'")

        return filters

    def GetTablesOwnUniqueIds (self):
        tables = Set([])
        tables.add("messages m")
        tables.add("messages_people mp")
        tables.add("people_upeople pup")

        return tables


    ##########
    #Generic functions to obtain FROM and WHERE clauses per type of report
    ##########

    def GetSQLBotFrom(self):
        # Tables needed to filter bots in mailing lists
        tables = Set([])

        tables.add("messages m")
        tables.add("messages_people mp")
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople u")

        return tables

    def GetSQLReportFrom (self, filters):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of 
        #such analysis

        From = Set([])

        type_analysis = filters.type_analysis
        #if (type_analysis is None or len(type_analysis) != 2): return From
        if type_analysis is not None:
            list_analysis = type_analysis[0].split(",") 
            #analysis = type_analysis[0]

            for analysis in list_analysis:
                if analysis == 'repository': From.union_update(self.GetSQLRepositoriesFrom())
                elif analysis == 'company': From.union_update(self.GetSQLCompaniesFrom())
                elif analysis == 'country': From.union_update(self.GetSQLCountriesFrom())
                elif analysis == 'domain': From.union_update(self.GetSQLDomainsFrom())
                elif analysis == 'project': From.union_update(self.GetSQLProjectsFrom())
                elif analysis == 'people2': From.union_update(self.GetSQLPeopleFrom())
                else: raise Exception( analysis + " not supported")

        if filters.people_out is not None:
            From.union_update(self.GetSQLBotFrom())

        return (From)

    def GetSQLBotWhere(self, bots):
        # Based on the tables provided in GetSQLBotFrom method, 
        # this method provides the clauses to join the several tables

        where = Set([])
        where.add("m.message_ID = mp.message_id")
        where.add("mp.email_address = pup.people_id")
        where.add("mp.type_of_recipient = \'From\'")
        where.add("pup.upeople_id = u.id")
        for bot in bots:
            where.add("u.identifier <> '" + bot + "'")

        return where

    def GetSQLReportWhere (self, filters):
        #generic function to generate 'where' clauses
        #"type" is a list of two values: type of analysis and value of 
        #such analysisd

        where = Set([])

        type_analysis = filters.type_analysis

        #if (type_analysis is None or len(type_analysis) != 2): return where
        if type_analysis is not None:
            analysis = type_analysis[0]
            value = type_analysis[1]

            # To be improved... not a very smart way of doing this...
            if type_analysis[1] is not None:
                list_values = type_analysis[1].split(",")
            else:
                list_values = None
            list_analysis = type_analysis[0].split(",")

            for analysis in list_analysis:
                if list_values is not None:
                    value = list_values[list_analysis.index(analysis)]
                else:
                    value = None
                if analysis == 'repository': where.union_update(self.GetSQLRepositoriesWhere(value))
                elif analysis == 'company': where.union_update(self.GetSQLCompaniesWhere(value))
                elif analysis == 'country': where.union_update(self.GetSQLCountriesWhere(value))
                elif analysis == 'domain': where.union_update(self.GetSQLDomainsWhere(value))
                elif analysis == 'people2': where.union_update(self.GetSQLPeopleWhere(value))
                elif analysis == 'project':
                    if (self.identities_db is None):
                        logging.error("project filter not supported without identities_db")
                        sys.exit(0)
                    else:
                        where.union_update(self.GetSQLProjectsWhere(value))
                elif analysis == 'people2': where.union_update(self.GetSQLPeopleWhere(value))

        if filters.people_out is not None:
            where.union_update(self.GetSQLBotWhere(filters.people_out))

        return (where)

    def GetStudies (self, period, startdate, enddate, type_analysis, evolutionary, study):
        # Generic function that counts evolution/agg number of specific studies with similar
        # database schema such as domains, companies and countries

        fields = Set([])
        tables = Set([])
        filters = Set([])

        metric_filters = MetricFilters(period, startdate, enddate, type_analysis, evolutionary)
        fields.add("count(distinct(name)) as " + study)
        tables.add("messages m")
        tables.union_update(self.GetSQLReportFrom(metric_filters))
        filters.add("m.is_response_of is null")
        filters.union_update(self.GetSQLReportWhere(metric_filters))        

        #Filtering last part of the query, not used in this case
        #filters = gsub("and\n( )+(d|c|cou|com).name =.*$", "", filters)

        q = self.BuildQuery(period, startdate, enddate, " m.first_date ", fields, tables, filters, evolutionary)
        q = re.sub(r'(d|c|cou|com).name.*and', "", q)

        return q

    # TODO: What's the difference among the following methods
    # and previously defined ones?. We should refactor this to have
    # only one of them.
    def GetTablesCountries (self):
        tables = Set([])
        tables.union_update(self.GetTablesOwnUniqueIds())
        tables.add(self.identities_db + ".countries c")
        tables.add(self.identities_db + ".upeople_countries upc")
      
        return tables

    def GetFiltersCountries (self):
        filters = Set([])
        filters.union_update(self.GetFiltersOwnUniqueIds())
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.country_id = c.id")
        
        return filters

    def GetTablesCompanies (self):
        tables = Set([])
        tables.union_update(self.GetTablesOwnUniqueIds())
        tables.add(self.identities_db + ".companies c")
        tables.add(self.identities_db + ".upeople_companies upc")

        return tables

    def GetFiltersCompanies (self):
        filters = Set([])
        filters.union_update(self.GetFiltersOwnUniqueIds())
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.company_id = c.id")
        filters.add("m.first_date >= upc.init")
        filters.add("m.first_date < upc.end")

        return filters

    def GetTablesDomains (self):
        tables = Set([])
        tables.union_update(self.GetTablesOwnUniqueIds())
        tables.add(self.identities_db + ".domains d")
        tables.add(self.identities_db + ".upeople_domains upd")

        return tables

    def GetFiltersDomains (self) :
        filters = Set([])
        filters.union_update(self.GetFiltersOwnUniqueIds())
        filters.add("pup.upeople_id = upd.upeople_id")
        filters.add("upd.domain_id = d.id")
        filters.add("m.first_date >= upd.init")
        filters.add("m.first_date < upd.end")

        return filters

class SCRQuery(DSQuery):
    """ Specific query builders for source code review source"""

    def GetSQLRepositoriesFrom (self):
        #tables necessaries for repositories
        tables = Set([])
        tables.add("trackers t")

        return tables

    def GetSQLRepositoriesWhere (self, repository):
        #fields necessaries to match info among tables
        filters = Set([])
        filters.add("t.url = '"+ repository + "'")
        filters.add("t.id = i.tracker_id")

        return filters

    def GetTablesOwnUniqueIds (self, table=''):
        tables = Set([])
        tables.add("people_upeople pup")
        if table == "issues":
            tables.add("issues i")
        else:
            #TODO: warning -> changes c is using the same acronym as companies
            tables.add("changes c")

        return tables


    def GetFiltersOwnUniqueIds  (self, table=''):
        filters = Set([])

        if table == "issues":
            filters.add("pup.people_id = i.submitted_by")
        else:
            filters.add("pup.people_id = c.changed_by")

        return filters

    def GetSQLProjectFrom (self):
        # projects are mapped to repositories
        tables = Set([])
        tables.add("trackers t")

        return tables

    def GetSQLProjectWhere (self, project):
        # include all repositories for a project and its subprojects
        filters = Set([])

        repos = """t.url IN (
               SELECT repository_name
               FROM   %s.projects p, %s.project_repositories pr
               WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
                   AND pr.data_source='scr'
        )""" % (self.identities_db, self.identities_db, self.get_subprojects(project))
        filters.add(repos)
        filters.add("t.id = i.tracker_id")

        return filters

    def GetSQLCompaniesFrom (self):
        #tables necessaries for companies
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db+".upeople_companies upc")
        tables.add(self.identities_db+".companies c")

        return tables

    def GetSQLCompaniesWhere (self, company):
        #fields necessaries to match info among tables
        filters = Set([])

        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("i.submitted_on >= upc.init")
        filters.add("i.submitted_on < upc.end")
        filters.add("upc.company_id = c.id")
        if company is not None:
            filters.add("c.name = '"+ company+"'")

        return filters

    def GetSQLCountriesFrom (self):
        #tables necessaries for companies
        tables = Set([])

        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople_countries upc")
        #TODO: warning -> countries is using the same acronym as companies
        tables.add(self.identities_db + ".countries c")

        return tables

    def GetSQLCountriesWhere (self, country):
        #fields necessaries to match info among tables
        filters = Set([])
        
        filters.add("i.submitted_by = pup.people_id")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.country_id = c.id")
        if country is not None:
            filters.add("c.name = '"+country+"'")

        return filters

    def GetSQLPeopleFrom (self):
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople up")

        return tables

    def GetSQLPeopleWhere (self, name, table = "changes"):

        filters = Set([])

        if table == "issues":
            filters.add("i.submitted_by")
        else:
            filters.add("ch.changed_by")

        filters.add(field + " = pup.people_id")
        filters.add("up.id = pup.upeople_id")

        if name is not None: 
            filters.add("up.identifier = " + name)

        return filters

    def GetSQLAutoReviewsFrom(self):
        tables = Set([])

        tables.add("issues i")

        return tables

    def GetSQLAutoReviewsWhere(self, value):
        """ Remove auto reviews

        An auto review is defined as the process of submitting
        the changeset and merging a patch from that changeset into
        the source code.

        Limitations: if there are other developers that upload other
        patchset to the loop, those are not taking into account.
        """

        filters = Set([])

        filters.add("""i.id not in
                       (select distinct(i.id) as issue_id
                       from issues i,
                            changes ch
                       where i.submitted_by = ch.changed_by and
                             i.id = ch.issue_id and
                             ch.field = 'status' and
                             ch.new_value = 'MERGED')
                    """)

        return filters

    ##########
    #Generic functions to obtain FROM and WHERE clauses per type of report
    ##########
    def GetSQLReportFrom (self, type_analysis):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of
        #such analysis

        From = Set([])

        if type_analysis is not None:

            list_analysis = type_analysis[0].split(",")

            for analysis in list_analysis:
                if analysis == 'repository': From.union_update(self.GetSQLRepositoriesFrom())
                elif analysis == 'company': From.union_update(self.GetSQLCompaniesFrom())
                elif analysis == 'country': From.union_update(self.GetSQLCountriesFrom())
                elif analysis == 'project': From.union_update(self.GetSQLProjectFrom())
                elif analysis == 'people2': From.union_update(self.GetSQLPeopleFrom())
                elif analysis == 'autoreviews': From.union_update(self.GetSQLAutoReviewsFrom())
                else: raise Exception( analysis + " not supported in From")

        return From

    def GetSQLReportWhere (self, type_analysis):
        #generic function to generate 'where' clauses

        #"type" is a list of two values: type of analysis and value of
        #such analysis

        where = Set([])

        if type_analysis is not None and len(type_analysis)>1:
            analysis = type_analysis[0]
            values = type_analysis[1]

            if values is not None:
                if type(values) is str:
                    # To be improved... not a very smart way of doing this...
                    list_values = values.split(",")
                elif type(values) is list:
                    # On item or list of lists. Unify to list of lists
                    list_values = values
                    if list_values[0] is not list:
                        list_values = [list_values]
            else:
                list_values = None

            list_analysis = type_analysis[0].split(",")

            pos = 0
            for analysis in list_analysis:
                if list_values is not None:
                    value = list_values[pos]
                else:
                    value = None


                if analysis == 'repository': where.union_update(self.GetSQLRepositoriesWhere(value))
                elif analysis == 'company': where.union_update(self.GetSQLCompaniesWhere(value))
                elif analysis == 'country': where.union_update(self.GetSQLCountriesWhere(value))
                elif analysis == 'people2': where.union_update(self.GetSQLPeopleWhere(value, "issues"))
                elif analysis == 'autoreviews': where.union_update(self.GetSQLAutoReviewsWhere(value))
                elif analysis == 'project':
                    if (self.identities_db is None):
                        logging.error("project filter not supported without identities_db")
                        sys.exit(0)
                    else:
                        where.union_update(self.GetSQLProjectWhere(value))
        return where

    def GetReviewsSQL (self, period, startdate, enddate, type_, type_analysis, evolutionary):
        #Building the query
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(i.issue)) as " + type_)

        tables.add("issues i")
        tables.add("issues_ext_gerrit ie")
        tables.union_update(self.GetSQLReportFrom(type_analysis))

        if type_ == "submitted": filters = Set([])
        elif type_ == "opened": filters.add("(i.status = 'NEW' or i.status = 'WORKINPROGRESS')")
        elif type_ == "new": filters.add("i.status = 'NEW'")
        elif type_ == "inprogress": filters.add("i.status = 'WORKINGPROGRESS'")
        elif type_ == "closed": filters.add("(i.status = 'MERGED' or i.status = 'ABANDONED')")
        elif type_ == "merged": filters.add("i.status = 'MERGED'")
        elif type_ == "abandoned": filters.add("i.status = 'ABANDONED'")
        filters.union_update(self.GetSQLReportWhere(type_analysis))
        filters.add("i.id = ie.issue_id")

        if (self.GetIssuesFiltered() != ""): filters.union_update(self.GetIssuesFiltered())

        date_field = "i.submitted_on"
        if type_ in ["closed", "merged", "abandoned"]: date_field = "ie.mod_date"
        # Not include reviews before startdate no matter mod_date is after startdate
        filters.add("i.submitted_on >= " + startdate)

        # TODO: query builder should not depend on report_tool
        from vizgrimoire.report import Report
        start_analysis = Report.get_start_date()
        if 'scr_start_date' in Report.get_config()['r']:
            start_analysis = Report.get_config()['r']['scr_start_date']
            filters.add("i.submitted_on >= '" + start_analysis  + "'")

        #Hack to use the Upload date of the first patchset and not the
        #submission date. This is needed given that in some cases the creation
        #date of the changeset is incorrect in Gerrit servers.
        if type_ == "submitted":
            tables.add("changes ch")
            filters.add("ch.issue_id = i.id")
            filters.add("field = 'Upload'") # upload event of a patchset
            filters.add("old_value = 1")  # first patchset
            date_field = "ch.changed_on" # date filter
            filters.remove("i.submitted_on >= " + startdate) # removing noise
            filters.add("ch.changed_on >= " + startdate) # adding correct date filter

        q = self.BuildQuery (period, startdate, enddate, date_field, fields, tables,
                             filters, evolutionary, type_analysis)
        return q

    # Reviews status using changes table
    def GetReviewsChangesSQL (self, period, startdate, enddate, type_, type_analysis, 
                              evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(issue_id) as "+ type_+ "_changes")
        # TODO: warning -> using the same acronym "c" as in companies or countries
        tables.add("changes c")
        tables.add("issues i")
        tables.union_update(self.GetSQLReportFrom(type_analysis))
        filters.add("c.issue_id = i.id")
        filters.add("new_value = '"+type_+"'")
        filters.union_update(self.GetSQLReportWhere(type_analysis))

        q = self.BuildQuery (period, startdate, enddate, "changed_on", fields, tables, filters, evolutionary)

        if (self.GetChangesFiltered() != ""): filters.union_update(self.GetChangesFiltered())

        return q

    def GetEvaluationsSQL (self, period, startdate, enddate, type_, type_analysis, evolutionary):
        # verified - VRIF
        # approved - APRV
        # code review - CRVW
        # submitted - SUBM

        #Building the query
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(c.id)) as " + type_)
        tables.add("changes c")
        tables.add("issues i")
        tables.union_update(self.GetSQLReportFrom(type_analysis))
        if type_ == "verified": filters.add("(c.field = 'VRIF' OR c.field = 'Verified')")
        elif type_ == "approved": filters.add("c.field = 'APRV'")
        elif type_ == "codereview": filters.add("(c.field = 'CRVW' OR c.field = 'Code-Review')")
        elif type_ == "sent": filters.add("c.field = 'SUBM'")
        filters.add("i.id = c.issue_id")
        filters.union_update(self.GetSQLReportWhere(type_analysis))

        q = self.BuildQuery (period, startdate, enddate, "c.changed_on",
                             fields, tables, filters, evolutionary)
        return q

    def GetWaiting4ReviewerSQL (self, period, startdate, enddate, type_analysis, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(c.id)) as WaitingForReviewer")
        tables.add("changes c")
        tables.add("issues i")
        tables.add("""(select c.issue_id as issue_id,
                              c.old_value as old_value,
                              max(c.id) as id
                       from changes c,
                            issues i
                       where c.issue_id = i.id and
                             i.status='NEW'
                       group by c.issue_id, c.old_value) t1""")
        tables.union_update(self.GetSQLReportFrom(type_analysis))

        filters.add("i.id = c.issue_id")
        filters.add("t1.id = c.id")
        filters.add("(c.field='CRVW' or c.field='Code-Review' or c.field='Verified' or c.field='VRIF')")
        filters.add("(c.new_value=1 or c.new_value=2)")
        filters.union_update(self.GetSQLReportWhere(type_analysis))

        q = self.BuildQuery (period, startdate, enddate, "c.changed_on",
                             fields, tables, filters, evolutionary)
        return q

    def GetWaiting4SubmitterSQL (self, period, startdate, enddate, type_analysis, evolutionary):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(c.id)) as WaitingForSubmitter")
        tables.add("changes c")
        tables.add("issues i")
        tables.add("""(select c.issue_id as issue_id,
                              c.old_value as old_value,
                              max(c.id) as id
                       from changes c,
                            issues i
                       where c.issue_id = i.id and
                             i.status='NEW'
                       group by c.issue_id, c.old_value) t1""")
        tables.union_update(self.GetSQLReportFrom(type_analysis))
        filters.add("i.id = c.issue_id")
        filters.add("t1.id = c.id")
        filters.add("(c.field='CRVW' or c.field='Code-Review' or c.field='Verified' or c.field='VRIF')")
        filters.add("(c.new_value=-1 or c.new_value=-2)")
        filters.union_update(self.GetSQLReportWhere(type_analysis))

        q = self.BuildQuery (period, startdate, enddate, "c.changed_on",
                             fields, tables, filters, evolutionary)
        return q

    # Real reviews spend >1h, are not autoreviews, and bots are filtered out.
    def GetTimeToReviewQuerySQL (self, startdate, enddate, type_analysis = [], bots = []):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        filter_bots = ''
        for bot in bots:
            filters.add("people.name <> '"+bot+"'")

        # Subquery to get the time to review for all reviews
        # Initially we can not trust the i.submitted_on date. 
        # Thus, we're retrieving the first Upload patch date that should
        # be close to the actual changeset submission
        fields.add("TIMESTAMPDIFF(SECOND, ch_ext.changed_on, ch.changed_on)/(24*3600) AS revtime")
        fields.add("ch.changed_on")
        
        tables.add("issues i")
        tables.add("changes ch")
        tables.add("changes ch_ext, people")
        tables.union_update(self.GetSQLReportFrom(type_analysis))

        filters.add("i.id = ch.issue_id")
        filters.add("people.id = ch.changed_by")
        filters.add("i.id = ch_ext.issue_id")
        filters.add("ch_ext.field = 'Upload'")
        filters.add("ch_ext.old_value = 1")
        filters.union_update(self.GetSQLReportWhere(type_analysis))
        filters.add("ch.field = 'status'")
        filters.add("ch.new_value='MERGED'")
        filters.add("summary not like '%WIP%'")
        # remove autoreviews
        filters.add("i.submitted_by<>ch.changed_by")
        #filters.add("ORDER BY ch_ext.changed_on")

        fields_str = self._get_fields_query(fields)
        tables_str = self._get_tables_query(tables)
        filters_str = self._get_filters_query(filters)
        filters_str = filters_str + " ORDER BY ch_ext.changed_on"
        q = self.GetSQLGlobal('ch.changed_on', fields_str, tables_str, filters_str,
                        startdate, enddate)
        # min_days_for_review = 0.042 # one hour
        # q = "SELECT revtime, changed_on FROM ("+q+") qrevs WHERE revtime>"+str(min_days_for_review)
        return q

    # Time to review accumulated for pending submissions using submit date or update date
    def GetTimeToReviewPendingQuerySQL (self, startdate, enddate, identities_db = None,
                                        type_analysis = [], bots = [],
                                        reviewers = False, uploaded = False):

        filter_bots = ''
        for bot in bots:
            filter_bots = filter_bots + " people.name<>'"+bot+"' AND "

        sql_max_patchset = self.get_sql_max_patchset_for_reviews()
        sql_reviews_reviewed = self.get_sql_reviews_reviewed(startdate)

        fields = "TIMESTAMPDIFF(SECOND, submitted_on, NOW())/(24*3600) AS revtime, submitted_on "
        if (uploaded):
            fields = "TIMESTAMPDIFF(SECOND, ch.changed_on, NOW())/(24*3600) AS revtime, i.submitted_on as submitted_on "
        tables = "issues i, people, issues_ext_gerrit ie "
        if (uploaded): tables += " , changes ch, ("+sql_max_patchset+") last_patch "
        tables += self._get_tables_query(self.GetSQLReportFrom(type_analysis))
        filters = filter_bots + " people.id = i.submitted_by "
        filters += self._get_filters_query(self.GetSQLReportWhere(type_analysis))
        filters += " AND status<>'MERGED' AND status<>'ABANDONED' "
        filters += " AND ie.issue_id  = i.id "
        if (uploaded):
            filters += " AND ch.issue_id  = i.id AND i.id = last_patch.issue_id "
            filters += " AND ch.old_value = last_patch.maxPatchset  AND ch.field = 'Upload'"
        if reviewers:
                filters += " AND i.summary not like '%WIP%' "
                filters += """ AND i.id NOT IN (%s)
                """ % (sql_reviews_reviewed)

        if (self.GetIssuesFiltered() != ""): filters += " AND " + self.GetIssuesFiltered()

        # if (uploaded): filters += " GROUP BY comm.issue_id "
        filters += " ORDER BY  i.submitted_on"
        q = self.GetSQLGlobal('i.submitted_on', fields, tables, filters,
                              startdate, enddate)

        return(q)

    def get_sql_last_change_for_reviews(self, before = None):
        # last changes for reviews
        # if before specified, just for changes before this date
        before_sql = ""
        if before:
            before_sql = "AND changed_on <= '"+before+"'"

        q_last_change = """
            SELECT c.issue_id as issue_id,  max(c.id) as id
            FROM changes c, issues i
            WHERE c.issue_id = i.id and field<>'status' %s
            GROUP BY c.issue_id
        """ % (before_sql)
        return q_last_change

    def get_sql_max_patchset_for_reviews(self, date_analysis = None):
        # SQL for getting the number of the last patchset sent to a review
        # If date_analysis is specified, only analyze changes before this date

        before_sql = ""
        if date_analysis:
            before_sql = "AND changed_on <= '"+date_analysis+"'"

        sql_max_patchset = """
            SELECT issue_id, max(CAST(old_value as UNSIGNED)) maxPatchset
            FROM changes
            WHERE old_value<>'' and old_value<>'None' %s
            group by issue_id
        """ % (before_sql)

        return sql_max_patchset

    def get_sql_reviews_reviewed(self, startdate, date_analysis = None):
        # SQL for getting the list of reviews already reviewed

        sql_max_patchset = self.get_sql_max_patchset_for_reviews(date_analysis)

        sql_reviews_reviewed = """
           SELECT i.id from issues i, changes ch, (%s) t
           WHERE  i.id = t.issue_id and ch.issue_id = i.id
             AND ch.old_value = t.maxPatchset
             AND (    (field = 'Code-Review' AND (new_value = -1 or new_value = -2))
                 OR  (field = 'Verified' AND (new_value = -1 or new_value = -2))
             )
             AND i.submitted_on >= %s
        """ % (sql_max_patchset, startdate)

        return sql_reviews_reviewed

    def get_sql_reviews_closed (self, startdate, date_analysis):
        # closed date is the mod_date for merged and abandoned reviews

        sql_reviews_closed = """
            SELECT i.id as closed FROM issues i, issues_ext_gerrit ie
            WHERE submitted_on >= %s AND i.id = ie.issue_id
            AND (status='MERGED' OR status='ABANDONED')
            AND mod_date <= '%s'
        """ % (startdate, date_analysis)

        return sql_reviews_closed

    def GetPeopleQuerySubmissions (self, developer_id, period, startdate, enddate, evol):
        fields = "COUNT(i.id) AS submissions"
        tables = self.GetTablesOwnUniqueIds('issues')
        filters = self.GetFiltersOwnUniqueIds('issues')+ " AND pup.upeople_id = "+ str(developer_id)

        if (evol):
            q = self.GetSQLPeriod(period,'submitted_on', fields, tables, filters,
                    startdate, enddate)
        else:
            fields = fields + \
                    ",DATE_FORMAT (min(submitted_on),'%Y-%m-%d') as first_date, "+\
                    "  DATE_FORMAT (max(submitted_on),'%Y-%m-%d') as last_date"
            q = self.GetSQLGlobal('submitted_on', fields, tables, filters,
                    startdate, enddate)
        return (q)

    def GetPeopleEvolSubmissionsSCR (self, developer_id, period, startdate, enddate):
        q = self.GetPeopleQuerySubmissions(developer_id, period, startdate, enddate, True)
        return(self.ExecuteQuery(q))

    def GetPeopleStaticSubmissionsSCR (self, developer_id, startdate, enddate):
        q = self.GetPeopleQuerySubmissions(developer_id, None, startdate, enddate, False)
        return(self.ExecuteQuery(q))

    def GetPeopleIntake(self, min, max):
        filters = self.GetIssuesFiltered()
        if (filters != ""): filters  = " WHERE " + filters
        filters = ""

        q_people_num_submissions_evol = """
            SELECT COUNT(*) AS total, submitted_by,
                YEAR(submitted_on) as year, MONTH(submitted_on) as monthid
            FROM issues
            %s
            GROUP BY submitted_by, year, monthid
            HAVING total > %i AND total <= %i
            ORDER BY submitted_on DESC
            """ % (filters, min, max)

        q_people_num_evol = """
            SELECT COUNT(*) as people, year*12+monthid AS month
            FROM (%s) t
            GROUP BY year, monthid
            """ % (q_people_num_submissions_evol)

        return self.ExecuteQuery(q_people_num_evol)

    # No use of generic query because changes table is not used
    def GetCompaniesQuarters (self, year, quarter, limit = 25):
        filters = self.GetIssuesFiltered()
        if (filters != ""): filters  += " AND "
        filters = ""
        q = """
            SELECT COUNT(i.id) AS total, c.name, c.id, QUARTER(submitted_on) as quarter, YEAR(submitted_on) year
            FROM issues i, people p , people_upeople pup, %s.upeople_companies upc,%s.companies c
            WHERE %s i.submitted_by=p.id AND pup.people_id=p.id
                AND pup.upeople_id = upc.upeople_id AND upc.company_id = c.id
                AND status='merged'
                AND QUARTER(submitted_on) = %s AND YEAR(submitted_on) = %s
              GROUP BY year, quarter, c.id
              ORDER BY year, quarter, total DESC, c.name
              LIMIT %s
            """ % (self.identities_db, self.identities_db, filters,  quarter, year, limit)

        return (self.ExecuteQuery(q))


    # PEOPLE
    def GetPeopleQuarters (self, year, quarter, limit = 25, bots = []) :
        filter_bots = ''
        for bot in bots:
            filter_bots = filter_bots + " up.identifier<>'"+bot+"' AND "

        filters = self.GetIssuesFiltered()
        if (filters != ""): filters  = filter_bots + filters + " AND "
        else: filters = filter_bots

        filters = filter_bots

        q = """
            SELECT COUNT(i.id) AS total, p.name, pup.upeople_id as id,
                QUARTER(submitted_on) as quarter, YEAR(submitted_on) year
            FROM issues i, people p , people_upeople pup, %s.upeople up
            WHERE %s i.submitted_by=p.id AND pup.people_id=p.id AND pup.upeople_id = up.id
                AND status='merged'
                AND QUARTER(submitted_on) = %s AND YEAR(submitted_on) = %s
           GROUP BY year, quarter, pup.upeople_id
           ORDER BY year, quarter, total DESC, id
           LIMIT %s
           """ % (self.identities_db, filters, quarter, year, limit)
        return (self.ExecuteQuery(q))

    def GetPeopleList (self, startdate, enddate, bots):

        filter_bots = ""
        for bot in bots:
            filter_bots += " name<>'"+bot+"' and "

        fields = "DISTINCT(pup.upeople_id) as id, count(i.id) as total, name"
        tables = self._get_tables_query(self.GetTablesOwnUniqueIds('issues')) + ", people"
        filters = filter_bots
        filters += self._get_filters_query(self.GetFiltersOwnUniqueIds('issues')) + " and people.id = pup.people_id"
        filters += " GROUP BY id ORDER BY total desc, name"
        q = self.GetSQLGlobal('submitted_on', fields, tables, filters, startdate, enddate)
        return(self.ExecuteQuery(q))

    def GetCompaniesName  (self,startdate, enddate, limit = 0):
        limit_sql=""
        if (limit > 0): limit_sql = " LIMIT " + str(limit)

        q = "SELECT c.id as id, c.name as name, COUNT(DISTINCT(i.id)) AS total "+\
                   "FROM  "+ self.identities_db +".companies c, "+\
                           self.identities_db +".upeople_companies upc, "+\
                    "     people_upeople pup, "+\
                    "     issues i "+\
                   "WHERE i.submitted_by = pup.people_id AND "+\
                   "  upc.upeople_id = pup.upeople_id AND "+\
                   "  c.id = upc.company_id AND "+\
                   "  i.status = 'merged' AND "+\
                   "  i.submitted_on >="+  startdate+ " AND "+\
                   "  i.submitted_on < "+ enddate+ " "+\
                   "GROUP BY c.name "+\
                   "ORDER BY total DESC,name " + limit_sql
        return(self.ExecuteQuery(q))

    # Global filter to remove all results from Wikimedia KPIs from SCR
    def __init__(self, user, password, database, identities_db = None, host="127.0.0.1", port=3306, group=None):
        super(SCRQuery, self).__init__(user, password, database, identities_db, host, port, group)
        # _filter_submitter_id as a static global var to avoid SQL re-execute
        people_userid = 'l10n-bot'
        q = "SELECT id FROM people WHERE user_id = '%s'" % (people_userid)
        self._filter_submitter_id = self.ExecuteQuery(q)['id']

    # To be used for issues table
    def GetIssuesFiltered(self):
        filters = ""
        if self._filter_submitter_id is not None:
            filters = " i.submitted_by <> %s" % (self._filter_submitter_id)
        return filters

    # To be used for changes table
    def GetChangesFiltered(self):
        filters = ""
        if self._filter_submitter_id is not None:
            filters = " changed_by <> %s" % (self._filter_submitter_id)
        return filters

class IRCQuery(DSQuery):

    def GetSQLRepositoriesFrom (self):
        # tables necessary for repositories
        fields = Set([])
        # TODO: channels c should be changed to channels ch
        #       c is typically used for companies table
        fields.add("channels c")

        return fields

    def GetSQLRepositoriesWhere(self, repository):
        # filters necessaries for repositories
        filters = Set([])
        filters.add("i.channel_id = c.id")
        filters.add("c.name = " + repository)

        return filters

    def GetSQLCompaniesFrom(self):
        # tables necessary to companies analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".companies c")
        tables.add(self.identities_db + ".upeople_companies upc")

        return tables

    def GetSQLCompaniesWhere(self, name):
        # filters necessary to companies analysis
        filters = Set([])
        filters.add("i.nick = pup.people_id")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.company_id = c.id")
        filters.add("i.date >= upc.init")
        filters.add("i.date < upc.end")
        filters.add("c.name = " + name)

        return filters

    def GetSQLCountriesFrom(self):
        # tables necessary to countries analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".countries c")
        tables.add(self.identities_db + ".upeople_countries upc")

        return tables

    def GetSQLCountriesWhere(self, name):
        # filters necessary to countries analysis
        filters = Set([])
        filters.add("i.nick = pup.people_id")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.country_id = c.id")
        filters.add("c.name = " + name)

        return filters

    def GetSQLDomainsFrom(self):
        # tables necessary to domains analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".domains d")
        tables.add(self.identities_db + ".upeople_domains upd")

        return tables

    def GetSQLDomainsWhere(self, name):
        # filters necessary to domains analysis
        filters = Set([])
        filters.add("i.nick = pup.people_id")
        filters.add("pup.upeople_id = upd.upeople_id")
        filters.add("upd.domain_id = d.id")
        filters.add("d.name = " + name)

        return filters

    def GetSQLPeople2From(self):
        # tables necessary to countries analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople up")

        return tables

    def GetSQLPeople2Where(self, name):
        # filters necessary to countries analysis
        filters = Set([])
        filters.add("i.nick = pup.people_id")
        filters.add("pup.upeople_id = up.id")

        return filters

    def GetTablesOwnUniqueIds (self):
        tables = Set([])
        tables.add("irclog i")
        tables.add("people_upeople pup")

        return tables

    def GetFiltersOwnUniqueIds (self) :
        filters = Set([])
        filters.add("pup.people_id = i.nick")

        return filters

    def GetSQLReportFrom(self, type_analysis):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of 
        #such analysis

        From = Set([])

        if (type_analysis is None or len(type_analysis) != 2): return From

        analysis = type_analysis[0]

        if analysis == 'repository': From.union_update(self.GetSQLRepositoriesFrom())
        elif analysis == 'company': From.union_update(self.GetSQLCompaniesFrom())
        elif analysis == 'country': From.union_update(self.GetSQLCountriesFrom())
        elif analysis == 'domain': From.union_update(self.GetSQLDomainsFrom())
        elif analysis == 'people2': From.union_update(self.GetSQLPeople2From())

        return From

    def GetSQLReportWhere (self, type_analysis):
        #generic function to generate 'where' clauses
        #"type" is a list of two values: type of analysis and value of 
        #such analysis

        where = Set([])

        if (type_analysis is None or len(type_analysis) != 2): return where

        analysis = type_analysis[0]
        value = type_analysis[1]

        if analysis == 'repository': where.union_update(self.GetSQLRepositoriesWhere(value))
        elif analysis == 'company': where.union_update(self.GetSQLCompaniesWhere(value))
        elif analysis == 'country': where.union_update(self.GetSQLCountriesWhere(value))
        elif analysis == 'domain': where.union_update(self.GetSQLDomainsWhere(value))
        elif analysis == 'people2': where.union_update(self.GetSQLPeople2Where(value))

        return where

class MediawikiQuery(DSQuery):

    def GetSQLPeople2Where(self, name = None):
        # filters necessary to companies analysis
        filters = Set([])

        filters.add("pup.upeople_id = up.id")
        if name is not None:
            filters.add("pup.people_id = " + name)

        return filters

    def GetSQLPeople2From(self):
        # tables necessary to domains analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople up")

        return tables

    def GetSQLReportFrom (self, filters):
        tables = Set([])
        tables.add("wiki_pages_revs")
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".upeople up")

        type_analysis = filters.type_analysis

        if (type_analysis is None or len(type_analysis) != 2):
            return tables

        analysis = type_analysis[0]
        value = type_analysis[1]

        if analysis == 'people2': tables.union_update(self.GetSQLPeople2From())

        return tables

    def GetSQLReportWhere (self, filters):
        from vizgrimoire.Mediawiki import Mediawiki

        where = Set([])
        where.add(self.get_bots_filter_sql(Mediawiki, filters))
        where.add("pup.people_id = wiki_pages_revs.user")
        where.add("pup.upeople_id = up.id")

        type_analysis = filters.type_analysis

        if (type_analysis is None or len(type_analysis) != 2):
            return where

        analysis = type_analysis[0]
        value = type_analysis[1]

        if analysis == 'people2': where.union_update(self.GetSQLPeople2Where(value))

        return where

class QAForumsQuery(DSQuery):
    """ Specific query builders for question and answer platforms """

    def create_indexes(self):
        try:
            q = "create index q_id_a_idx on answers (question_identifier)"
            self.ExecuteQuery(q)
        except Exception:
            pass
            # logging.info("Indexes for QAForums already created")
            # import traceback
            # traceback.print_exc(file=sys.stdout)
        try:
            q = "create index q_id_qt_idx on questionstags (question_identifier)"
            self.ExecuteQuery(q)
        except:
            pass
        try:
            q = "create index tag_id_qt_idx on questionstags (question_identifier)"
            self.ExecuteQuery(q)
        except:
            pass
        try:
            q = "create index tag_idx_t on tags (tag)"
            self.ExecuteQuery(q)
        except:
            pass

    def GetSQLReportFrom(self, type_analysis):
        # generic function to generate "from" clauses
        # type_analysis contains two values: type of analysis (company, country...)
        # and the value itself

        #WARNING: if needed, identities_db is accessed as self.identities_db
        fields = Set([])
        tables = Set([])
        filters = Set([])

        report = ""
        value = ""

        if type_analysis is not None and len(type_analysis) == 2:
            report = type_analysis[0]
            value = type_analysis[1]

        #TODO: repository needs to be change to tag, once this is accepted as new
        #      data source in VizGrimoireJS-lib
        if report == "repository":
            tables.add("tags t")
            tables.add("questionstags qt")

        #rest of reports to be implemented

        return tables

    def GetSQLReportWhere(self, type_analysis, table):
        # generic function to generate "where" clauses
        # type_analysis contains two values: type of analysis (company, country...)
        # and the value itself

        shorttable = str(table[0])

        #WARNING: if needed, identities_db is accessed as self.identities_db
        fields = Set([])
        tables = Set([])
        filters = Set([])

        report = ""
        value = ""

        if type_analysis is not None and len(type_analysis) == 2:
            report = type_analysis[0]
            value = type_analysis[1]

        #TODO: repository needs to be change to tag, once this is accepted as new
        #      data source in VizGrimoireJS-lib
        if report == "repository":
            filters.add(shorttable + ".question_identifier = qt.question_identifier")
            filters.add("qt.tag_id = t.id")
            filters.add("t.tag = " + value)
 
        return filters

    def __get_date_field(self, table_name):
        # the tables of the Sibyl tool are not coherent among them
        #so we have different fields for the date of the different posts
        if (table_name == "questions"):
            return "added_at"
        elif (table_name == "answers"):
            return "submitted_on"
        elif (table_name == "comments"):
            return "submitted_on"
        # FIXME add exceptions here

    def __get_author_field(self, table_name):
        # the tables of the Sibyl tool are not coherent among them
        #so we have different fields for the author ids of the different posts
        if (table_name == "questions"):
            return "author_identifier"
        elif (table_name == "answers"):
            return "user_identifier"
        elif (table_name == "comments"):
            return "user_identifier"
        # FIXME add exceptions here

    def __get_metric_name(self, type_post, suffix):
        metric_str = ""
        if (type_post == "questions"):
            metric_str = "q"
        elif (type_post == "answers"):
            metric_str = "a"
        elif (type_post == "comments"):
            metric_str = "c"
        metric_str += suffix
        #else: raise UnexpectedParameter
        return metric_str

    def get_sent(self, period, startdate, enddate, type_analysis, evolutionary,
                 type_post = "questions"):
        # type_post has to be "comment", "question", "answer"

        fields = Set([])
        tables = Set([])
        filters = Set([])

        date_field = self.__get_date_field(type_post)
        date_field = " " + date_field + " "

        if ( type_post == "questions"):
            fields.add("count(distinct(q.id)) as qsent")
            tables.add("questions q")
            tables.union_update(self.GetSQLReportFrom(type_analysis))
            filters.union_update(self.GetSQLReportWhere(type_analysis, "questions"))
        elif ( type_post == "answers"):
            fields.add("count(distinct(a.id)) as asent")
            tables.add("answers a")
            tables.union_update(self.GetSQLReportFrom(type_analysis))
            filters.union_update(self.GetSQLReportWhere(type_analysis, "answers"))
        else:
            fields.add("count(distinct(c.id)) as csent")
            tables.add("comments c")
            tables.union_update(self.GetSQLReportFrom(type_analysis))
            filters.union_update(self.GetSQLReportWhere(type_analysis, "comments"))

        q = self.BuildQuery(period, startdate, enddate, date_field, fields,
                            tables, filters, evolutionary, type_analysis)

        return q

    def get_senders(self, period, startdate, enddate, type_analysis, evolutionary,
                    type_post = "questions"):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        table_name = type_post
        date_field = self.__get_date_field(table_name)
        author_field = self.__get_author_field(table_name)


        if ( type_post == "questions"):
            fields.add("count(distinct(q.%s)) as qsenders" % (author_field))
            tables.add("questions q")
            tables.union_update(self.GetSQLReportFrom(type_analysis))
            filters.union_update(self.GetSQLReportWhere(type_analysis, "questions"))
        elif ( type_post == "answers"):
            fields.add("count(distinct(a.%s)) as asenders" % (author_field))
            tables.add("answers a")
            tables.union_update(self.GetSQLReportFrom(type_analysis))
            filters.union_update(self.GetSQLReportWhere(type_analysis, "answers"))
        else:
            fields.add("count(distinct(c.%s)) as csenders" % (author_field))
            tables.add("comments c")
            tables.union_update(self.GetSQLReportFrom(type_analysis))
            filters.union_update(self.GetSQLReportWhere(type_analysis, "comments"))


        q = self.BuildQuery(period, startdate, enddate, date_field, fields,
                            tables, filters, evolutionary, type_analysis)

        return q



    def static_num_sent(self, period, startdate, enddate, type_analysis=[],
                        type_post = "questions"):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        table_name = type_post #type_post matches the name of the table
        date_field = self.__get_date_field(table_name)
        prefix_table = table_name[0]

        if type_post == "questions":
            metric_name = "qsent"
        if type_post == "answers":
            metric_name = "asent"
        if type_post == "comments":
            metric_name = "csent"

        fields.add("count(distinct("+prefix_table+".id)) as "+metric_name+", \
        DATE_FORMAT (min(" + prefix_table + "." + date_field + "), '%Y-%m-%d') as first_date, \
        DATE_FORMAT (max(" + prefix_table + "." + date_field + "), '%Y-%m-%d') as last_date")

        tables.add("%s %s" % (table_name, prefix_table))
        tables.union_update(self.GetSQLReportFrom(type_analysis))

        filters.add("%s.%s >= %s AND %s.%s < %s " % (prefix_table, date_field, startdate, prefix_table, date_field, enddate))
        filters.union_update(self.GetSQLReportWhere(type_analysis, type_post))

        query = "select " + fields + " from " + tables + " where " + filters

        return query

    def static_num_senders(self, period, startdate, enddate, type_analysis=[],
                           type_post = "questions"):

        fields = Set([])
        tables = Set([])
        filters = Set([])

        table_name = type_post #type_post matches the name of the table
        date_field = self.__get_date_field(table_name)
        author_field = self.__get_author_field(table_name)
        prefix_table = table_name[0]

        if type_post == "questions":
            metric_name = "qsenders"
        if type_post == "answers":
            metric_name = "asenders"
        if type_post == "comments":
            metric_name = "csenders"

        fields.add("count(distinct(%s.%s)) as %s" % (prefix_table, author_field, metric_name))
        tables.add("%s %s " % (table_name, prefix_table))
        tables.union_update(self.GetSQLReportFrom(type_analysis))
        filters.add("%s.%s >= %s AND %s.%s < %s " % (prefix_table, date_field, startdate, prefix_table, date_field, enddate))
        filters.union_update(self.GetSQLReportWhere(type_analysis, type_post))

        query = "select " + fields + " from " + tables + " where " + filters

        return query

    def __get_date_field(self, table_name):
        # the tables of the Sibyl tool are not coherent among them
        #so we have different fields for the date of the different posts
        if (table_name == "questions"):
            return "added_at"
        elif (table_name == "answers"):
            return "submitted_on"
        elif (table_name == "comments"):
            return "submitted_on"
        # FIXME add exceptions here

    def __get_author_field(self, table_name):
        # the tables of the Sibyl tool are not coherent among them
        #so we have different fields for the author ids of the different posts
        if (table_name == "questions"):
            return "author_identifier"
        elif (table_name == "answers"):
            return "user_identifier"
        elif (table_name == "comments"):
            return "user_identifier"
        # FIXME add exceptions here


    def get_top_senders(self, days, startdate, enddate, limit, type_post):
        # FIXME: neither using unique identities nor filtering bots
        from vizgrimoire.QAForums import QAForums
        table_name = type_post
        date_field = self.__get_date_field(table_name)
        author_field = self.__get_author_field(table_name)
        date_limit = ""
        bots = QAForums.get_bots()

        filter_bots = ''
        for bot in bots:
            filter_bots = filter_bots + " p.username<>'"+bot+"' and "

        if (days != 0):
            sql = "SELECT @maxdate:=max(%s) from %s limit 1" % (date_field, table_name)
            res = self.ExecuteQuery(sql)
            date_limit = " AND DATEDIFF(@maxdate, %s) < %s" % (date_field, str(days))
            #end if

        select = "SELECT %s AS id, p.username AS senders, COUNT(DISTINCT(%s.id)) AS sent" % \
          (author_field, table_name)
        fromtable = " FROM %s, people p" % (table_name)
        filters = " WHERE %s %s = p.identifier AND %s >= %s AND %s < %s " % \
          (filter_bots, author_field, date_field, startdate, date_field, enddate)

        tail = " GROUP BY senders ORDER BY sent DESC, senders LIMIT %s" % (limit)
        q = select + fromtable + filters + date_limit + tail
        return(self.ExecuteQuery(q))

class DownloadsDSQuery(DSQuery):
    """ Specific query builders for downloads """

    pass

class ReleasesDSQuery(DSQuery):
    """ Specific query builders for downloads """
    pass


class PullpoQuery(DSQuery):

    def GetSQLRepositoriesFrom (self):
        # tables necessary for repositories
        fields = Set([])
        fields.add("repositories re")

        return fields

    def GetSQLRepositoriesWhere(self, repository):
        # filters necessaries for repositories
        filters = Set([])
        filters.add("pr.repo_id = re.id")
        filters.add("re.url = " + repository)

        return filters

    def GetSQLCompaniesFrom(self):
        # tables necessary to companies analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".companies c")
        tables.add(self.identities_db + ".upeople_companies upc")

        return tables

    def GetSQLCompaniesWhere(self, name):
        # filters necessary to companies analysis
        filters = Set([])
        filters.add("pr.user_id = pup.people_id")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.company_id = c.id")
        filters.add("pr.created_at >= upc.init")
        filters.add("pr.created_at < upc.end")
        filters.add("c.name = " + name)

        return filters

    def GetSQLCountriesFrom(self):
        # tables necessary to countries analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".countries c")
        tables.add(self.identities_db + ".upeople_countries upc")

        return tables

    def GetSQLCountriesWhere(self, name):
        # filters necessary to countries analysis
        filters = Set([])
        filters.add("pr.user_id = pup.people_id")
        filters.add("pup.upeople_id = upc.upeople_id")
        filters.add("upc.country_id = c.id")
        filters.add("c.name = " + name)

        return filters

    def GetSQLDomainsFrom(self):
        # tables necessary to domains analysis
        tables = Set([])
        tables.add("people_upeople pup")
        tables.add(self.identities_db + ".domains d")
        tables.add(self.identities_db + ".upeople_domains upd")

        return tables

    def GetSQLDomainsWhere(self, name):
        # filters necessary to domains analysis
        filters = Set([])
        filters.add("pr.user_id = pup.people_id")
        filters.add("pup.upeople_id = upd.upeople_id")
        filters.add("upd.domain_id = d.id")
        filters.add("d.name = " + name)

        return filters

    def GetSQLProjectFrom (self):
        # projects are mapped to repositories
        tables = Set([])
        tables.add("repositories re")

        return tables

    def GetSQLProjectWhere (self, project):
        # include all repositories for a project and its subprojects
        filters = Set([])

        repos = """re.url IN (
               SELECT re.name
               FROM   %s.projects p, %s.project_repositories prep
               WHERE  p.project_id = prep.project_id AND p.project_id IN (%s)
                   AND prep.data_source='pullpo'
        )""" % (self.identities_db, self.identities_db, self.get_subprojects(project))
        filters.add(repos)
        filters.add("re.id = pr.repo_id")

        return filters


    def GetSQLReportFrom (self, type_analysis):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of
        #such analysis

        From = Set([])

        if (type_analysis is None or len(type_analysis) != 2): return From

        analysis = type_analysis[0]

        if (analysis):
            if analysis == 'repository': From.union_update(self.GetSQLRepositoriesFrom())
            elif analysis == 'company': From.union_update(self.GetSQLCompaniesFrom())
            elif analysis == 'country': From.union_update(self.GetSQLCountriesFrom())
            elif analysis == 'domain': From.union_update(self.GetSQLDomainsFrom())
            elif analysis == 'project': From.union_update(self.GetSQLProjectFrom())
        return From

    def GetSQLReportWhere (self, type_analysis):
        #generic function to generate 'where' clauses

        #"type" is a list of two values: type of analysis and value of
        #such analysis

        where = Set([])
        if (type_analysis is None or len(type_analysis) != 2): return where

        analysis = type_analysis[0]
        value = type_analysis[1]

        if (analysis):
            if analysis == 'repository': where.union_update(self.GetSQLRepositoriesWhere(value))
            elif analysis == 'company': where.union_update(self.GetSQLCompaniesWhere(value))
            elif analysis == 'country': where.union_update(self.GetSQLCountriesWhere(value))
            elif analysis == 'domain': where.union_update(self.GetSQLDomainsWhere(value))
            elif analysis == 'project':
                if (self.identities_db is None):
                    logging.error("project filter not supported without identities_db")
                    sys.exit(0)
                else:
                    where.union_update(self.GetSQLProjectWhere(value))
        return where

    def GetReviewsSQL (self, period, startdate, enddate, type_, type_analysis, evolutionary):
        #Building the query
        fields = Set([])
        tables = Set([])
        filters = Set([])

        fields.add("count(distinct(pr.github_id)) as " + type_)

        tables.add("pull_requests pr")
        tables.union_update(self.GetSQLReportFrom(type_analysis))

        if type_ == "submitted": filters = Set([])
        elif type_ == "new": filters.add("pr.state = 'open'")
        elif type_ == "closed": filters.add("pr.state = 'closed'")
        elif type_ == "merged": filters.add("pr.state = 'closed' and merged_at is not NULL")
        elif type_ == "abandoned": filters.add("pr.state = 'closed' and merged_at is NULL")
        filters.union_update(self.GetSQLReportWhere(type_analysis))

        date_field = "pr.created_at"
        if type_ in ["closed", "merged", "abandoned"]: date_field = "pr.updated_at"
        # Not include reviews before startdate no matter mod_date is after startdate
        filters.add("pr.created_at >= " + startdate)

        q = self.BuildQuery (period, startdate, enddate, date_field, fields, tables,
                             filters, evolutionary, type_analysis)
        return q

    def GetTimeToReviewQuerySQL (self, startdate, enddate, type_analysis = [], bots = []):
        fields = Set([])
        tables = Set([])
        filters = Set([])

        filter_bots = ''
        for bot in bots:
            filters.add("people.login <> '"+bot+"'")

        fields.add("TIMESTAMPDIFF(SECOND, created_at, closed_at)/(24*3600) AS revtime")

        tables.add("pull_requests pr")
        tables.union_update(self.GetSQLReportFrom(type_analysis))

        filters.union_update(self.GetSQLReportWhere(type_analysis))
        # filters.add("merged_at is NOT NULL")
        # remove autoreviews
        # filters.add("i.submitted_by<>ch.changed_by")
        #filters.add("ORDER BY ch_ext.changed_on")

        fields_str = self._get_fields_query(fields)
        tables_str = self._get_tables_query(tables)
        filters_str = self._get_filters_query(filters)
        q = self.GetSQLGlobal('closed_at', fields_str, tables_str, filters_str,
                              startdate, enddate)
        # min_days_for_review = 0.042 # one hour
        # q = "SELECT revtime, changed_on FROM ("+q+") qrevs WHERE revtime>"+str(min_days_for_review)
        return q


