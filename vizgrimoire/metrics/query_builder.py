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

class DSQuery(object):
    """ Generic methods to control access to db """

    def __init__(self, user, password, database, identities_db = None, host="127.0.0.1", port=3306, group=None):
        self.identities_db = identities_db
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.group = group
        self.cursor = self.__SetDBChannel__(user, password, database, host, port, group)

    def GetSQLGlobal(self, date, fields, tables, filters, start, end):
        sql = 'SELECT '+ fields
        sql += ' FROM '+ tables
        sql += ' WHERE '+date+'>='+start+' AND '+date+'<'+end
        reg_and = re.compile("^[ ]*and", re.IGNORECASE)
        if (filters != ""):
            if (reg_and.match (filters.lower())) is not None: sql += " " + filters
            else: sql += ' AND '+filters
        return(sql)

    def GetSQLPeriod(self, period, date, fields, tables, filters, start, end):
        # kind = ['year','month','week','day']
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
            sys.exit(1)
        # sql = paste(sql, 'DATE_FORMAT (',date,', \'%d %b %Y\') AS date, ')
        sql += fields
        sql += ' FROM ' + tables
        sql = sql + ' WHERE '+date+'>='+start+' AND '+date+'<'+end
        reg_and = re.compile("^[ ]*and", re.IGNORECASE)

        if (filters != ""):
            if (reg_and.match (filters.lower())) is not None: sql += " " + filters
            else: sql += ' AND ' + filters

        if (period == 'year'):
            sql += ' GROUP BY YEAR('+date+')'
            sql += ' ORDER BY YEAR('+date+')'
        elif (period == 'month'):
            sql += ' GROUP BY YEAR('+date+'),MONTH('+date+')'
            sql += ' ORDER BY YEAR('+date+'),MONTH('+date+')'
        elif (period == 'week'):
            sql += ' GROUP BY YEARWEEK('+date+','+str(iso_8601_mode)+')'
            sql += ' ORDER BY YEARWEEK('+date+','+str(iso_8601_mode)+')'
        elif (period == 'day'):
            sql += ' GROUP BY YEAR('+date+'),DAYOFYEAR('+date+')'
            sql += ' ORDER BY YEAR('+date+'),DAYOFYEAR('+date+')'
        else:
            logging.error("PERIOD: "+period+" not supported")
            sys.exit(1)
        return(sql)


    def BuildQuery (self, period, startdate, enddate, date_field, fields, tables, filters, evolutionary):
        # Select the way to evolutionary or aggregated dataset
        q = ""

        if (evolutionary):
            q = self.GetSQLPeriod(period, date_field, fields, tables, filters,
                              startdate, enddate)
        else:
            q = self.GetSQLGlobal(date_field, fields, tables, filters,
                              startdate, enddate)

        return(q)

    def __SetDBChannel__ (self, user=None, password=None, database=None,
                      host="127.0.0.1", port=3306, group=None):
        if (group == None):
            db = MySQLdb.connect(user=user, passwd=password,
                                 db=database, host=host, port=port)
        else:
            db = MySQLdb.connect(read_default_group=group, db=database)

        cursor = db.cursor()
        cursor.execute("SET NAMES 'utf8'")
        return cursor

    def ExecuteQuery (self, sql):
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

class SCMQuery(DSQuery):
    """ Specific query builders for source code management system data source """

    def GetSQLRepositoriesFrom (self):
        #tables necessaries for repositories
        return (" , repositories r")

    def GetSQLRepositoriesWhere (self, repository):
        #fields necessaries to match info among tables
        return (" and r.name ="+ repository + \
                " and r.id = s.repository_id")

    def GetSQLProjectFrom (self):
        #tables necessaries for repositories
        return (" , repositories r")

    def GetSQLProjectWhere (self, project, role, identities_db):
        # include all repositories for a project and its subprojects
        # Remove '' from project name
        if (project[0] == "'" and project[-1] == "'"):
            project = project[1:-1]

        repos = """and r.uri IN (
               SELECT repository_name
               FROM   %s.projects p, %s.project_repositories pr
               WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
                     AND pr.data_source='scm'
               )""" % (identities_db, identities_db, get_subprojects(project, identities_db))

        return (repos   + " and r.id = s.repository_id")

    def GetSQLCompaniesFrom (self, identities_db):
        #tables necessaries for companies
        return (" , "+identities_db+".people_upeople pup,"+\
                      identities_db+".upeople_companies upc,"+\
                      identities_db+".companies c")

    def GetSQLCompaniesWhere (self, company, role):
         #fields necessaries to match info among tables
         return ("and s."+role+"_id = pup.people_id "+\
                 "  and pup.upeople_id = upc.upeople_id "+\
                 "  and s.date >= upc.init "+\
                 "  and s.date < upc.end "+\
                 "  and upc.company_id = c.id "+\
                 "  and c.name =" + company)

    def GetSQLCountriesFrom (self, identities_db):
        #tables necessaries for companies
        return (" , "+identities_db+".people_upeople pup, "+\
                      identities_db+".upeople_countries upc, "+\
                      identities_db+".countries c")

    def GetSQLCountriesWhere (self, country, role):
         #fields necessaries to match info among tables
        return ("and s."+role+"_id = pup.people_id "+\
                      "and pup.upeople_id = upc.upeople_id "+\
                      "and upc.country_id = c.id "+\
                      "and c.name ="+ country)

    def GetSQLDomainsFrom (self, identities_db) :
        #tables necessaries for domains
        return (" , "+identities_db+".people_upeople pup, "+\
                    identities_db+".upeople_domains upd, "+\
                    identities_db+".domains d")

    def GetSQLDomainsWhere (self, domain, role) :
        #fields necessaries to match info among tables
        return ("and s."+role+"_id = pup.people_id "+\
                "and pup.upeople_id = upd.upeople_id "+\
                "and upd.domain_id = d.id "+\
                "and d.name ="+ domain)

    def GetSQLReportFrom (self, type_analysis):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of 
        #such analysis

        From = ""

        if (type_analysis is None or len(type_analysis) != 2): return From

        analysis = type_analysis[0]
        # value = type_analysis[1]

        if analysis == 'repository': From = self.GetSQLRepositoriesFrom()
        elif analysis == 'company': From = self.GetSQLCompaniesFrom(self.identities_db)
        elif analysis == 'country': From = self.GetSQLCountriesFrom(self.identities_db)
        elif analysis == 'domain': From = self.GetSQLDomainsFrom(self.identities_db)
        elif analysis == 'project': From = self.GetSQLProjectFrom()

        return (From)

    def GetSQLReportWhere (self, type_analysis, role):
        #generic function to generate 'where' clauses

        #"type" is a list of two values: type of analysis and value of 
        #such analysis

        where = ""

        if (type_analysis is None or len(type_analysis) != 2): return where

        analysis = type_analysis[0]
        value = type_analysis[1]

        if analysis == 'repository': where = self.GetSQLRepositoriesWhere(value)
        elif analysis == 'company': where = self.GetSQLCompaniesWhere(value, role)
        elif analysis == 'country': where = self.GetSQLCountriesWhere(value, role)
        elif analysis == 'domain': where = self.GetSQLDomainsWhere(value, role)
        elif analysis == 'project': where = self.GetSQLProjectWhere(value, role, self.identities_db)

        return (where)

class ITSQuery(DSQuery):
    """ Specific query builders for issue tracking system data source """
    def GetSQLRepositoriesFrom (self):
        # tables necessary for repositories 
        return (", trackers t")

    def GetSQLRepositoriesWhere (self, repository):
        # fields necessary to match info among tables
        return (" i.tracker_id = t.id and t.url = "+repository+" ")

    def GetSQLProjectsFrom (self):
        # tables necessary for repositories
        return (", trackers t")

    def GetSQLProjectsWhere (self, project, identities_db):
        # include all repositories for a project and its subprojects
        # Remove '' from project name
        if (project[0] == "'" and project[-1] == "'"):
            project = project[1:-1]

        repos = """ t.url IN (
               SELECT repository_name
               FROM   %s.projects p, %s.project_repositories pr
               WHERE  p.project_id = pr.project_id AND p.project_id IN (%s)
                   AND pr.data_source='its'
        )""" % (identities_db, identities_db, get_subprojects(project, identities_db))

        return (repos   + " and t.id = i.tracker_id")

    def GetSQLCompaniesFrom (self, i_db):
        # fields necessary for the companies analysis

        return(" , people_upeople pup, "+\
               i_db+".companies c, "+\
               i_db+".upeople_companies upc")

    def GetSQLCompaniesWhere (self, name):
        # filters for the companies analysis
        return(" i.submitted_by = pup.people_id and "+\
               "pup.upeople_id = upc.upeople_id and "+\
               "upc.company_id = c.id and "+\
               "i.submitted_on >= upc.init and "+\
               "i.submitted_on < upc.end and "+\
               "c.name = "+name)

    def GetSQLCountriesFrom (self, i_db):
        # fields necessary for the countries analysis

        return(" , people_upeople pup, "+\
               i_db+".countries c, "+\
               i_db+".upeople_countries upc")

    def GetSQLCountriesWhere (self, name):
        # filters for the countries analysis
        return(" i.submitted_by = pup.people_id and "+\
               "pup.upeople_id = upc.upeople_id and "+\
               "upc.country_id = c.id and "+\
               "c.name = "+name)


    def GetSQLDomainsFrom (self, i_db):
        # fields necessary for the domains analysis

        return(" , people_upeople pup, "+\
               i_db+".domains d, "+\
               i_db+".upeople_domains upd")


    def GetSQLDomainsWhere (self, name):
        # filters for the domains analysis
        return(" i.submitted_by = pup.people_id and "+\
               "pup.upeople_id = upd.upeople_id and "+\
               "upd.domain_id = d.id and "+\
               "d.name = "+name)

    def GetSQLReportFrom (self, identities_db, type_analysis):
        #generic function to generate 'from' clauses
        #"type" is a list of two values: type of analysis and value of 
        #such analysis

        From = ""

        if (type_analysis is None or len(type_analysis) != 2): return From

        analysis = type_analysis[0]
        value = type_analysis[1]

        if analysis == 'repository': From = GetSQLRepositoriesFrom()
        elif analysis == 'company': From = GetSQLCompaniesFrom(identities_db)
        elif analysis == 'country': From = GetSQLCountriesFrom(identities_db)
        elif analysis == 'domain': From = GetSQLDomainsFrom(identities_db)
        elif analysis == 'project': From = GetSQLProjectsFrom()

        return (From)

    def GetSQLReportWhere (self, type_analysis, identities_db = None):
        #generic function to generate 'where' clauses

        #"type" is a list of two values: type of analysis and value of 
        #such analysis
        where = ""

        if (type_analysis is None or len(type_analysis) != 2): return where

        analysis = type_analysis[0]
        value = type_analysis[1]

        if analysis == 'repository': where = GetSQLRepositoriesWhere(value)
        elif analysis == 'company': where = GetSQLCompaniesWhere(value)
        elif analysis == 'country': where = GetSQLCountriesWhere(value)
        elif analysis == 'domain': where = GetSQLDomainsWhere(value)
        elif analysis == 'project': where = GetSQLProjectsWhere(value, identities_db)

        return (where)