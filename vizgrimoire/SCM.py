## Copyright (C) 2012, 2013 Bitergia
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
##
## Queries for SCM data analysis
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>


import os, logging

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod
# TODO integrate: from GrimoireSQL import  GetSQLReportFrom 
from GrimoireSQL import GetSQLReportWhere, ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds
from GrimoireUtils import createJSON, getPeriod, get_subprojects
from data_source import DataSource
from filter import Filter
from metrics_filter import MetricFilters
from query_builder import DSQuery

class SCM(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_cvsanaly"

    @staticmethod
    def get_name():
        return "scm"

    @staticmethod
    def get_date_init(startdate, enddate, identities_db = None, type_analysis = None):
        fields = "DATE_FORMAT (min(s.date), '%Y-%m-%d') as first_date"
        tables = "scmlog s"
        filters = ""
        q = GetSQLGlobal('s.date',fields, tables, filters, startdate, enddate)
        return ExecuteQuery(q)

    @staticmethod
    def get_date_end(startdate, enddate, identities_db = None, type_analysis = None):
        fields = "DATE_FORMAT (max(s.date), '%Y-%m-%d') as last_date"
        tables = "scmlog s"
        filters = ""
        q = GetSQLGlobal('s.date',fields, tables, filters, startdate, enddate)
        return ExecuteQuery(q)

    @staticmethod
    def get_url():
        return StaticURL()


    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        type_analysis = None
        if filter_ is not None:
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]
        evol_data = GetSCMEvolutionaryData(period, startdate, enddate, 
                                           identities_db, type_analysis)

        return evol_data

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  SCM.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = SCM().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_= None):


        if (filter_ is None):
            agg  = GetSCMStaticData(period, startdate, enddate, identities_db, filter_)

            static_url = SCM.get_url()
            agg = dict(agg.items() + static_url.items())
        else:
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]

            data = GetSCMStaticData(period, startdate, enddate, identities_db, type_analysis)
            agg = data

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_= None):
        data = SCM.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = SCM().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_project_top_companies (project, startdate, enddate, limit):
        # First we need to locate the SCM repositories
        from report import Report
        db = Report._automator['generic']['db_cvsanaly']
        db_i = Report._automator['generic']['db_identities']
        dbuser = Report._automator['generic']['db_user']
        dbpass = Report._automator['generic']['db_password']

        dbcon = SCM.get_query_builder()(dbuser, dbpass, db, db_i)
        top_companies = dbcon.get_project_top_companies (project, startdate, enddate, limit)
        return top_companies

    @staticmethod
    def get_project_top_authors (project, startdate, enddate, npeople):
        # First we need to locate the SCM repositories
        from report import Report
        db = Report._automator['generic']['db_cvsanaly']
        db_i = Report._automator['generic']['db_identities']
        dbuser = Report._automator['generic']['db_user']
        dbpass = Report._automator['generic']['db_password']

        dbcon = SCM.get_query_builder()(dbuser, dbpass, db, db_i)
        top_authors = dbcon.get_project_top_authors (project, startdate, enddate, npeople)
        return top_authors


    @staticmethod
    def get_top_data (startdate, enddate, i_db, filter_, npeople):
        top = {}
        bots = SCM.get_bots()

        if filter_ is None:
            top['authors.'] = top_people(0, startdate, enddate, "author" , bots , npeople)
            top['authors.last month']= top_people(31, startdate, enddate, "author", bots, npeople)
            top['authors.last year']= top_people(365, startdate, enddate, "author", bots, npeople)
        elif filter_.get_name() == "company":
            top = company_top_authors("'"+filter_.get_item()+"'", startdate, enddate, npeople)
        elif filter_.get_name() == "project":
            top = SCM.get_project_top_authors(filter_.get_item(), startdate, enddate, npeople)
            top_companies = SCM.get_project_top_companies(filter_.get_item(), startdate, enddate, npeople)
            top = dict(top.items() + top_companies.items())
        else:
            top = None
        return top

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = SCM.get_top_data (startdate, enddate, i_db, None, npeople)
        createJSON (data, destdir+"/"+SCM().get_top_filename())

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            items  = repos_name(startdate, enddate)
        elif (filter_name == "company"):
            items  = companies_name_wo_affs(bots, startdate, enddate)
        elif (filter_name == "country"):
            items = scm_countries_names (identities_db, startdate, enddate)
        elif (filter_name == "domain"):
            items = scm_domains_names (identities_db, startdate, enddate)
        elif (filter_name == "project"):
            items = scm_projects_name (identities_db, startdate, enddate)
        else:
            logging.error(filter_name + " not supported")
        return items

    @staticmethod
    def get_filter_summary(filter_, period, startdate, enddate, identities_db, limit):
        summary = None
        filter_name = filter_.get_name()

        if (filter_name == "company"):
            summary =  GetCommitsSummaryCompanies(period, startdate, enddate, identities_db, limit)
        return summary

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items = SCM.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return
        items = items['name']

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        fn = os.path.join(destdir, filter_.get_filename(SCM()))
        createJSON(items, fn)

        if filter_name in ("domain", "company", "repository"):
            items_list = {'name' : [], 'commits_365' : [], 'authors_365' : []}
        else:
            items_list = items

        for item in items :
            item_name = "'"+ item+ "'"
            logging.info (item_name)
            filter_item = Filter(filter_name, item)

            evol_data = SCM.get_evolutionary_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(SCM()))
            createJSON(evol_data, fn)

            agg = SCM.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(SCM()))
            createJSON(agg, fn)

            if filter_name in ("domain", "company", "repository"):
                items_list['name'].append(item.replace('/', '_'))
                items_list['commits_365'].append(agg['commits_365'])
                items_list['authors_365'].append(agg['authors_365'])

            if filter_name in ("company","project"):
                top_authors = SCM.get_top_data(startdate, enddate, identities_db, filter_item, npeople)
                fn = os.path.join(destdir, filter_item.get_top_filename(SCM()))
                createJSON(top_authors, fn)

        fn = os.path.join(destdir, filter_.get_filename(SCM()))
        createJSON(items_list, fn)

        if (filter_name == "company"):
            summary =  SCM.get_filter_summary(filter_, period, startdate, enddate, identities_db, 10)
            createJSON (summary, destdir+"/"+ filter_.get_summary_filename(SCM))

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        top_authors_data = SCM.get_top_data (startdate, enddate, identities_db, None, npeople)
        top = top_authors_data['authors.']["id"]
        top += top_authors_data['authors.last year']["id"]
        top += top_authors_data['authors.last month']["id"]
        # remove duplicates
        people = list(set(top))

        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        evol_data = GetEvolPeopleSCM(upeople_id, period, startdate, enddate)
        evol_data = completePeriodIds(evol_data, period, startdate, enddate)
        return evol_data

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        agg = GetStaticPeopleSCM(upeople_id,  startdate, enddate)
        return agg

    # Studies implemented in R
    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        unique_ids = True
        # Demographics
        vizr.ReportDemographicsAgingSCM(enddate, destdir, unique_ids)
        vizr.ReportDemographicsBirthSCM(enddate, destdir, unique_ids)

    @staticmethod
    def _remove_people(people_id):
        # Remove from people
        q = "DELETE FROM people_upeople WHERE people_id='%s'" % (people_id)
        ExecuteQuery(q)
        q = "DELETE FROM people WHERE id='%s'" % (people_id)
        ExecuteQuery(q)

    @staticmethod
    def _remove_scmlog(scmlog_id):
        # Get actions and remove mappings
        q = "SELECT * from actions where commit_id='%s'" % (scmlog_id)
        res = ExecuteQuery(q)
        if 'id' in res:
            if not isinstance(res['id'], list): res['id'] = [res['id']]
            for action_id in res['id']:
                # action_files is a view
                # q = "DELETE FROM action_files WHERE action_id='%s'" % (action_id)
                # ExecuteQuery(q)
                q = "DELETE FROM file_copies WHERE action_id='%s'" % (action_id)
                ExecuteQuery(q)
        # actions_file_names is a VIEW
        # q = "DELETE FROM actions_file_names WHERE commit_id='%s'" % (scmlog_id)
        # ExecuteQuery(q)
        q = "DELETE FROM commits_lines WHERE commit_id='%s'" % (scmlog_id)
        ExecuteQuery(q)
        q = "DELETE FROM file_links WHERE commit_id='%s'" % (scmlog_id)
        ExecuteQuery(q)
        q = "SELECT tag_id from tag_revisions WHERE commit_id='%s'" % (scmlog_id)
        res = ExecuteQuery(q)
        for tag_id in res['tag_id']:
            q = "DELETE FROM tags WHERE id='%s'" % (tag_id)
            ExecuteQuery(q)
            q = "DELETE FROM tag_revisions WHERE tag_id='%s'" % (tag_id)
            ExecuteQuery(q)
        q = "DELETE FROM scmlog WHERE id='%s'" % (scmlog_id)
        ExecuteQuery(q)

    @staticmethod
    def remove_filter_data(filter_):
        uri = filter_.get_item()
        logging.info("Removing SCM filter %s %s" % (filter_.get_name(),filter_.get_item()))
        q = "SELECT * from repositories WHERE uri='%s'" % (uri)
        repo = ExecuteQuery(q)
        if 'id' not in repo:
            logging.error("%s not found" % (uri))
            return
        # Remove people
        def get_people_one_repo(field):
            return  """
                SELECT %s FROM (SELECT COUNT(DISTINCT(repository_id)) AS total, %s
                FROM scmlog
                GROUP BY %s
                HAVING total=1) t
                """ % (field, field, field)
        ## Remove committer_id that exists only in this repository
        q = """
            SELECT DISTINCT(committer_id) from scmlog
            WHERE repository_id='%s' AND committer_id in (%s)
        """  % (repo['id'],get_people_one_repo("committer_id"))
        res = ExecuteQuery(q)
        for people_id in res['committer_id']:
            SCM._remove_people(people_id)
        ## Remove author_id that exists only in this repository
        q = """
            SELECT DISTINCT(author_id) from scmlog
            WHERE repository_id='%s' AND author_id in (%s)
        """  % (repo['id'],get_people_one_repo("author_id"))
        res = ExecuteQuery(q)
        for people_id in res['author_id']:
            SCM._remove_people(people_id)
        # Remove people activity
        q = "SELECT id from scmlog WHERE repository_id='%s'" % (repo['id'])
        res = ExecuteQuery(q)
        for scmlog_id in res['id']:
            SCM._remove_scmlog(scmlog_id)
        # Remove files
        q = "SELECT id FROM files WHERE repository_id='%s'" % (repo['id'])
        res = ExecuteQuery(q)
        for file_id in res['id']:
            q = "DELETE FROM file_types WHERE file_id='%s'" % (file_id)
            ExecuteQuery(q)
            q = "DELETE FROM files WHERE id='%s'" % (file_id)
            ExecuteQuery(q)
        # Remove filter
        q = "DELETE from repositories WHERE id='%s'" % (repo['id'])
        ExecuteQuery(q)

    @staticmethod
    def get_query_builder():
        from query_builder import SCMQuery
        return SCMQuery

    @staticmethod
    def get_metrics_core_agg():
        m  = ['commits','authors','committers','branches','files','actions','lines','repositories']
        m += ['avg_commits', 'avg_files', 'avg_commits_author', 'avg_files_author']
        return m

    @staticmethod
    def get_metrics_core_ts():
        m  = ['commits','authors','committers','branches','files','lines','repositories']
        return m

    @staticmethod
    def get_metrics_core_trends():
        return ['commits','authors','files','lines']



##########
# Meta-functions to automatically call metrics functions and merge them
##########

def GetSCMEvolutionaryData (period, startdate, enddate, i_db, type_analysis):
    filter_ = None
    if type_analysis is not None:
        filter_ = Filter(type_analysis[0],type_analysis[1])
    metrics = DataSource.get_metrics_data(SCM, period, startdate, enddate, i_db, filter_, True)
    if filter_ is not None: studies = {}
    else:
        studies = DataSource.get_studies_data(SCM, period, startdate, enddate, True)
    return dict(metrics.items()+studies.items())

def GetSCMStaticData (period, startdate, enddate, i_db, type_analysis):
    filter_ = None
    if type_analysis is not None:
        filter_ = Filter(type_analysis[0],type_analysis[1])
    metrics = DataSource.get_metrics_data(SCM, period, startdate, enddate, i_db, filter_, False)
    if filter_ is not None: studies = {}
    else:
        studies = DataSource.get_studies_data(SCM, period, startdate, enddate, False)
    return dict(metrics.items()+studies.items())

##########
# Specific FROM and WHERE clauses per type of report
##########
def GetSQLRepositoriesFrom ():
    #tables necessaries for repositories
    return (" , repositories r")


def GetSQLRepositoriesWhere (repository):
    #fields necessaries to match info among tables
    return (" and r.name ="+ repository + \
            " and r.id = s.repository_id")

def GetSQLProjectFrom ():
    #tables necessaries for repositories
    return (" , repositories r")


def GetSQLProjectWhere (project, role, identities_db):
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

def GetSQLCompaniesFrom (identities_db):
    #tables necessaries for companies
    return (" , "+identities_db+".people_upeople pup,"+\
                  identities_db+".upeople_companies upc,"+\
                  identities_db+".companies c")


def GetSQLCompaniesWhere (company, role):
    #fields necessaries to match info among tables
    return ("and s."+role+"_id = pup.people_id "+\
            "  and pup.upeople_id = upc.upeople_id "+\
            "  and s.date >= upc.init "+\
            "  and s.date < upc.end "+\
            "  and upc.company_id = c.id "+\
            "  and c.name =" + company)


def GetSQLCountriesFrom (identities_db):
    #tables necessaries for companies
    return (" , "+identities_db+".people_upeople pup, "+\
                  identities_db+".upeople_countries upc, "+\
                  identities_db+".countries c")


def GetSQLCountriesWhere (country, role):
    #fields necessaries to match info among tables
    return ("and s."+role+"_id = pup.people_id "+\
                  "and pup.upeople_id = upc.upeople_id "+\
                  "and upc.country_id = c.id "+\
                  "and c.name ="+ country)


def GetSQLDomainsFrom (identities_db) :
    #tables necessaries for domains
    return (" , "+identities_db+".people_upeople pup, "+\
                identities_db+".upeople_domains upd, "+\
                identities_db+".domains d")


def GetSQLDomainsWhere (domain, role) :
    #fields necessaries to match info among tables
    return ("and s."+role+"_id = pup.people_id "+\
            "and pup.upeople_id = upd.upeople_id "+\
            "and upd.domain_id = d.id "+\
            "and d.name ="+ domain)


##########
#Generic functions to obtain FROM and WHERE clauses per type of report
##########

# TODO: Use a SCM specific name
def GetSQLReportFrom (identities_db, type_analysis):
    #generic function to generate 'from' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    From = ""

    if (type_analysis is None or len(type_analysis) != 2): return From

    analysis = type_analysis[0]
    # value = type_analysis[1]

    if analysis == 'repository': From = GetSQLRepositoriesFrom()
    elif analysis == 'company': From = GetSQLCompaniesFrom(identities_db)
    elif analysis == 'country': From = GetSQLCountriesFrom(identities_db)
    elif analysis == 'domain': From = GetSQLDomainsFrom(identities_db)
    elif analysis == 'project': From = GetSQLProjectFrom()

    return (From)

def GetSQLReportWhere (type_analysis, role, identities_db = None):
    #generic function to generate 'where' clauses

    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    where = ""

    if (type_analysis is None or len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': where = GetSQLRepositoriesWhere(value)
    elif analysis == 'company': where = GetSQLCompaniesWhere(value, role)
    elif analysis == 'country': where = GetSQLCountriesWhere(value, role)
    elif analysis == 'domain': where = GetSQLDomainsWhere(value, role)
    elif analysis == 'project': where = GetSQLProjectWhere(value, role, identities_db)

    return (where)

##----------------------
## Auxiliary functions querying the database
##----------------------

def get_timespan():
    """Get timespan found in the SCM database.
    
    Returns
    -------
    startdate : datetime.datetime
        Time of first commit
    enddate : datetime.datetime
        Time of last commit

    Notes
    -----

    Just looks for first and last date in scmlog.

    """

    q = """SELECT DATE(MIN(date)) as startdate,
                  DATE(MAX(date)) as enddate FROM scmlog"""
    data = ExecuteQuery(q)
    return (data['startdate'], data['enddate'])

#########
#Functions to obtain info per type of basic piece of data
#########

# All of the EvolXXX or StaticXXX contains the same parameters:
#    period:
#    startdate:
#    enddate:
#    identities_db: MySQL database name
#    type_analysis: tuple with two values: typeof and value
#                   typeof = 'companies', 'countries', 'repositories' or ''
#                   value = any value that corresponds with the type of analysis

def GetCommits (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # This function contains basic parts of the query to count commits.
    # That query is built and results returned.

    fields = " count(distinct(s.id)) as commits "
    tables = " scmlog s, actions a " + GetSQLReportFrom(identities_db, type_analysis)
    filters = GetSQLReportWhere(type_analysis, "author", identities_db) + " and s.id=a.commit_id "

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolCommits (period, startdate, enddate, identities_db, type_analysis):
    # Returns the evolution of commits through the time

    return(GetCommits(period, startdate, enddate, identities_db, type_analysis, True))


def StaticURL () :
    # Returns the SCM URL     

    q = "select uri as url,type from repositories limit 1"
    return (ExecuteQuery(q))

#
# People
#

def GetTablesOwnUniqueIdsSCM () :
    return ('scmlog s, people_upeople pup')


def GetFiltersOwnUniqueIdsSCM () :
    return ('pup.people_id = s.author_id') 

def GetPeopleListSCM (startdate, enddate) :
    fields = "DISTINCT(pup.upeople_id) as pid, COUNT(s.id) as total"
    tables = GetTablesOwnUniqueIdsSCM()
    filters = GetFiltersOwnUniqueIdsSCM()
    filters +=" GROUP BY pid ORDER BY total desc, pid"
    q = GetSQLGlobal('s.date',fields,tables, filters, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)

def GetPeopleQuerySCM (developer_id, period, startdate, enddate, evol) :
    fields ='COUNT(s.id) AS commits'
    tables = GetTablesOwnUniqueIdsSCM()
    filters = GetFiltersOwnUniqueIdsSCM()
    filters +=" AND pup.upeople_id="+str(developer_id)
    if (evol) :
        q = GetSQLPeriod(period,'s.date', fields, tables, filters,
                startdate, enddate)
    else :
        fields += ",DATE_FORMAT (min(s.date),'%Y-%m-%d') as first_date, "+\
                  "DATE_FORMAT (max(s.date),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('s.date', fields, tables, filters, 
                startdate, enddate)

    return (q)


def GetEvolPeopleSCM (developer_id, period, startdate, enddate) :
    q = GetPeopleQuerySCM (developer_id, period, startdate, enddate, True)

    data = ExecuteQuery(q)
    return (data)


def GetStaticPeopleSCM (developer_id, startdate, enddate) :
    q = GetPeopleQuerySCM (developer_id, None, startdate, enddate, False)

    data = ExecuteQuery(q)
    return (data)

def GetActiveAuthorsSCM(days, enddate):
    #return unique ids of active authors during "days" day
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT distinct(pup.upeople_id) as active_authors "+\
        "FROM scmlog s, people_upeople pup " +\
        "WHERE pup.people_id = s.author_id and " +\
        "s.date >= (%s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActiveCommittersSCM(days, enddate):
    #return unique ids of active committers during "days" day
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT distinct(pup.upeople_id) as active_committers "+\
         "FROM scmlog s, people_upeople pup " +\
         "WHERE pup.people_id = s.committer_id and " + \
         "s.date >= (%s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActivePeopleSCM(days, enddate):
    #Gets IDs of active people on the repository during last x days
    authors = GetActiveAuthorsSCM(days, enddate)
    committers = GetActiveCommittersSCM(days, enddate)
    people_scm = authors['active_authors'] + committers['active_committers']
    people_scm = list(set(people_scm))
    return(people_scm)

def GetCommunityMembers():
    #Gets IDs of all community members with no filter
    q = "SELECT DISTINCT(id) as members FROM upeople"
    data = ExecuteQuery(q)
    return(data['members'])

def top_people (days, startdate, enddate, role, bots, limit) :
    # This function returns the 10 top people participating in the source code.
    # Dataset can be filtered removing bots.
    # In addition, the number of days allows to limit the study to the last
    # X days specified in that parameter

    filter_bots = ''
    for bot in bots:
        filter_bots = filter_bots + " u.identifier<>'"+bot+"' and "

    dtables = dfilters = ""
    if (days > 0):
        dtables = ", (SELECT MAX(date) as last_date from scmlog) t"
        dfilters = " DATEDIFF (last_date, date) < %s AND " % (days)

    q = "SELECT u.id as id, u.identifier as "+ role+ "s, "+\
        " count(distinct(s.id)) as commits "+\
        " FROM scmlog s, actions a, people_upeople pup, upeople u " + dtables +\
        " WHERE " + filter_bots + dfilters +\
        "s."+ role+ "_id = pup.people_id AND "+\
        " pup.upeople_id = u.id AND" +\
        " s.id = a.commit_id AND " +\
        " s.date >= "+ startdate+ " AND "+\
        " s.date < "+ enddate +\
        " GROUP BY u.identifier "+\
        " ORDER BY commits desc, "+role+"s "+\
        " LIMIT "+ limit
    data = ExecuteQuery(q)
    for id in data:
        if not isinstance(data[id], (list)): data[id] = [data[id]]
    return (data)	

def top_files_modified () :
    # Top 10 modified files

    #FIXME: to be updated to use stardate and enddate values
    q = "select file_name, count(commit_id) as modifications "+\
        "from action_files a join files f on a.file_id = f.id  "+\
        "where action_type='M'  "+\
        "group by f.id  "+\
        "order by modifications desc limit 10; "	
    data = ExecuteQuery(q)
    return (data)	

def top_authors_year (year, limit) :
    # Given a year, this functions provides the top 10 authors 
    # of such year
    q = "SELECT u.id as id, u.identifier as authors, "+\
        "       count(distinct(s.id)) as commits "+\
        "FROM scmlog s, actions a, "+\
        "     people_upeople pup, "+\
        "     upeople u "+\
        "where s.id = a.commit_id and " +\
        "      s.author_id = pup.people_id and "+\
        "      pup.upeople_id = u.id and "+\
        "      year(s.date) = "+year+" "+\
        "group by u.identifier "+\
        "order by commits desc "+\
        "LIMIT " + limit

    data = ExecuteQuery(q)
    return (data)


def people () :
    # List of people participating in the source code development
 
    q = "select id,identifier from upeople"

    data = ExecuteQuery(q)
    return (data);

def companies_name_wo_affs (affs_list, startdate, enddate) :
    #List of companies without certain affiliations
    affiliations = ""
    for aff in affs_list:
        affiliations += " c.name<>'"+aff+"' and "

    q_old = "select c.name "+\
               "  from companies c, "+\
               "       people_upeople pup, "+\
               "       upeople_companies upc, "+\
               "       scmlog s,  "+\
               "       actions a "+\
               "  where c.id = upc.company_id and "+\
               "        upc.upeople_id = pup.upeople_id and "+\
               "        s.date >= upc.init and "+\
               "        s.date < upc.end and "+\
               "        pup.people_id = s.author_id and "+\
               "        s.id = a.commit_id and "+\
               "        "+affiliations+"  "+\
               "        s.date >="+ startdate+ " and "+\
               "        s.date < "+ enddate+ " "+\
               "  group by c.name "+\
               "  order by count(distinct(s.id)) desc"


    q = """
        select c.name, count(distinct(t.s_id)) as total
        from companies c,  (
          select distinct(s.id) as s_id, company_id
          from companies c, people_upeople pup, upeople_companies upc,
               scmlog s,  actions a
          where c.id = upc.company_id and  upc.upeople_id = pup.upeople_id
            and  s.date >= upc.init and s.date < upc.end
            and pup.people_id = s.author_id
            and s.id = a.commit_id and
            %s s.date >=%s and s.date < %s) t
        where c.id = t.company_id
        group by c.name
        order by count(distinct(t.s_id)) desc
    """ % (affiliations, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)


def companies_name (startdate, enddate) :
    # companies_limit = 30

    q = "select c.name "+\
         "from companies c, "+\
         "     people_upeople pup, "+\
         "     upeople_companies upc, "+\
         "     scmlog s,  "+\
         "     actions a "+\
         "where c.id = upc.company_id and "+\
         "      upc.upeople_id = pup.upeople_id and "+\
         "      pup.people_id = s.author_id and "+\
         "      s.id = a.commit_id and "+\
         "      s.date >="+ startdate+ " and "+\
         "      s.date < "+ enddate+ " "+\
         "group by c.name "+\
         "order by count(distinct(s.id)) desc"
        # order by count(distinct(s.id)) desc LIMIT ", companies_limit

    data = ExecuteQuery(q)	
    return (data)

def company_top_authors (company_name, startdate, enddate, limit) :
    # Returns top ten authors per company

    q1 = "select u.id as id, u.identifier  as authors, "+\
        "       count(distinct(s.id)) as commits "+\
        " from people p, "+\
        "      scmlog s, "+\
        "      actions a,  "+\
        "      people_upeople pup, "+\
        "      upeople u, "+\
        "      upeople_companies upc, "+\
        "      companies c "+\
        " where  s.id = a.commit_id and "+\
        "        p.id = s.author_id and  "+\
        "        s.author_id = pup.people_id and "+\
        "        pup.upeople_id = upc.upeople_id and "+\
        "        pup.upeople_id = u.id and "+\
        "        s.date >= upc.init and  "+\
        "        s.date < upc.end and "+\
        "        upc.company_id = c.id and "+\
        "        s.date >="+ startdate+ " and "+\
        "        s.date < "+ enddate+ " and "+\
        "        c.name ="+ company_name+ " "+\
        "group by u.id "+\
        "order by count(distinct(s.id)) desc "+\
        "limit " + limit

    q = """
        SELECT id, authors, count(logid) AS commits FROM (
        SELECT DISTINCT u.id AS id, u.identifier AS authors, s.id as logid
        FROM people p,  scmlog s,  actions a, people_upeople pup, upeople u,
             upeople_companies upc,  companies c
        WHERE  s.id = a.commit_id AND p.id = s.author_id AND s.author_id = pup.people_id  AND
          pup.upeople_id = upc.upeople_id AND pup.upeople_id = u.id AND  s.date >= upc.init AND
          s.date < upc.end AND upc.company_id = c.id AND
          s.date >=%s AND s.date < %s AND c.name =%s) t
        GROUP BY id
        ORDER BY commits DESC
        LIMIT %s
    """ % (startdate, enddate, company_name, limit)

    data = ExecuteQuery(q)
    return (data)

def company_top_authors_year (company_name, year, limit):
    # Top 10 authors per company and in a given year

    q = "select u.id as id, u.identifier as authors, "+\
        "        count(distinct(s.id)) as commits "+\
        " from people p, "+\
        "      scmlog s, "+\
        "      actions a, "+\
        "      people_upeople pup, "+\
        "      upeople u, "+\
        "      upeople_companies upc, "+\
        "      companies c "+\
        " where  p.id = s.author_id and "+\
        "        s.author_id = pup.people_id and "+\
        "        pup.upeople_id = upc.upeople_id and "+\
        "        pup.upeople_id = u.id and "+\
        "        s.id = a.commit_id and "+\
        "        s.date >= upc.init and "+\
        "        s.date < upc.end and "+\
        "        year(s.date)="+str(year)+" and "+\
        "        upc.company_id = c.id and "+\
        "        c.name ="+ company_name+ " "+\
        " group by u.id "+\
        " order by count(distinct(s.id)) desc "+\
        " limit " + limit

    data = ExecuteQuery(q)
    return (data)

def repos_name (startdate, enddate) :
    # List of repositories name

    # This query needs pretty large tmp tables
    q = "select count(distinct(s.id)) as total, "+\
        "        name "+\
        " from actions a, "+\
        "      scmlog s, "+\
        "      repositories r "+\
        " where s.id = a.commit_id and "+\
        "       s.repository_id=r.id and "+\
        "       s.date >"+startdate+ " and "+\
        "       s.date <= "+enddate+ " "+\
        " group by repository_id  "+\
        " order by total desc,name";

    q = """
        select count(distinct(sid)) as total, name  
        from repositories r, (
          select distinct(s.id) as sid, repository_id from actions a, scmlog s
          where s.id = a.commit_id  and s.date >%s and s.date <= %s) t
        WHERE repository_id = r.id
        group by repository_id   
        order by total desc,name
        """ % (startdate, enddate)

    data = ExecuteQuery(q)
    return (data)	




# COUNTRIES support
def scm_countries_names (identities_db, startdate, enddate) :

    countries_limit = 30 
    rol = "author" #committer

    q = "SELECT count(s.id) as commits, c.name as name "+\
        "FROM scmlog s,  "+\
        "     people_upeople pup, "+\
        "     "+identities_db+".countries c, "+\
        "     "+identities_db+".upeople_countries upc "+\
        "WHERE pup.people_id = s."+rol+"_id AND "+\
        "      pup.upeople_id  = upc.upeople_id and "+\
        "      upc.country_id = c.id and "+\
        "      s.date >="+startdate+ " and "+\
        "      s.date < "+enddate+ " "+\
        "group by c.name "+\
        "order by commits desc LIMIT "+ str(countries_limit)

    data = ExecuteQuery(q)	
    return (data)

# Companies / Countries support

def scm_companies_countries_evol (identities_db, company, country, period, startdate, enddate) :

    rol = "author" #committer

    q = "SELECT ((to_days(s.date) - to_days("+startdate+")) div "+str(period)+") as id, "+\
        "count(s.id) AS commits, "+\
        "COUNT(DISTINCT(s."+rol+"_id)) as "+rol+"s "+\
        "FROM scmlog s,  "+\
        "     people_upeople pup, "+\
        "     "+identities_db+".countries ct, "+\
        "     "+identities_db+".upeople_countries upct, "+\
        "     "+identities_db+".companies com, "+\
        "     "+identities_db+".upeople_companies upcom "+\
        "WHERE pup.people_id = s."+rol+"_id AND "+\
        "      pup.upeople_id  = upct.upeople_id and "+\
        "      pup.upeople_id = upcom.upeople_id AND "+\
        "      upcom.company_id = com.id AND "+\
        "      upct.country_id = ct.id and "+\
        "      s.date >="+ startdate+ " and "+\
        "      s.date < "+ enddate+ " and "+\
        "      ct.name = '"+ country+ "' AND "+\
        "      com.name ='"+company+"' "+\
        "GROUP BY ((to_days(s.date) - to_days("+startdate+")) div "+str(period)+")"

    data = ExecuteQuery(q)	
    return (data)


def scm_domains_names (identities_db, startdate, enddate) :

    rol = "author" #committer

    q = "SELECT count(s.id) as commits, d.name as name "+\
        "FROM scmlog s, "+\
        "  people_upeople pup, "+\
        "  "+identities_db+".domains d, "+\
        "  "+identities_db+".upeople_domains upd "+\
        "WHERE pup.people_id = s."+rol+"_id AND "+\
        "  pup.upeople_id  = upd.upeople_id and "+\
        "  upd.domain_id = d.id and "+\
        "  s.date >="+ startdate+ " and "+\
        "  s.date < "+ enddate+ " "+\
        "GROUP BY d.name "+\
        "ORDER BY commits desc"

    data = ExecuteQuery(q)
    return (data)

def scm_projects_name  (identities_db, startdate, enddate, limit = 0):
    # Projects activity needs to include subprojects also
    logging.info ("Getting projects list for SCM")

    # Get all projects list
    q = "SELECT p.id AS name FROM  %s.projects p" % (identities_db)
    projects = ExecuteQuery(q)
    data = []

    # Loop all projects getting reviews
    for project in projects['name']:
        type_analysis = ['project', project]
        period = None
        evol = False
        commits = GetCommits (period, startdate, enddate, identities_db, type_analysis, evol)
        commits = commits['commits']
        if (commits > 0):
            data.append([commits,project])

    # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
    from operator import itemgetter
    data_sort = sorted(data, key=itemgetter(0),reverse=True)
    names = [name[1] for name in data_sort]

    if (limit > 0): names = names[:limit]
    return({"name":names})

##############
# Micro Studies
##############

def GetCommitsSummaryCompanies (period, startdate, enddate, identities_db, num_companies):
    # This function returns the following dataframe structrure
    # unixtime, date, week/month/..., company1, company2, ... company[num_companies -1], others
    # The 3 first fields are used for data and ordering purposes
    # The "companyX" fields are those that provide info about that company
    # The "Others" field is the aggregated value of the rest of the companies
    # Companies above num_companies will be aggregated in Others

    companies  = companies_name_wo_affs(["-Bot", "-Individual", "-Unknown"], startdate, enddate)
    companies = companies['name']

    first_companies = {}
    count = 1
    for company in companies:
        company_name = "'"+company+"'"

        commits = EvolCommits(period, startdate, enddate, identities_db, ["company", company_name])
        commits = completePeriodIds(commits, period, startdate, enddate)
        # Rename field commits to company name
        commits[company] = commits["commits"]
        del commits['commits']

        if (count <= num_companies):
            #Case of companies with entity in the dataset
            first_companies = dict(first_companies.items() + commits.items())
        else :
            #Case of companies that are aggregated in the field Others
            if 'Others' not in first_companies:
                first_companies['Others'] = commits[company]
            else:
                first_companies['Others'] = [a+b for a, b in zip(first_companies['Others'],commits[company])]
        count = count + 1

    #TODO: remove global variables...
    first_companies = completePeriodIds(first_companies, period, startdate, enddate)

    return(first_companies)
