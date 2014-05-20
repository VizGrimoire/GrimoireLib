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
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
## AuxiliarySCM.R
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

class SCM(DataSource):

    @staticmethod
    def get_db_name():
        return "db_cvsanaly"

    @staticmethod
    def get_name():
        return "scm"

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        if filter_ is not None:
            type_analysis = [filter_.get_name(), "'"+filter_.get_item()+"'"]
            evol_data = GetSCMEvolutionaryData(period, startdate, enddate, 
                                               identities_db, type_analysis)

        else:
            data = GetSCMEvolutionaryData(period, startdate, enddate, identities_db, None)
            evol_data = completePeriodIds(data, period, startdate, enddate)

            data = EvolCompanies(period, startdate, enddate)
            evol_data = dict(evol_data.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolCountries(period, startdate, enddate)
            evol_data = dict(evol_data.items() + completePeriodIds(data, period, startdate, enddate).items())
            data = EvolDomains(period, startdate, enddate)
            evol_data = dict(evol_data.items() + completePeriodIds(data, period, startdate, enddate).items())

        return evol_data

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  SCM.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = SCM().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_trends (period, enddate, identities_db, filter_= None):
        trends = {}
        type_analysis = None
        if filter_ is not None: type_analysis = filter_.get_type_analysis()
        # Tendencies
        for i in [7,30,365]:
            data = GetDiffCommitsDays(period, enddate, identities_db, i, type_analysis)
            trends = dict(trends.items() + data.items())
            data = GetDiffAuthorsDays(period, enddate, identities_db, i, type_analysis)
            trends = dict(trends.items() + data.items())
            data = GetDiffFilesDays(period, enddate, identities_db, i, type_analysis)
            trends = dict(trends.items() + data.items())
            data = GetDiffLinesDays(period, enddate, identities_db, i, type_analysis)
            trends = dict(trends.items() + data.items())
        return trends


    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_= None):


        if (filter_ is None):
            agg  = GetSCMStaticData(period, startdate, enddate, identities_db, filter_)
            static_url = StaticURL()
            agg = dict(agg.items() + static_url.items())

            data = evol_info_data_companies (startdate, enddate)
            agg = dict(agg.items() + data.items())

            data = evol_info_data_countries (startdate, enddate)
            agg = dict(agg.items() + data.items())

            data = evol_info_data_domains (startdate, enddate)
            agg = dict(agg.items() + data.items())

            data = GetCodeCommunityStructure(period, startdate, enddate, identities_db)
            agg = dict(agg.items() + data.items())

            data = SCM.get_trends (period, enddate, identities_db, filter_= None)

            # Last Activity: to be removed
            for i in [7,14,30,60,90,180,365,730]:
                data = last_activity(i)
                agg = dict(agg.items() + data.items())
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
    def get_top_data (startdate, enddate, i_db, filter_, npeople):
        top = {}

        if filter_ is None:
            top['authors.'] = top_people(0, startdate, enddate, "author" , "" , npeople)
            top['authors.last month']= top_people(31, startdate, enddate, "author", "", npeople)
            top['authors.last year']= top_people(365, startdate, enddate, "author", "", npeople)
        elif filter_.get_name() == "company":
            top = company_top_authors("'"+filter_.get_item()+"'", startdate, enddate, npeople)
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
            # Add tendencies for project filter
            if filter_name in ("project"):
                data = SCM.get_trends (period, enddate, identities_db, filter_item)
                agg = dict(agg.items() + data.items())
            fn = os.path.join(destdir, filter_item.get_static_filename(SCM()))
            createJSON(agg, fn)

            if filter_name in ("domain", "company", "repository"):
                items_list['name'].append(item.replace('/', '_'))
                items_list['commits_365'].append(agg['commits_365'])
                items_list['authors_365'].append(agg['authors_365'])

            if (filter_name == "company"):
                top_authors = SCM.get_top_data(startdate, enddate, identities_db, filter_item, npeople)
                fn = os.path.join(destdir, filter_item.get_top_filename(SCM()))
                createJSON(top_authors, fn)

                # Old code not used. To be removed.
                for i in [2006,2009,2012]:
                    data = company_top_authors_year(item_name, i, npeople)
                    createJSON(data, destdir+"/"+item+"-"+SCM.get_name()+"-top-authors_"+str(i)+".json")

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
    def get_metrics_definition ():
        mdef = {
            "scm_commits" : {
                "divid" : "scm_commits",
                "column" : "commits",
                "name" : "Commits",
                "desc" : "Number of commits (changes to source code), aggregating all branches",
                "envision" : {
                    "y_labels" : "true",
                    "show_markers" : "true"
                }
            },
            "scm_committers" : {
                "divid" : "scm_committers",
                "column" : "committers",
                "name" : "Committers",
                "desc" : "Number of persons committing (merging changes to source code)",
                "action" : "commits",
                "envision" : {
                    "gtype" : "whiskers"
                }
            },
            "scm_authors" : {
                "divid" : "scm_authors",
                "column" : "authors",
                "name" : "Authors",
                "desc" : "Number of persons authoring commits (changes to source code)",
                "action" : "commits",
                "envision" : {
                    "gtype" : "whiskers"
                }
            },
            "scm_branches" : {
                "divid" : "scm_branches",
                "column" : "branches",
                "name" : "Branches",
                "desc" : "Number of active branches"
            },
            "scm_files" : {
                "divid" : "scm_files",
                "column" : "files",
                "name" : "Files",
                "desc" : "Number of files modified by at least one commit"
            },
            "scm_added_lines" : {
                "divid" : "scm_added_lines",
                "column" : "added_lines",
                "name" : "Lines added",
                "desc" : "Addition of lines added in all commits"
            },
            "scm_removed_lines" : {
                "divid" : "scm_removed_lines",
                "column" : "removed_lines",
                "name" : "Lines removed",
                "desc" : "Addition of lines removed in all commits"
            },
            "scm_repositories" : {
                "divid" : "scm_repositories",
                "column" : "repositories",
                "name" : "Repositories",
                "desc" : "Evolution of the number of source code repositories",
                "envision" : {
                    "gtype" : "whiskers"
                }
            },
            "scm_companies" : {
                "divid" : "scm_companies",
                "column" : "companies",
                "name" : "Organizations",
                "desc" : "Number of organizations (companies, etc.) with persons active in changing code"
            },
            "scm_countries" : {
                "divid" : "scm_countries",
                "column" : "countries",
                "name" : "Countries",
                "desc" : "Number of countries with persons active in changing code"
            },
            "scm_domains" : {
                "divid" : "scm_domains",
                "column" : "domains",
                "name" : "Domains",
                "desc" : "Number of distinct domains with persons active in changing code"
            },
            "scm_people" : {
                "divid" : "scm_people",
                "column" : "people",
                "name" : "Persons",
                "desc" : "Number of persons active in changing code (authors, committers)"
            }
        }
        return mdef

##########
# Meta-functions to automatically call metrics functions and merge them
##########


def GetSCMEvolutionaryData (period, startdate, enddate, i_db, type_analysis):
    # Meta function that includes basic evolutionary metrics from the source code
    # management system. Those are merged and returned.

    # 1- Retrieving information
    commits = completePeriodIds(EvolCommits(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)
    authors = completePeriodIds(EvolAuthors(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)
    committers = completePeriodIds(EvolCommitters(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)
    files = completePeriodIds(EvolFiles(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)
    lines = completePeriodIds(EvolLines(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)
    branches = completePeriodIds(EvolBranches(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)
    repositories = completePeriodIds(EvolRepositories(period, startdate, enddate, i_db, type_analysis), period, startdate, enddate)

    # 2- Merging information
    evol = dict(commits.items() + repositories.items() + committers.items())
    evol = dict(evol.items() + authors.items() + files.items())
    evol = dict(evol.items() + lines.items() + branches.items())

    return (evol)


def GetSCMStaticData (period, startdate, enddate, i_db, type_analysis):
    # Meta function that includes basic aggregated metrics from the source code
    # management system. Those are merged and returned.

    # 1- Retrieving information
    commits = StaticNumCommits(period, startdate, enddate, i_db, type_analysis)
    authors = StaticNumAuthors(period, startdate, enddate, i_db, type_analysis)
    committers = StaticNumCommitters(period, startdate, enddate, i_db, type_analysis)
    files = StaticNumFiles(period, startdate, enddate, i_db, type_analysis)
    branches = StaticNumBranches(period, startdate, enddate, i_db, type_analysis)
    repositories = StaticNumRepositories(period, startdate, enddate, i_db, type_analysis)
    actions = StaticNumActions(period, startdate, enddate, i_db, type_analysis)
    lines = StaticNumLines(period, startdate, enddate, i_db, type_analysis)
    avg_commits_period = StaticAvgCommitsPeriod(period, startdate, enddate, i_db, type_analysis)
    avg_files_period = StaticAvgFilesPeriod(period, startdate, enddate, i_db, type_analysis)
    avg_commits_author = StaticAvgCommitsAuthor(period, startdate, enddate, i_db, type_analysis)
    # avg_authors_period = StaticAvgAuthorPeriod(period, startdate, enddate, i_db, type_analysis)
    # avg_committer_period = StaticAvgCommitterPeriod(period, startdate, enddate, i_db, type_analysis)
    avg_files_author = StaticAvgFilesAuthor(period, startdate, enddate, i_db, type_analysis)

    # Data from the last 365 days
    fromdate = GetDates(enddate, 365)[1]
    commits_365 = StaticNumCommits(period, fromdate, enddate, i_db, type_analysis)
    authors_365 = StaticNumAuthors(period, fromdate, enddate, i_db, type_analysis)

    # 2- Merging information
    agg = dict(commits.items() + repositories.items() + committers.items())
    agg = dict(agg.items() + authors.items() + files.items() + actions.items())
    agg = dict(agg.items() + lines.items() + branches.items())
    agg = dict(agg.items() + avg_commits_period.items() + avg_files_period.items())
    agg = dict(agg.items() + avg_commits_author.items() + avg_files_author.items())
    agg['commits_365'] = commits_365['commits']
    agg['authors_365'] = authors_365['authors']

    return (agg)

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


#StaticNumCommits (period, startdate, enddate, identities_db, type_analysis):
#    return(GetCommits(period, startdate, enddate, identities_db, type_analysis, False))
#


def GetAuthors (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # This function contains basic parts of the query to count authors
    # That query is later built and executed

    fields = " count(distinct(pup.upeople_id)) AS authors "
    tables = " scmlog s "
    filters = GetSQLReportWhere(type_analysis, "author", identities_db)

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id"

    elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
        #Adding people_upeople table
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id "

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolAuthors (period, startdate, enddate, identities_db, type_analysis):
    # returns the evolution of authors through the time
    return (GetAuthors(period, startdate, enddate, identities_db, type_analysis, True))


def StaticNumAuthors (period, startdate, enddate, identities_db, type_analysis):
    # returns the aggregated number of authors in the specified timeperiod (enddate - startdate)
    return (GetAuthors(period, startdate, enddate, identities_db, type_analysis, False))


def GetDiffAuthorsDays (period, date, identities_db, days, type_analysis = None):
    # This function provides the percentage in activity between two periods:
    chardates = GetDates(date, days)
    last = StaticNumAuthors(period, chardates[1], chardates[0], identities_db, type_analysis)
    last = int(last['authors'])
    prev = StaticNumAuthors(period, chardates[2], chardates[1], identities_db, type_analysis)
    prev = int(prev['authors'])

    data = {}
    data['diff_netauthors_'+str(days)] = last - prev
    data['percentage_authors_'+str(days)] = GetPercentageDiff(prev, last)
    # data['authors_'+str(days)] = last
    return (data)

def GetCommitters (period, startdate, enddate, identities_db, type_analysis, evolutionary) :
    # This function contains basic parts of the query to count committers
    # That query is later built and executed

    fields = 'count(distinct(pup.upeople_id)) AS committers '
    tables = "scmlog s "
    filters = GetSQLReportWhere(type_analysis, "committer", identities_db)

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += " ,  "+identities_db+".people_upeople pup "
        filters += " and s.committer_id = pup.people_id"

    elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
        #Adding people_upeople table
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.committer_id = pup.people_id "

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolCommitters (period, startdate, enddate, identities_db, type_analysis):
    # returns the evolution of the number of committers through the time
    return(GetCommitters(period, startdate, enddate, identities_db, type_analysis, True))


def StaticNumCommitters (period, startdate, enddate, identities_db, type_analysis):
    # returns the aggregate number of committers in the specified timeperiod (enddate - startdate)
    return(GetCommitters(period, startdate, enddate, identities_db, type_analysis, False))



def GetFiles (period, startdate, enddate, identities_db, type_analysis, evolutionary) :
    # This function contains basic parts of the query to count files
    # That query is later built and executed

    fields = " count(distinct(a.file_id)) as files "
    tables = " scmlog s, actions a "
    filters = " a.commit_id = s.id "

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)
    #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #executing the query

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolFiles (period, startdate, enddate, identities_db, type_analysis):
    # returns the evolution of the number of files through the time
    return (GetFiles(period, startdate, enddate, identities_db, type_analysis, True))


def StaticNumFiles (period, startdate, enddate, identities_db, type_analysis):
    # returns the aggregate number of unique files in the specified timeperiod (enddate - startdate)
    return (GetFiles(period, startdate, enddate, identities_db, type_analysis, False))


def GetDiffFilesDays (period, date, identities_db, days, type_analysis = None):
    # This function provides the percentage in activity between two periods:
    chardates = GetDates(date, days)
    last = StaticNumFiles(period, chardates[1], chardates[0], identities_db, type_analysis)
    last = int(last['files'])
    prev = StaticNumFiles(period, chardates[2], chardates[1], identities_db, type_analysis)
    prev = int(prev['files'])

    data = {}
    data['diff_netfiles_'+str(days)] = last - prev
    data['percentage_files_'+str(days)] = GetPercentageDiff(prev, last)
    # data['files_'+str(days)] = last
    return (data)


def GetLines (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # This function contains basic parts of the query to count lines
    # That query is later built and executed

    # basic parts of the query
    fields = "sum(cl.added) as added_lines, sum(cl.removed) as removed_lines"
    tables = "scmlog s, commits_lines cl "
    filters = "cl.commit_id = s.id "

    # specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)
    #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #executing the query
    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    data = ExecuteQuery(q)

    if not (isinstance(data['removed_lines'], list)): data['removed_lines'] = [data['removed_lines']]
    if not (isinstance(data['added_lines'], list)): data['added_lines'] = [data['added_lines']]

    data['removed_lines'] = [float(lines)  for lines in data['removed_lines']]
    data['added_lines'] = [float(lines)  for lines in data['added_lines']]
    # TODO: not used so we don't need it - acs
    data['negative_removed_lines'] = [-float(removed) for removed in data['removed_lines']]
    # data$negative_removed_lines = -data$removed_lines
    return (data)


def EvolLines (period, startdate, enddate, identities_db, type_analysis) :
    # returns the evolution of the number of lines through the time
    return (GetLines(period, startdate, enddate, identities_db, type_analysis, True))


# TODO: two version of this funcion. Unify.
def StaticNumLines2 (period, startdate, enddate, identities_db, type_analysis):
    # returns the aggregate number of lines in the specified timeperiod (enddate - startdate)
    return (GetLines(period, startdate, enddate, identities_db, type_analysis, False))

def GetDiffLinesDays (period, date, identities_db, days, type_analysis = None):
    # This function provides the percentage in activity between two periods:
    chardates = GetDates(date, days)
    last = StaticNumLines(period, chardates[1], chardates[0], identities_db, type_analysis)
    last_added = int(last['added_lines'])
    last_removed = int(last['removed_lines'])
    prev = StaticNumLines(period, chardates[2], chardates[1], identities_db, type_analysis)
    prev_added = int(prev['added_lines'])
    prev_removed = int(prev['removed_lines'])

    data = {}
    data['diff_netadded_lines_'+str(days)] = last_added - prev_added
    data['percentage_added_lines_'+str(days)] = GetPercentageDiff(prev_added, last_added)
    data['diff_netremoved_lines_'+str(days)] = last_removed - prev_removed
    data['percentage_removed_lines_'+str(days)] = GetPercentageDiff(prev_removed, last_removed)

    return (data)


def GetBranches (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # This function contains basic parts of the query to count branches
    # That query is later built and executed

    # basic parts of the query
    fields = "count(distinct(a.branch_id)) as branches "
    tables = " scmlog s, actions a "
    filters = " a.commit_id = s.id "

    # specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)
    #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #executing the query
    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolBranches (period, startdate, enddate, identities_db, type_analysis):
    # returns the evolution of the number of branches through the time
    return (GetBranches(period, startdate, enddate, identities_db, type_analysis, True))


def StaticNumBranches (period, startdate, enddate, identities_db, type_analysis):
    # returns the aggregate number of branches in the specified timeperiod (enddate - startdate)
    return (GetBranches(period, startdate, enddate, identities_db, type_analysis, False))


def GetRepositories (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # This function contains basic parts of the query to count repositories
    # That query is later built and executed

    # basic parts of the query
    fields = "count(distinct(s.repository_id)) AS repositories "
    tables = "scmlog s "

    # specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)
    #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
    filters = GetSQLReportWhere(type_analysis, "author", identities_db)

    #executing the query
    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolRepositories (period, startdate, enddate, identities_db, type_analysis):
    # returns the evolution of the number of repositories through the time
    return (GetRepositories(period, startdate, enddate, identities_db, type_analysis, True))


def StaticNumRepositories (period, startdate, enddate, identities_db, type_analysis):
    # returns the aggregate number of repositories in the specified timeperiod (enddate - startdate)
    return (GetRepositories(period, startdate, enddate, identities_db, type_analysis, False))


def StaticNumCommits (period, startdate, enddate, identities_db, type_analysis) :
    # returns the aggregate number of commits in the specified timeperiod (enddate - startdate)
    # TODO: this function is deprecated, but the new one is not ready yet. This should directly call
    #       GetCommits as similarly done by EvolCommits function.

    #TODO: first_date and last_date should be in another function
    fields = "SELECT count(distinct(s.id)) as commits, "+\
             "DATE_FORMAT (min(s.date), '%Y-%m-%d') as first_date, "+\
             "DATE_FORMAT (max(s.date), '%Y-%m-%d') as last_date "
    tables = " FROM scmlog s, actions a " 
    filters = " where s.date >="+ startdate+ " and "+\
            " s.date < "+ enddate+ " and "+\
            " s.id = a.commit_id "

    # specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)
    #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #executing the query
    q = fields + tables + filters

    return(ExecuteQuery(q))


def GetDiffCommitsDays (period, date, identities_db, days, type_analysis = None):
    # This function provides the percentage in activity between two periods:

    chardates = GetDates(date, days)
    last = StaticNumCommits(period, chardates[1], chardates[0], identities_db, type_analysis)
    last = int(last['commits'])
    prev = StaticNumCommits(period, chardates[2], chardates[1], identities_db, type_analysis)
    prev = int(prev['commits'])

    data = {}
    data['diff_netcommits_'+str(days)] = last - prev
    data['percentage_commits_'+str(days)] = GetPercentageDiff(prev, last)
    # data['commits_'+str(days)] = last
    return (data)


def GetActions (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # This function contains basic parts of the query to count actions.
    # An action is any type of change done in a file (addition, copy, removal, etc)
    # That query is later built and executed

    fields = " count(distinct(a.id)) as actions "
    tables = " scmlog s, actions a "
    filters = " a.commit_id = s.id "

    tables += GetSQLReportFrom(identities_db, type_analysis)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


def EvolActions (period, startdate, enddate, identities_db, type_analysis):
    # returns the evolution of the number of actions through the time
    return(GetActions(period, startdate, enddate, identities_db, type_analysis, True))


def StaticNumActions (period, startdate, enddate, identities_db, type_analysis) :
    # returns the aggregate number of actions in the specified timeperiod (enddate - startdate)
    return(GetActions(period, startdate, enddate, identities_db, type_analysis, False))


def StaticNumLines (period, startdate, enddate, identities_db, type_analysis) :
    # returns the aggregate number of repositories in the specified timeperiod (enddate - startdate)
    # TODO: this function is deprecated, this should call GetLines

    select = "select sum(cl.added) as added_lines, "+\
             "sum(cl.removed) as removed_lines "
    tables = " FROM scmlog s, commits_lines cl "
    filters = " where s.date >="+ startdate+ " and "+\
              " s.date < "+enddate+ " and "+\
              " cl.commit_id = s.id "

    # specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)
    #TODO: left "author" as generic option coming from parameters (this should be specified by command line)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #executing the query
    q = select + tables + filters
    data = ExecuteQuery(q)
    if (data['added_lines'] is None): data['added_lines'] = 0
    if (data['removed_lines'] is None): data['removed_lines'] = 0
    return(data)


def GetAvgCommitsPeriod (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # returns the average number of commits per period of time (day, week, month, etc...) 
    # in the specified timeperiod (enddate - startdate)

    fields = " count(distinct(s.id))/timestampdiff("+period+",min(s.date),max(s.date)) as avg_commits_"+period
    tables = " scmlog s, actions a "
    filters = " s.id = a.commit_id "

    tables += GetSQLReportFrom(identities_db, type_analysis)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


#EvolAvgCommitsPeriod (period, startdate, enddate, identities_db, type_analysis) :
#WARNING: This function should provide same information as EvolCommits, do not use this.
#    return (GetAvgCommitsPeriod(period, startdate, enddate, identities_db, type_analysis, True))
#

def StaticAvgCommitsPeriod (period, startdate, enddate, identities_db, type_analysis) :
    # returns the average number of commits per period (weekly, monthly, etc) in the specified timeperiod (enddate - startdate)
    return (GetAvgCommitsPeriod(period, startdate, enddate, identities_db, type_analysis, False))



def GetAvgFilesPeriod (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # returns the average number of files per period (Weekly, monthly, etc) in the specified
    # time period (enddate - startdate) 
    fields = " count(distinct(a.file_id))/timestampdiff("+period+",min(s.date),max(s.date)) as avg_files_"+period
    tables = " scmlog s, actions a "
    filters = " s.id = a.commit_id "

    tables += GetSQLReportFrom(identities_db, type_analysis)
    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


#EvolAvgFilesPeriod (period, startdate, enddate, identities_db, type_analysis):
#WARNING: this function should return same info as EvolFiles, do not use this
#    return (GetAvgFilesPeriod(period, startdate, enddate, identities_db, type_analysis, True))
#

def StaticAvgFilesPeriod (period, startdate, enddate, identities_db, type_analysis):
    # returns the average number of files per period (Weekly, monthly, etc) in the specified
    # time period (enddate - startdate)
    return (GetAvgFilesPeriod(period, startdate, enddate, identities_db, type_analysis, False))

def GetAvgCommitsAuthor (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # returns the average number of commits per author in the specified
    # time period (enddate - startdate)

    fields = " count(distinct(s.id))/count(distinct(pup.upeople_id)) as avg_commits_author "
    tables = " scmlog s, actions a " 
    filters = " s.id = a.commit_id " 

    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id"

    elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
        #Adding people_upeople table
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id "

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(q))


def EvolAvgCommitsAuthor (period, startdate, enddate, identities_db, type_analysis):
    # returns the average number of commits per author in the specified
    # time period (enddate - startdate)
    return (GetAvgCommitsAuthor(period, startdate, enddate, identities_db, type_analysis, True))


def StaticAvgCommitsAuthor (period, startdate, enddate, identities_db, type_analysis):
    # returns the average and total number of commits per author in the specified
    # time period (enddate - startdate)
    return (GetAvgCommitsAuthor(period, startdate, enddate, identities_db, type_analysis, False))



def GetAvgAuthorPeriod (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # returns the average number of authors per period (weekly, monthly) in the specified
    # time period (enddate - startdate)

    fields = " count(distinct(pup.upeople_id))/timestampdiff("+period+",min(s.date),max(s.date)) as avg_authors_"+period 
    tables = " scmlog s "
    # filters = ""

    filters = GetSQLReportWhere(type_analysis, "author", identities_db)

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id"

    elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
        #Adding people_upeople table
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id "


    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


#EvolAvgAuthorPeriod (period, startdate, enddate, identities_db, type_analysis):
#WARNING: this function should return same information as EvolAuthors, do not use this
#    return (GetAvgAuthorPeriod(period, startdate, enddate, identities_db, type_analysis, True))
#

def StaticAvgAuthorPeriod (period, startdate, enddate, identities_db, type_analysis):
    # returns the average number of authors per period (weekly, monthly) in the specified
    # time period (enddate - startdate)

    return (GetAvgAuthorPeriod(period, startdate, enddate, identities_db, type_analysis, False))

def GetAvgCommitterPeriod (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # returns the average number of committers per period (weekly, monthly) in the specified
    # time period (enddate - startdate)

    fields = " count(distinct(pup.upeople_id))/timestampdiff("+period+",min(s.date),max(s.date)) as avg_authors_"+period
    tables = " scmlog s "
    # filters = ""

    filters = GetSQLReportWhere(type_analysis, "committer", identities_db)

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.committer_id = pup.people_id"

    elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
        #Adding people_upeople table
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.committer_id = pup.people_id "

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)

    return(ExecuteQuery(q))


#EvolAvgCommitterPeriod (period, startdate, enddate, identities_db, type_analysis):
#WARNING: this function should return same info as EvolCommitters, do not use this
#    return (GetAvgCommitterPeriod(period, startdate, enddate, identities_db, type_analysis, True))
#

def StaticAvgCommitterPeriod (period, startdate, enddate, identities_db, type_analysis):
    # returns the average number of committers per period (weekly, monthly) in the specified
    # time period (enddate - startdate)
    return (GetAvgCommitterPeriod(period, startdate, enddate, identities_db, type_analysis, False))



def GetAvgFilesAuthor (period, startdate, enddate, identities_db, type_analysis, evolutionary):
    # returns the average number of files per author (weekly, monthly) in the specified
    # time period (enddate - startdate)    

    fields = " count(distinct(a.file_id))/count(distinct(pup.upeople_id)) as avg_files_author "
    tables = " scmlog s, actions a "
    filters = " s.id = a.commit_id "

    filters += GetSQLReportWhere(type_analysis, "author", identities_db)

    #specific parts of the query depending on the report needed
    tables += GetSQLReportFrom(identities_db, type_analysis)

    if (type_analysis is None or len (type_analysis) != 2) :
        #Specific case for the basic option where people_upeople table is needed
        #and not taken into account in the initial part of the query
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id"

    elif (type_analysis[0] == "repository" or type_analysis[0] == "project"):
        #Adding people_upeople table
        tables += ",  "+identities_db+".people_upeople pup"
        filters += " and s.author_id = pup.people_id "

    q = BuildQuery(period, startdate, enddate, " s.date ", fields, tables, filters, evolutionary)


    return(ExecuteQuery(q))


def EvolAvgFilesAuthor (period, startdate, enddate, identities_db, type_analysis) :
    # returns the average number of files per author and its evolution (weekly, monthly) in the specified
    # time period (enddate - startdate)  
    return(GetAvgFilesAuthor(period, startdate, enddate, identities_db, type_analysis, True))

def StaticAvgFilesAuthor (period, startdate, enddate, identities_db, type_analysis) :
    # returns the average number of files per author (weekly, monthly) in the specified
    # time period (enddate - startdate)  
    return(GetAvgFilesAuthor(period, startdate, enddate, identities_db, type_analysis, False))

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

# 
# Legacy and non legacy code - Cleanup
#

def EvolCompanies (period, startdate, enddate):	
    # Returns the evolution in the provided period of the number of total companies

    fields = "count(distinct(upc.company_id)) as companies"
    tables = " scmlog s, people_upeople pup, upeople_companies upc"
    filters = "s.author_id = pup.people_id and "+\
               "pup.upeople_id = upc.upeople_id and "+\
               "s.date >= upc.init and  "+\
               "s.date < upc.end"
    q = GetSQLPeriod(period,'s.date', fields, tables, filters,
                     startdate, enddate)

    return(ExecuteQuery(q))


def EvolCountries (period, startdate, enddate):	
    # Returns the evolution in the provided period of the number of total countries

    fields = "count(distinct(upc.country_id)) as countries"
    tables = "scmlog s, people_upeople pup, upeople_countries upc"
    filters = "s.author_id = pup.people_id and pup.upeople_id = upc.upeople_id"
    q = GetSQLPeriod(period,'s.date', fields, tables, filters, 
               startdate, enddate)

    countries= ExecuteQuery(q)
    return(countries)


def EvolDomains (period, startdate, enddate):
    # Returns the evolution in the provided period of the number of total domains

    fields = "COUNT(DISTINCT(upd.domain_id)) AS domains"
    tables = "scmlog s, people_upeople pup, upeople_domains upd"
    filters = "s.author_id = pup.people_id and pup.upeople_id = upd.upeople_id"
    q = GetSQLPeriod(period,'s.date', fields, tables, filters,
            startdate, enddate)

    domains= ExecuteQuery(q)
    return(domains)


def last_activity (days) :
    # Given a number of days, this function calculates the number of
    # commits, authors, files, added and removed lines that took place
    # in a project. 

    days = str(days)

    #commits
    q = "select count(*) as commits_"+days+" "+\
        "from scmlog  "+\
        "where date >= ( "+\
        "      select (max(date) - INTERVAL "+days+" day) "+\
        "      from scmlog)";

    data1 = ExecuteQuery(q)

    #authors
    q = "select count(distinct(pup.upeople_id)) as authors_"+days+" "+\
        "from scmlog s, "+\
        "     people_upeople pup "+\
        "where pup.people_id = s.author_id and "+\
        "      s.date >= (select (max(date) - INTERVAL "+days+" day) from scmlog)";

    data2 = ExecuteQuery(q)


    #files
    q = "select count(distinct(a.file_id)) as files_"+days+" "+\
        "from scmlog s, "+\
        "     actions a "+\
        "where a.commit_id = s.id and "+\
        "      s.date >= (select (max(date) - INTERVAL "+days+" day) from scmlog)"

    data3 = ExecuteQuery(q)

    #added_removed lines
    q = " select sum(cl.added) as added_lines_"+days+", "+\
        "        sum(cl.removed) as removed_lines_"+days+" "+\
        " from scmlog s, "+\
        "      commits_lines cl "+\
        " where cl.commit_id = s.id and "+\
        "       s.date >= (select (max(date) - INTERVAL "+days+" day) from scmlog)"

    data4 = ExecuteQuery(q)

    agg_data = dict(data1.items() +  data2.items() + data3.items() +data4.items())

    return (agg_data)

def top_people (days, startdate, enddate, role, filters, limit) :
    # This function returns the 10 top people participating in the source code.
    # Dataset can be filtered by the affiliations, where specific companies
    # can be ignored. This is done by using the argument "filters",
    # which can be "None" (no filetering) or the list of companies to
    # filter out.
    # In addition, the number of days allows to limit the study to the last
    # X days specified in that parameter

    if not filters:
        affiliations_from = ""
        affiliations_where = ""
    else:
        affiliations = ""
        for aff in filters:
            affiliations += " c.name<>'"+aff+"' and "
        affiliations_from = ", upeople_companies upc, companies c "
        affiliations_where = " and u.id = upc.upeople_id and "+\
            " s.date >= upc.init and "+\
            " s.date < upc.end and "+ affiliations+ " "+\
            " upc.company_id = c.id "
 
    date_limit = ""
    if (days != 0 ) :
        ExecuteQuery("SELECT @maxdate:=max(date) from scmlog limit 1")
        date_limit = " AND DATEDIFF(@maxdate, date)<"+str(days)

    q = "SELECT u.id as id, u.identifier as "+ role+ "s, "+\
        "count(distinct(s.id)) as commits "+\
        " FROM scmlog s, "+\
        " people_upeople pup, "+\
        " upeople u "+\
        affiliations_from +\
        " WHERE s."+ role+ "_id = pup.people_id and "+\
        " pup.upeople_id = u.id and "+\
        " s.date >= "+ startdate+ " and "+\
        " s.date < "+ enddate+" "+ date_limit +\
        affiliations_where +\
        " GROUP BY u.identifier "+\
        " ORDER BY commits desc, "+role+"s "+\
        " LIMIT "+ limit

    data = ExecuteQuery(q)
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


## TODO: Follow top_committers implementation
def top_authors (startdate, enddate, limit) :
    # Top 10 authors without filters
    #
    # DEPRECATED use top_people instead
    #

    q = "SELECT u.id as id, u.identifier as authors, "+\
        "       count(distinct(s.id)) as commits "+\
        "FROM scmlog s, "+\
        "     actions a, "+\
        "     people_upeople pup, "+\
        "     upeople u "+\
        "where s.id = a.commit_id and "+\
        "      s.author_id = pup.people_id and "+\
        "      pup.upeople_id = u.id and "+\
        "      s.date >="+ startdate+ " and "+\
        "      s.date < "+ enddate+ " "+\
        "group by u.identifier "+\
        "order by commits desc "+\
        "LIMIT " + limit

    data = ExecuteQuery(q)
    return (data)



def top_authors_wo_affiliations (list_affs, startdate, enddate, limit) :
    # top ten authors with affiliation removal
    #list_affs
    affiliations = ""
    for aff in list_affs:
        affiliations += " c.name<>'"+aff+"' and "

    q = "SELECT u.id as id, u.identifier as authors, "+\
        "       count(distinct(s.id)) as commits "+\
        "FROM scmlog s, "+\
        "     actions a, "+\
        "     people_upeople pup, "+\
        "     upeople u,  "+\
        "     upeople_companies upc, "+\
        "     companies c "+\
        "where s.id = a.commit_id and "+\
        "      s.author_id = pup.people_id and "+\
        "      pup.upeople_id = u.id and "+\
        "      s.date >="+ startdate+ " and "+\
        "      s.date < "+ enddate+ " and "+\
        "      "+affiliations+" "+\
        "      pup.upeople_id = upc.upeople_id and "+\
        "      upc.company_id = c.id "+\
        "group by u.identifier "+\
        "order by commits desc "+\
        "LIMIT " + limit

    data = ExecuteQuery(q)
    return (data)


def top_authors_year (year, limit) :
    # Given a year, this functions provides the top 10 authors 
    # of such year
    q = "SELECT u.id as id, u.identifier as authors, "+\
        "       count(distinct(s.id)) as commits "+\
        "FROM scmlog s, "+\
        "     people_upeople pup, "+\
        "     upeople u "+\
        "where s.author_id = pup.people_id and "+\
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



def evol_info_data_companies (startdate, enddate) :
    # DEPRECATED FUNCTION; TO BE REMOVED	

    q = "select count(distinct(c.id)) as companies "+\
         "from companies c, "+\
         "     upeople_companies upc, "+\
         "     people_upeople pup, "+\
         "     scmlog s "+\
         "where c.id = upc.company_id and "+\
         "      upc.upeople_id = pup.upeople_id and "+\
         "      pup.people_id = s.author_id and "+\
         "      s.date >="+ startdate+ " and "+\
         "      s.date < "+ enddate

    data13 = ExecuteQuery(q)

    q = "select count(distinct(c.id)) as companies_2006 "+\
        "from scmlog s, "+\
        "  people_upeople pup, "+\
        "  upeople_companies upc, "+\
        "  companies c "+\
        "where s.author_id = pup.people_id and "+\
        "  pup.upeople_id = upc.upeople_id and "+\
        "  s.date >= upc.init and  "+\
        "  s.date < upc.end and "+\
        "  upc.company_id = c.id and "+\
        "  year(s.date) = 2006"

    data14 = ExecuteQuery(q)

    q = "select count(distinct(c.id)) as companies_2009 "+\
        "from scmlog s, "+\
        "  people_upeople pup, "+\
        "  upeople_companies upc, "+\
        "  companies c "+\
        "where s.author_id = pup.people_id and "+\
        "  pup.upeople_id = upc.upeople_id and "+\
        "  s.date >= upc.init and  "+\
        "  s.date < upc.end and "+\
        "  upc.company_id = c.id and "+\
        "  year(s.date) = 2009"

    data15 = ExecuteQuery(q)

    q = "select count(distinct(c.id)) as companies_2012 "+\
        "from scmlog s, "+\
        "  people_upeople pup, "+\
        "  upeople_companies upc, "+\
        "  companies c "+\
        "where s.author_id = pup.people_id and "+\
        "  pup.upeople_id = upc.upeople_id and "+\
        "  s.date >= upc.init and "+\
        "  s.date < upc.end and "+\
        "  upc.company_id = c.id and "+\
        "  year(s.date) = 2012"

    data16 = ExecuteQuery(q)


    agg_data = dict(data13.items() + data14.items() + data15.items() + data16.items())
    return (agg_data)


def evol_info_data_countries (startdate, enddate) :

    q = "select count(distinct(upc.country_id)) as countries "+\
         "from upeople_countries upc, "+\
         "     people_upeople pup, "+\
         "     scmlog s "+\
         "where upc.upeople_id = pup.upeople_id and "+\
         "      pup.people_id = s.author_id and "+\
         "      s.date >="+ startdate+ " and "+\
         "      s.date < "+ enddate

    data = ExecuteQuery(q)
    return (data)

def company_top_authors (company_name, startdate, enddate, limit) :
    # Returns top ten authors per company

    q = "select u.id as id, u.identifier  as authors, "+\
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
        SELECT id, authors, commits FROM (
        SELECT u.id AS id, u.identifier  AS authors, count(distinct(s.id)) AS commits
        FROM people p,  scmlog s,  actions a, people_upeople pup, upeople u,
             upeople_companies upc,  companies c
        WHERE  s.id = a.commit_id AND p.id = s.author_id AND s.author_id = pup.people_id  AND
          pup.upeople_id = upc.upeople_id AND pup.upeople_id = u.id AND  s.date >= upc.init AND
          s.date < upc.end AND upc.company_id = c.id AND
          s.date >=%s AND s.date < %s AND c.name =%s) t
        GROUP BY id
        ORDER BY COUNT(commits) DESC
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
        "      people_upeople pup, "+\
        "      upeople u, "+\
        "      upeople_companies upc, "+\
        "      companies c "+\
        " where  p.id = s.author_id and "+\
        "        s.author_id = pup.people_id and "+\
        "        pup.upeople_id = upc.upeople_id and "+\
        "        pup.upeople_id = u.id and "+\
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


def evol_companies (period, startdate, enddate):	
    # Evolution of companies, also deprecated function

    q = "select ((to_days(s.date) - to_days("+startdate+")) div "+period+") as id, "+\
        "       count(distinct(upc.company_id)) as companies "+\
        "from   scmlog s, "+\
        "       people_upeople pup, "+\
        "       upeople_companies upc "+\
        "where  s.author_id = pup.people_id and "+\
        "       pup.upeople_id = upc.upeople_id and "+\
        "       s.date >= upc.init and  "+\
        "       s.date < upc.end and "+\
        "       s.date >="+ startdate+ " and "+\
        "       s.date < "+ enddate+ " "+\
        "group by ((to_days(s.date) - to_days("+startdate+")) div "+period+")"

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


# Domains
def evol_info_data_domains (startdate, enddate) :
    q = "SELECT COUNT(DISTINCT(upd.domain_id)) AS domains "+\
        "FROM upeople_domains upd, "+\
        "  people_upeople pup, "+\
        "  scmlog s "+\
        "WHERE upd.upeople_id = pup.upeople_id AND "+\
        "  pup.people_id = s.author_id AND "+\
        "  s.date >="+ startdate+ " AND "+\
        "  s.date < "+ enddate

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

def GetCodeCommunityStructure (period, startdate, enddate, identities_db):
    # This function provides information about the general structure of the community.
    # This is divided into core, regular and ocassional authors
    # Core developers are defined as those doing up to a 80% of the total commits
    # Regular developers are defind as those doing from the 80% to a 99% of the total commits
    # Occasional developers are defined as those doing from the 99% to the 100% of the commits

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
         "      s.date>="+startdate+" and "+\
         "      s.date<="+enddate+";"

    total = ExecuteQuery(q)
    total_commits = float(total['total'])

    # Database access: developer, %commits
    q = " select pup.upeople_id, "+\
        "        (count(distinct(s.id))) as commits "+\
        " from scmlog s, "+\
        "      actions a, "+\
        "      people_upeople pup, "+\
        "      people p "+\
        " where s.id = a.commit_id and "+\
        "       s.date>="+startdate+" and "+\
        "       s.date<="+enddate+" and "+\
        "       s.author_id = pup.people_id and "+\
        "       s.author_id = p.id and "+\
        "       p.email <> '%gerrit@%' and "+\
        "       p.email <> '%jenkins@%' "+\
        " group by pup.upeople_id "+\
        " order by commits desc; "

    people = ExecuteQuery(q)
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
