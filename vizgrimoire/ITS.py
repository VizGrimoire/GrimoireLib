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
## ITS.py
##
## Metrics for Issues Tracking System data source
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>
##   Luis Canas-Diaz <lcanas@bitergia.com>

import logging, os, re

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from GrimoireSQL import ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, getPeriod
from GrimoireUtils import createJSON, get_subprojects
from metrics_filter import MetricFilters

from data_source import DataSource
from filter import Filter
from query_builder import ITSQuery
import report


class ITS(DataSource):
    _metrics_set = []
    _backend = None
    debug = False


    @staticmethod
    def get_db_name():
        return "db_bicho"

    @staticmethod
    def get_name(): return "its"

    @staticmethod
    def get_date_init(startdate, enddate, identities_db, type_analysis):
        """Get the date of the first activity in the data source"""
        return GetInitDate (startdate, enddate, identities_db, type_analysis)

    @staticmethod
    def get_date_end(startdate, enddate, identities_db, type_analysis):
        """Get the date of the last activity in the data source"""
        return GetEndDate (startdate, enddate, identities_db, type_analysis)

    @staticmethod
    def get_url():
        """Get the URL from which the data source was gathered"""

        q = "SELECT url, name as type FROM trackers t JOIN "+\
            "supported_trackers s ON t.type = s.id limit 1"

        return(ExecuteQuery(q))

    @staticmethod
    def set_backend(its_name):
        backend = Backend(its_name)
        ITS._backend = backend

    @staticmethod
    def _get_backend():
        if ITS._backend == None:
            automator = report.Report.get_config()
            its_backend = automator['bicho']['backend']
            backend = Backend(its_backend)
        else:
            backend = ITS._backend
        return backend

    @staticmethod
    def _get_closed_condition():
        return ITS._get_backend().closed_condition

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        closed_condition = ITS._get_closed_condition()

        metrics = DataSource.get_metrics_data(ITS, period, startdate, enddate, identities_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(ITS, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  ITS.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = ITS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        closed_condition = ITS._get_closed_condition()

        metrics = DataSource.get_metrics_data(ITS, period, startdate, enddate, identities_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(ITS, period, startdate, enddate, False)
        agg =  dict(metrics.items()+studies.items())

        if filter_ is None:
            data = ITS.get_url()
            agg = dict(agg.items() +  data.items())

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data = ITS.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = ITS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = ITS.get_bots()
        closed_condition =  ITS._get_closed_condition()
        top = None
        mopeners = DataSource.get_metrics("openers", ITS)
        mclosers = DataSource.get_metrics("closers", ITS)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)


        if filter_ is None:
            top_closers_data = {}
            top_closers_data['closers.'] =  mclosers.get_list(mfilter, 0)
            top_closers_data['closers.last month']= mclosers.get_list(mfilter, 31)
            top_closers_data['closers.last year']= mclosers.get_list(mfilter, 365)

            top_openers_data = {}
            top_openers_data['openers.'] = mopeners.get_list(mfilter, 0)
            top_openers_data['openers.last month'] = mopeners.get_list(mfilter, 31)
            top_openers_data['openers.last year'] = mopeners.get_list(mfilter, 365)

            top = dict(top_closers_data.items() + top_openers_data.items())

            from top_issues import TopIssues
            from report import Report
            db_identities= Report.get_config()['generic']['db_identities']
            dbuser = Report.get_config()['generic']['db_user']
            dbpass = Report.get_config()['generic']['db_password']
            dbname = Report.get_config()['generic']['db_bicho']
            dbcon = ITSQuery(dbuser, dbpass, dbname, db_identities)
            metric_filters = MetricFilters(None, startdate, enddate, [])
            top_issues_data = TopIssues(dbcon, metric_filters).result()

            top = dict(top.items() + top_issues_data.items())

        else:
            filter_name = filter_.get_name()
            if filter_name in ["company","domain","repository"]:
                top = mclosers.get_list(mfilter)
            else:
                top = None

        return top

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = ITS.get_top_data (startdate, enddate, i_db, None, npeople)
        createJSON (data, destdir+"/"+ITS().get_top_filename())

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            items  = GetReposNameITS(startdate, enddate)
        elif (filter_name == "company"):
            items  = GetCompaniesNameITS(startdate, enddate, identities_db, ITS._get_closed_condition(), bots)
        elif (filter_name == "country"):
            items = GetCountriesNamesITS(startdate, enddate, identities_db, ITS._get_closed_condition())
        elif (filter_name == "domain"):
            items = GetDomainsNameITS(startdate, enddate, identities_db, ITS._get_closed_condition(), bots)
        elif (filter_name == "project"):
            items = get_projects_name(startdate, enddate, identities_db, ITS._get_closed_condition())
        else:
            logging.error(filter_name + " not supported")
        return items

    @staticmethod
    def get_filter_summary(filter_, period, startdate, enddate, identities_db, limit):
        summary = None
        filter_name = filter_.get_name()
        closed_condition =  ITS._get_closed_condition()

        if (filter_name == "company"):
            summary =  GetClosedSummaryCompanies(period, startdate, enddate, identities_db, closed_condition, limit)
        return summary

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items = ITS.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return
        items = items['name']

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        fn = os.path.join(destdir, filter_.get_filename(ITS()))
        createJSON(items, fn)

        if filter_name in ("domain", "company", "repository"):
            items_list = {'name' : [], 'closed_365' : [], 'closers_365' : []}
        else:
            items_list = items

        for item in items :
            item_name = "'"+ item+ "'"
            logging.info (item_name)
            filter_item = Filter(filter_name, item)

            evol_data = ITS.get_evolutionary_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(ITS()))
            createJSON(evol_data, fn)

            agg = ITS.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(ITS()))
            createJSON(agg, fn)

            if filter_name in ["domain", "company", "repository"]:
                items_list['name'].append(item.replace('/', '_'))
                items_list['closed_365'].append(agg['closed_365'])
                items_list['closers_365'].append(agg['closers_365'])

            if filter_name in ["company","domain","repository"]:
                top = ITS.get_top_data(startdate, enddate, identities_db, filter_item, npeople)
                fn = os.path.join(destdir, filter_item.get_top_filename(ITS()))
                createJSON(top, fn)

        fn = os.path.join(destdir, filter_.get_filename(ITS()))
        createJSON(items_list, fn)

        if (filter_name == "company"):
            closed = ITS.get_filter_summary(filter_, period, startdate, enddate, identities_db, 10)
            createJSON (closed, destdir+"/"+ filter_.get_summary_filename(ITS))

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        top_data = ITS.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['closers.']["id"]
        top += top_data['closers.last year']["id"]
        top += top_data['closers.last month']["id"]
        top += top_data['openers.']["id"]
        top += top_data['openers.last year']["id"]
        top += top_data['openers.last month']["id"]
        # remove duplicates
        people = list(set(top))

        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        closed_condition =  ITS._get_closed_condition()

        evol = GetPeopleEvolITS(upeople_id, period, startdate, enddate, closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        closed_condition =  ITS._get_closed_condition()
        return GetPeopleStaticITS(upeople_id, startdate, enddate, closed_condition)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        backend = ITS._get_backend().its_type
        # Change name for R code
        if (backend == "bg"): backend = "bugzilla"
        vizr.ReportTimeToCloseITS(backend, destdir)
        unique_ids = True

        # Demographics
        vizr.ReportDemographicsAgingITS(enddate, destdir, unique_ids)
        vizr.ReportDemographicsBirthITS(enddate, destdir, unique_ids)

        # Markov
        vizr.ReportMarkovChain(destdir)

    @staticmethod
    def _remove_people(people_id):
        # Remove from people
        q = "DELETE FROM people_upeople WHERE people_id='%s'" % (people_id)
        ExecuteQuery(q)
        q = "DELETE FROM people WHERE id='%s'" % (people_id)
        ExecuteQuery(q)

    @staticmethod
    def _remove_issue(issue_id):
        # Backend name
        its_type = ITS._get_backend().its_type
        db_ext = its_type
        if its_type == "lp": db_ext = "launchpad"
        elif its_type == "bg": db_ext = "bugzilla"
        # attachments
        q = "DELETE FROM attachments WHERE issue_id='%s'" % (issue_id)
        ExecuteQuery(q)
        # changes
        q = "DELETE FROM changes WHERE issue_id='%s'" % (issue_id)
        ExecuteQuery(q)
        # comments
        q = "DELETE FROM comments WHERE issue_id='%s'" % (issue_id)
        ExecuteQuery(q)
        # related_to
        q = "DELETE FROM related_to WHERE issue_id='%s'" % (issue_id)
        ExecuteQuery(q)
        # issues_ext_bugzilla
        q = "DELETE FROM issues_ext_%s WHERE issue_id='%s'" % (db_ext, issue_id)
        ExecuteQuery(q)
        # issues_log_bugzilla
        q = "DELETE FROM issues_log_%s WHERE issue_id='%s'" % (db_ext, issue_id)
        ExecuteQuery(q)
        # issues_watchers
        q = "DELETE FROM issues_watchers WHERE issue_id='%s'" % (issue_id)
        ExecuteQuery(q)
        # issues
        q = "DELETE FROM issues WHERE id='%s'" % (issue_id)
        ExecuteQuery(q)

    @staticmethod
    def remove_filter_data(filter_):
        uri = filter_.get_item()
        logging.info("Removing ITS filter %s %s" % (filter_.get_name(),filter_.get_item()))
        q = "SELECT * from trackers WHERE url='%s'" % (uri)
        repo = ExecuteQuery(q)
        if 'id' not in repo:
            logging.error("%s not found" % (uri))
            return

        def get_people_one_repo(field):
            return  """
                SELECT %s FROM (SELECT COUNT(DISTINCT(tracker_id)) AS total, %s
                FROM issues
                GROUP BY %s
                HAVING total=1) t
                """ % (field, field, field)


        logging.info("Removing people")
        ## Remove submitted_by that exists only in this repository
        q = """
            SELECT DISTINCT(submitted_by) from issues
            WHERE tracker_id='%s' AND submitted_by in (%s)
        """  % (repo['id'],get_people_one_repo("submitted_by"))
        res = ExecuteQuery(q)
        for people_id in res['submitted_by']:
            ITS._remove_people(people_id)
        ## Remove assigned_to that exists only in this repository
        q = """
            SELECT DISTINCT(assigned_to) from issues
            WHERE tracker_id='%s' AND assigned_to in (%s)
        """  % (repo['id'],get_people_one_repo("assigned_to"))
        res = ExecuteQuery(q)
        for people_id in res['assigned_to']:
            ITS._remove_people(people_id)

        # Remove people activity
        logging.info("Removing issues")
        q = "SELECT id from issues WHERE tracker_id='%s'" % (repo['id'])
        res = ExecuteQuery(q)
        for issue_id in res['id']:
            ITS._remove_issue(issue_id)
        # Remove filter
        q = "DELETE from trackers WHERE id='%s'" % (repo['id'])
        ExecuteQuery(q)

    @staticmethod
    def get_query_builder():
        from query_builder import ITSQuery
        return ITSQuery

    @staticmethod
    def get_metrics_core_agg():
        m = ['closed','closers','changed','changers',"opened",'openers','trackers']
        m += ['allhistory_participants']
        return m

    @staticmethod
    def get_metrics_core_ts():
        m = ['closed','closers','changed','changers',"opened",'openers','trackers']
        return m

    @staticmethod
    def get_metrics_core_trends():
        return ['closed','closers','changed','changers',"opened",'openers']


##############
# Specific FROM and WHERE clauses per type of report
##############

def GetITSSQLRepositoriesFrom ():
    # tables necessary for repositories 
    return (", trackers t")

def GetITSSQLRepositoriesWhere (repository):
    # fields necessary to match info among tables
    return (" i.tracker_id = t.id and t.url = "+repository+" ")

def GetITSSQLProjectsFrom ():
    # tables necessary for repositories
    return (", trackers t")

def GetITSSQLProjectsWhere (project, identities_db):
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



def GetITSSQLCompaniesFrom (i_db):
    # fields necessary for the companies analysis

    return(" , people_upeople pup, "+\
           i_db+".companies c, "+\
           i_db+".upeople_companies upc")

def GetITSSQLCompaniesWhere (name):
    # filters for the companies analysis
    return(" i.submitted_by = pup.people_id and "+\
           "pup.upeople_id = upc.upeople_id and "+\
           "upc.company_id = c.id and "+\
           "i.submitted_on >= upc.init and "+\
           "i.submitted_on < upc.end and "+\
           "c.name = "+name)

def GetITSSQLCountriesFrom (i_db):
    # fields necessary for the countries analysis

    return(" , people_upeople pup, "+\
           i_db+".countries c, "+\
           i_db+".upeople_countries upc")

def GetITSSQLCountriesWhere (name):
    # filters for the countries analysis
    return(" i.submitted_by = pup.people_id and "+\
           "pup.upeople_id = upc.upeople_id and "+\
           "upc.country_id = c.id and "+\
           "c.name = "+name)


def GetITSSQLDomainsFrom (i_db):
    # fields necessary for the domains analysis

    return(" , people_upeople pup, "+\
           i_db+".domains d, "+\
           i_db+".upeople_domains upd")


def GetITSSQLDomainsWhere (name):
    # filters for the domains analysis
    return(" i.submitted_by = pup.people_id and "+\
           "pup.upeople_id = upd.upeople_id and "+\
           "upd.domain_id = d.id and "+\
           "d.name = "+name)

##########
#Generic functions to obtain FROM and WHERE clauses per type of report
##########

def GetITSSQLReportFrom (identities_db, type_analysis):
    #generic function to generate 'from' clauses
    #"type" is a list of two values: type of analysis and value of 
    #such analysis

    From = ""

    if (type_analysis is None or len(type_analysis) != 2): return From

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': From = GetITSSQLRepositoriesFrom()
    elif analysis == 'company': From = GetITSSQLCompaniesFrom(identities_db)
    elif analysis == 'country': From = GetITSSQLCountriesFrom(identities_db)
    elif analysis == 'domain': From = GetITSSQLDomainsFrom(identities_db)
    elif analysis == 'project': From = GetITSSQLProjectsFrom()

    return (From)

def GetITSSQLReportWhere (type_analysis, identities_db = None):
    #generic function to generate 'where' clauses

    #"type" is a list of two values: type of analysis and value of 
    #such analysis
    where = ""

    if (type_analysis is None or len(type_analysis) != 2): return where

    analysis = type_analysis[0]
    value = type_analysis[1]

    if analysis == 'repository': where = GetITSSQLRepositoriesWhere(value)
    elif analysis == 'company': where = GetITSSQLCompaniesWhere(value)
    elif analysis == 'country': where = GetITSSQLCountriesWhere(value)
    elif analysis == 'domain': where = GetITSSQLDomainsWhere(value)
    elif analysis == 'project': where = GetITSSQLProjectsWhere(value, identities_db)

    return (where)

def GetDate (startdate, enddate, identities_db, type_analysis, type):
    # date of submmitted issues (type= max or min)
    if (type=="max"):
        fields = " DATE_FORMAT (max(submitted_on), '%Y-%m-%d') as last_date"
    else :
        fields = " DATE_FORMAT (min(submitted_on), '%Y-%m-%d') as first_date"

    tables = " issues i " + GetITSSQLReportFrom(identities_db, type_analysis)
    filters = GetITSSQLReportWhere(type_analysis, identities_db)

    q = BuildQuery(None, startdate, enddate, " i.submitted_on ", fields, tables, filters, False)
    data = ExecuteQuery(q)
    return(data)

def GetInitDate (startdate, enddate, identities_db, type_analysis):
    #Initial date of submitted issues
    return(GetDate(startdate, enddate, identities_db, type_analysis, "min"))

def GetEndDate (startdate, enddate, identities_db, type_analysis):
    #End date of submitted issues
    return(GetDate(startdate, enddate, identities_db, type_analysis, "max"))


###############
# Others
###############

def AggAllParticipants (startdate, enddate):
    # All participants from the whole history
    q = "SELECT count(distinct(pup.upeople_id)) as allhistory_participants from people_upeople pup"

    return(ExecuteQuery(q))

###############
# Lists of repositories, companies, countries and other analysis
###############

def GetReposNameITS (startdate, enddate) :
    # List the url of each of the repositories analyzed
    # Those are order by the number of opened issues (dec order)
    q = " SELECT t.url as name "+\
               "   FROM issues i, "+\
               "        trackers t "+\
               "   WHERE i.tracker_id=t.id and "+\
               "         i.submitted_on >= "+ startdate+ " and "+\
               "         i.submitted_on < "+ enddate+\
               "   GROUP BY t.url  "+\
               "   ORDER BY count(distinct(i.id)) DESC "

    data = ExecuteQuery(q)
    return (data)

def get_projects_name (startdate, enddate, identities_db, closed_condition) :
    # Projects activity needs to include subprojects also
    logging.info ("Getting projects list for ITS")

    # debug
    if ITS.debug:
        return {"name":['eclipse']}

    # Get all projects list
    q = "SELECT p.id AS name FROM  %s.projects p" % (identities_db)
    projects = ExecuteQuery(q)
    data = []

    # Loop all projects getting reviews
    for project in projects['name']:
        type_analysis = ['project', project]

        period = None
        filter_com = MetricFilters(period, startdate, enddate, type_analysis)
        mclosed = ITS.get_metrics("closed", ITS)
        mclosed.filters = filter_com
        issues = mclosed.get_agg()

        issues = issues['closed']
        if (issues > 0):
            data.append([issues,project])

    # Order the list using reviews: https://wiki.python.org/moin/HowTo/Sorting
    from operator import itemgetter
    data_sort = sorted(data, key=itemgetter(0),reverse=True)
    names = [name[1] for name in data_sort]

    return({"name":names})

def GetTablesDomainsITS (i_db, table='') :
    tables = GetTablesOwnUniqueIdsITS(table)
    tables += ','+i_db+'.upeople_domains upd'
    return(tables)

def GetFiltersDomainsITS (table='') :
    filters = GetFiltersOwnUniqueIdsITS(table)
    filters += " AND pup.upeople_id = upd.upeople_id"
    return(filters)

def GetDomainsNameITS (startdate, enddate, identities_db, closed_condition, filter) :
    affiliations = ""
    for aff in filter:
        affiliations += " dom.name<>'"+aff+"' and "

    tables = GetTablesDomainsITS(identities_db)
    tables += ","+identities_db+".domains dom"

    q = "SELECT dom.name "+\
        "FROM "+ tables + " "+\
        "WHERE " + GetFiltersDomainsITS() +" AND "+\
        "       dom.id = upd.domain_id and "+\
        "       "+ affiliations +" "+\
        "       c.changed_on >= "+ startdate+ " AND "+\
        "       c.changed_on < "+ enddate+ " AND "+\
        "       "+ closed_condition+" "+\
        "GROUP BY dom.name "+\
        "ORDER BY COUNT(DISTINCT(c.issue_id)) DESC"
    data = ExecuteQuery(q)
    return (data)

def GetCountriesNamesITS (startdate, enddate, identities_db, closed_condition) :
    # List each of the countries analyzed
    # Those are order by number of closed issues
    q = "select cou.name "+\
        "from issues i, "+\
        "     changes ch, "+\
        "     people_upeople pup, "+\
        "     "+ identities_db+ ".upeople_countries upc, "+\
        "     "+ identities_db+ ".countries cou "+\
        "where i.id = ch.issue_id and "+\
        "      ch.changed_by = pup.people_id and "+\
        "      pup.upeople_id = upc.upeople_id and "+\
        "      upc.country_id = cou.id and "+\
        "      ch.changed_on >= "+ startdate+ " and "+\
        "      ch.changed_on < "+ enddate+" and "+\
        "      "+ closed_condition+ " "+\
        "      group by cou.name  "+\
        "      order by count(distinct(i.id)) desc"

    data = ExecuteQuery(q)
    return (data)

def GetCompaniesNameITS (startdate, enddate, identities_db, closed_condition, filter) :
    affiliations = ""
    for aff in filter:
        affiliations += " c.name<>'"+aff+"' and "

    # list each of the companies analyzed
    # those are order by number of closed issues
    q = "select c.name "+\
        "from issues i, "+\
        "     changes ch, "+\
        "     people_upeople pup, "+\
        "     "+ identities_db+ ".upeople_companies upc, "+\
        "     "+ identities_db+ ".companies c "+\
        "where i.id = ch.issue_id and "+\
        "      ch.changed_by = pup.people_id and "+\
        "      pup.upeople_id = upc.upeople_id and "+\
        "      upc.company_id = c.id and "+\
        "      ch.changed_on >= "+ startdate+ " and "+\
        "      ch.changed_on < "+ enddate+" and "+\
        "      i.submitted_on >= upc.init and "+\
        "      i.submitted_on < upc.end and "+\
        "      "+ affiliations  +\
               closed_condition +\
        "      group by c.name  "+\
        "      order by count(distinct(i.id)) desc"

    data = ExecuteQuery(q)
    return (data)

def GetLastActivityITS (days, closed_condition):
    # opened issues
    days = str(days)
    q = "select count(*) as opened_"+days+" "+\
        "from issues "+\
        "where submitted_on >= ( "+\
        "      select (max(submitted_on) - INTERVAL "+days+" day) "+\
        "      from issues)"

    data1 = ExecuteQuery(q)

    # closed issues
    q = "select count(distinct(issue_id)) as closed_"+days+" "+\
        "from changes "+\
        "where  "+closed_condition+" "+\
        "and changed_on >= ( "+\
        "      select (max(changed_on) - INTERVAL "+days+" day) "+\
        "      from changes)"

    data2 = ExecuteQuery(q)

    # closers
    q = "SELECT count(distinct(pup.upeople_id)) as closers_"+days+" "+\
         "FROM changes, people_upeople pup "+\
         "WHERE pup.people_id = changes.changed_by and "+\
         "changed_on >= ( "+\
         "    select (max(changed_on) - INTERVAL "+days+" day) "+\
         "     from changes) AND "+ closed_condition


    data3 = ExecuteQuery(q)

    # people_involved    
    q = "SELECT count(distinct(pup.upeople_id)) as changers_"+days+" "+\
         "FROM changes, people_upeople pup "+\
         "WHERE pup.people_id = changes.changed_by and "+\
         "changed_on >= ( "+\
         "    select (max(changed_on) - INTERVAL "+days+" day) "+\
         "     from changes)"

    data4 = ExecuteQuery(q)

    agg_data = dict(data1.items()+data2.items())
    agg_data = dict(agg_data.items()+data3.items())

    return (agg_data)


def GetActiveChangersITS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT distinct(pup.upeople_id) as active_changers"+\
        " FROM changes, people_upeople pup "+\
        " WHERE pup.people_id = changes.changed_by and "+\
        " changed_on >= ( %s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActiveCommentersITS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT DISTINCT(pup.upeople_id) AS active_commenters"+\
        " FROM comments c, people_upeople pup"+\
        " WHERE pup.people_id = c.submitted_by AND"+\
        " submitted_on >= (%s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActiveSubmittersITS(days, enddate):
    # FIXME parameters should be: startdate and enddate
    q0 = "SELECT DISTINCT(pup.upeople_id) AS active_submitters"+\
      " FROM issues i, people_upeople pup"+\
      " WHERE pup.people_id = i.submitted_by AND"+\
      " submitted_on >= ( %s - INTERVAL %s day)"
    q1 = q0 % (enddate, days)
    data = ExecuteQuery(q1)
    return(data)

def GetActivePeopleITS(days, enddate):
    #Gets the IDs of the active people during the last days (until enddate)
    # for comments, issue creation and changes
    submitters = GetActiveSubmittersITS(days, enddate)
    changers = GetActiveChangersITS(days, enddate)
    commenters = GetActiveCommentersITS(days, enddate)
    people_its = submitters['active_submitters'] + changers['active_changers'] +\
        commenters['active_commenters']
    people_its = list(set(people_its))
    return(people_its)


################
# Top functions
################

def GetTopClosersByAssignee (days, startdate, enddate, identities_db, filter) :

    affiliations = ""
    for aff in filter:
        affiliations += " com.name<>'"+ aff +"' and "

    date_limit = ""
    if (days != 0 ) :
        sql = "SELECT @maxdate:=max(changed_on) from changes limit 1"
        ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate, changed_on)<"+str(days)

    q = "SELECT up.id as id, "+\
        "       up.identifier as closers, "+\
        "       count(distinct(ill.issue_id)) as closed "+\
        "FROM people_upeople pup,  "+\
        "     "+ identities_db+ ".upeople_companies upc, "+\
        "     "+ identities_db+ ".upeople up,  "+\
        "     "+ identities_db+ ".companies com, "+\
        "     issues_log_launchpad ill  "+\
        "WHERE ill.assigned_to = pup.people_id and "+\
        "      pup.upeople_id = up.id and  "+\
        "      up.id = upc.upeople_id and  "+\
        "      upc.company_id = com.id and "+\
        "      "+ affiliations+ " "+\
        "      ill.date >= upc.init and "+\
        "      ill.date < upc.end and  "+\
        "      ill.change_id  in (  "+\
        "         select id "+\
        "         from changes  "+\
        "         where new_value='Fix Committed' and "+\
        "               changed_on>="+ startdate+ " and  "+\
        "               changed_on<"+ enddate+ " "+ date_limit+") "+\
        "GROUP BY up.identifier "+\
        "ORDER BY closed desc, closers limit 10"


    data = ExecuteQuery(q)
    return (data)


def GetTablesOwnUniqueIdsITS (table='') :
    tables = 'changes c, people_upeople pup'
    if (table == "issues"): tables = 'issues i, people_upeople pup'
    return (tables)

def GetFiltersOwnUniqueIdsITS (table='') :
    filters = 'pup.people_id = c.changed_by'
    if (table == "issues"): filters = 'pup.people_id = i.submitted_by'
    return (filters)


#################
# People information, to be refactored
#################

def GetPeopleListITS (startdate, enddate) :
    fields = "DISTINCT(pup.upeople_id) as pid, count(c.id) as total"
    tables = GetTablesOwnUniqueIdsITS()
    filters = GetFiltersOwnUniqueIdsITS()
    filters += " GROUP BY pid ORDER BY total desc"
    q = GetSQLGlobal('changed_on',fields,tables, filters, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)


def GetPeopleQueryITS (developer_id, period, startdate, enddate, evol,  closed_condition) :
    fields = " COUNT(distinct(c.issue_id)) AS closed"
    tables = GetTablesOwnUniqueIdsITS()
    filters = GetFiltersOwnUniqueIdsITS() + " AND pup.upeople_id = "+ str(developer_id)
    filters += " AND "+ closed_condition

    if (evol) :
        q = GetSQLPeriod(period,'changed_on', fields, tables, filters,
                            startdate, enddate)
    else :
        fields += ",DATE_FORMAT (min(changed_on),'%Y-%m-%d') as first_date, "+\
                  "DATE_FORMAT (max(changed_on),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('changed_on', fields, tables, filters,
                            startdate, enddate)

    return (q)

def GetPeopleEvolITS (developer_id, period, startdate, enddate, closed_condition) :
    ## FIXME is this function used only to calculate closed issues? if not it must be
    ## fixed
    q = GetPeopleQueryITS(developer_id, period, startdate, enddate, True, closed_condition)

    data = ExecuteQuery(q)
    return (data)

def GetPeopleStaticITS (developer_id, startdate, enddate, closed_condition) :
    ## FIXME is this function used only to calculate closed issues? if not it must be
    ## fixed
    q = GetPeopleQueryITS(developer_id, None, startdate, enddate, False, closed_condition)

    data = ExecuteQuery(q)
    return (data)

#################
# Micro studies
#################

def EvolBMIIndex(period, startdate, enddate, identities_db, type_analysis, closed_condition):
    # Metric based on chapter 4.3.1 from
    # "Metrics and Models in Software Quality Engineering"
    # by Stephen H. Kan
    closed = EvolIssuesClosed(period, startdate, enddate, identities_db, type_analysis, closed_condition)
    closed = completePeriodIds(closed, period, startdate, enddate)
    opened = EvolIssuesOpened(period, startdate, enddate, identities_db, type_analysis)
    opened = completePeriodIds(opened, period, startdate, enddate)

    evol_bmi = []
    for i in closed["closed"]:

        index = closed["closed"].index(i)
        if opened["opened"][index] == 0:
            #div by 0
            evol_bmi.append(100) # some "neutral" value, although this should be infinite
        else:
            evol_bmi.append((float(i) / float(opened['opened'][index])) * 100)

    return {'closed' : closed['closed'],
            'opened' : opened['opened'],
            'bmi' : evol_bmi}

def GetClosedSummaryCompanies (period, startdate, enddate, identities_db, closed_condition, num_companies):
    count = 1
    first_companies = {}

    companies = GetCompaniesNameITS(startdate, enddate, identities_db, closed_condition, ["-Bot", "-Individual", "-Unknown"])
    companies = companies['name']

    for company in companies:
        type_analysis = ["company", "'"+company+"'"]
        filter_com = MetricFilters(period, startdate, enddate, type_analysis)
        mclosed = ITS.get_metrics("closed", ITS)
        mclosed.filters = filter_com
        closed = mclosed.get_ts()
        # Rename field closed to company name
        closed[company] = closed["closed"]
        del closed['closed']

        if (count <= num_companies):
            #Case of companies with entity in the dataset
            first_companies = dict(first_companies.items() + closed.items())
        else :
            #Case of companies that are aggregated in the field Others
            if 'Others' not in first_companies:
                first_companies['Others'] = closed[company]
            else:
                first_companies['Others'] = [a+b for a, b in zip(first_companies['Others'],closed[company])]
        count = count + 1
    first_companies = completePeriodIds(first_companies, period, startdate, enddate)

    return(first_companies)


class Backend(object):

    its_type = ""
    closed_condition = ""
    reopened_condition = ""
    name_log_table = ""
    statuses = ""
    open_status = ""
    reopened_status = ""
    name_log_table = ""

    def __init__(self, its_type):
        self.its_type = its_type
        if (its_type == 'allura'):
            self.closed_condition = "new_value='CLOSED'"
        elif (its_type == 'bugzilla' or its_type == 'bg'):
            self.closed_condition = "(new_value='RESOLVED' OR new_value='CLOSED')"
            self.reopened_condition = "new_value='NEW'"
            self.name_log_table = 'issues_log_bugzilla'
            self.statuses = ["NEW", "ASSIGNED"]
            #Pretty specific states in Red Hat's Bugzilla
            self.statuses = ["ASSIGNED", "CLOSED", "MODIFIED", "NEW", "ON_DEV", \
                    "ON_QA", "POST", "RELEASE_PENDING", "VERIFIED"]
            Backend.priority = ["Unprioritized", "Low", "Normal", "High", "Highest", "Immediate"]
            Backend.severity = ["trivial", "minor", "normal", "major", "blocker", "critical", "enhancement"]

        elif (its_type == 'github'):
            self.closed_condition = "field='closed'"

        elif (its_type == 'jira'):
            self.closed_condition = "(new_value='Closed')"
            self.reopened_condition = "new_value='Reopened'"
            #self.new_condition = "status='Open'"
            #self.reopened_condition = "status='Reopened'"
            self.statuses = ["Open", "In Progress", "Ready To Review", "Reviewable", "Closed", "Resolved", "Reopened"]
            self.open_status = 'Open'
            self.reopened_status = 'Reopened'
            self.name_log_table = 'issues_log_jira'

        elif (its_type == 'lp' or its_type == 'launchpad'):
            #self.closed_condition = "(new_value='Fix Released' or new_value='Invalid' or new_value='Expired' or new_value='Won''t Fix')"
            self.closed_condition = "(new_value='Fix Committed')"
            self.statuses = ["Confirmed", "Fix Committed", "New", "In Progress", "Triaged", "Incomplete", "Invalid", "Won\\'t Fix", "Fix Released", "Opinion", "Unknown", "Expired"]
            self.name_log_table = 'issues_log_launchpad'

        elif (its_type == 'redmine'):
            self.statuses = ["New", "Verified", "Need More Info", "In Progress", "Feedback",
                         "Need Review", "Testing", "Pending Backport", "Pending Upstream",
                         "Resolved", "Closed", "Rejected", "Won\\'t Fix", "Can\\'t reproduce",
                         "Duplicate"]
            self.closed_condition = "(new_value='Resolved' OR new_value='Closed' OR new_value='Rejected'" +\
                                  " OR new_value='Won\\'t Fix' OR new_value='Can\\'t reproduce' OR new_value='Duplicate')"
            self.reopened_condition = "new_value='Reopened'" # FIXME: fake condition
            self.name_log_table = 'issues_log_redmine'
        else:
            logging.error("Backend not found: " + its_type)
            raise Exception
