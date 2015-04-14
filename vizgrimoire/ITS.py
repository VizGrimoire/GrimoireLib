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

from vizgrimoire.GrimoireSQL import GetSQLGlobal, GetSQLPeriod
from vizgrimoire.GrimoireSQL import ExecuteQuery, BuildQuery
from vizgrimoire.GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, getPeriod, check_array_value
from vizgrimoire.GrimoireUtils import createJSON
from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.metrics.query_builder import ITSQuery
import vizgrimoire.report
from vizgrimoire.data_source import DataSource
from vizgrimoire.filter import Filter


class ITS(DataSource):
    _metrics_set = []
    _backend = None
    _global_filter = []
    debug = False


    @staticmethod
    def get_db_name():
        return "db_bicho"

    @staticmethod
    def get_name(): return "its"
 
    @staticmethod
    def get_url():
        """Get the URL from which the data source was gathered"""

        q = "SELECT url, name as type FROM trackers t JOIN "+\
            "supported_trackers s ON t.type = s.id limit 1"

        return(ExecuteQuery(q))

    @classmethod
    def set_backend(cls, its_name):
        backend = Backend(its_name)
        cls._backend = backend

    @staticmethod
    def _get_backend():
        import vizgrimoire.report
        if ITS._backend == None:
            automator = vizgrimoire.report.Report.get_config()
            its_backend = automator['bicho']['backend']
            backend = Backend(its_backend)
        else:
            backend = ITS._backend
        return backend

    @classmethod
    def _get_closed_condition(cls):
        # print cls
        return cls._get_backend().closed_condition

    @classmethod
    def get_evolutionary_data (cls, period, startdate, enddate, identities_db, filter_ = None):
        closed_condition = cls._get_closed_condition()

        metrics = cls.get_metrics_data(period, startdate, enddate, identities_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(cls, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @classmethod
    def create_evolutionary_report (cls, period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  cls.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = cls().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @classmethod
    def get_agg_data (cls, period, startdate, enddate, identities_db, filter_ = None):
        closed_condition = cls._get_closed_condition()

        metrics = cls.get_metrics_data(period, startdate, enddate, identities_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(cls, period, startdate, enddate, False)
        agg =  dict(metrics.items()+studies.items())

        if filter_ is None:
            data = cls.get_url()
            agg = dict(agg.items() +  data.items())

        return agg

    @classmethod
    def create_agg_report (cls, period, startdate, enddate, destdir, i_db, filter_ = None):
        data = cls.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = cls().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_metrics ():
        return ["openers","closers"]

    @classmethod
    def get_top_data (cls, startdate, enddate, identities_db, filter_, npeople):
        bots = cls.get_bots()
        closed_condition =  cls._get_closed_condition()
        # TODO: It should be configurable from Automator
        top_issues_on = False
        top = None
        mopeners = DataSource.get_metrics("openers", cls)
        mclosers = DataSource.get_metrics("closers", cls)
        # We should check this metric is ON
        stories_openers = DataSource.get_metrics("stories_openers", cls)
        if mopeners is None or mclosers is None: return None
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)
        if mclosers.filters.closed_condition is not None:
             mfilter.closed_condition = mclosers.filters.closed_condition

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

            if top_issues_on:
                from vizgrimoire.analysis.top_issues import TopIssues
                from vizgrimoire.report import Report
                db_identities= Report.get_config()['generic']['db_identities']
                dbuser = Report.get_config()['generic']['db_user']
                dbpass = Report.get_config()['generic']['db_password']
                dbname = Report.get_config()['generic']['db_bicho']
                dbcon = ITSQuery(dbuser, dbpass, dbname, db_identities)
                metric_filters = MetricFilters(None, startdate, enddate, [])
                top_issues_data = TopIssues(dbcon, metric_filters).result()

                top = dict(top.items() + top_issues_data.items())

            if False and stories_openers is not None:
                top_sopeners_data = {}
                top_sopeners_data['stories_openers.'] = stories_openers.get_list(mfilter, 0)
                top_sopeners_data['stories_openers.last month'] = stories_openers.get_list(mfilter, 31)
                top_sopeners_data['stories_openers.last year'] = stories_openers.get_list(mfilter, 365)

                top = dict(top.items() + top_sopeners_data.items())
        else:
            filter_name = filter_.get_name()
            if filter_name in ["company","domain","repository"]:
                if filter_name in ["company","domain","repository"]:
                    top = {}
                    top['closers.'] =  mclosers.get_list(mfilter, 0)
                    top['closers.last month']= mclosers.get_list(mfilter, 31)
                    top['closers.last year']= mclosers.get_list(mfilter, 365)
                else:
                    # Remove filters above if there are performance issues
                    top = mclosers.get_list(mfilter)
            else:
                top = None

        return top

    @classmethod
    def create_top_report (cls, startdate, enddate, destdir, npeople, i_db):
        data = cls.get_top_data (startdate, enddate, i_db, None, npeople)
        createJSON (data, destdir+"/"+cls().get_top_filename())

    @classmethod
    def get_filter_items(cls, filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        if (filter_name == "repository"):
            metric = DataSource.get_metrics("trackers", cls)
        elif (filter_name == "company"):
            metric = DataSource.get_metrics("organizations",  cls)
        elif (filter_name == "country"):
            metric = DataSource.get_metrics("countries", cls)
        elif (filter_name == "domain"):
            metric = DataSource.get_metrics("domains", cls)
        elif (filter_name == "project"):
            metric = DataSource.get_metrics("projects", cls)
        elif (filter_name == "people2"):
            metric = DataSource.get_metrics("people2", cls)
        elif (filter_name == "company"+MetricFilters.DELIMITER+"country"):
            metric = DataSource.get_metrics("companies+countries", cls)
        else:
            logging.error(filter_name + " not supported")
            return items

        items = metric.get_list()
        return items

    @classmethod
    def get_filter_summary(cls,filter_, period, startdate, enddate, identities_db, limit):
        summary = None
        filter_name = filter_.get_name()
        closed_condition =  cls._get_closed_condition()

        if (filter_name == "company"):
            from vizgrimoire.analysis.summaries import GetClosedSummaryCompanies

            summary =  GetClosedSummaryCompanies(period, startdate, enddate, identities_db, closed_condition, limit)
        return summary

    @classmethod
    def create_filter_report(cls, filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items = cls.get_filter_items(filter_, startdate, enddate, identities_db)
            if (items == None): return
            items = items['name']

        filter_name = filter_.get_name()

        if not isinstance(items, (list)):
            items = [items]

        fn = os.path.join(destdir, filter_.get_filename(cls()))
        createJSON(items, fn)

        if filter_name in ("domain", "company", "repository"):
            items_list = {'name' : [], 'closed_365' : [], 'closers_365' : []}
        else:
            items_list = items

        for item in items :
            item_name = "'"+ item+ "'"
            logging.info (item_name)
            filter_item = Filter(filter_name, item)

            evol_data = cls.get_evolutionary_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_evolutionary_filename(cls()))
            createJSON(evol_data, fn)

            agg = cls.get_agg_data(period, startdate, enddate, identities_db, filter_item)
            fn = os.path.join(destdir, filter_item.get_static_filename(cls()))
            createJSON(agg, fn)

            if filter_name in ["domain", "company", "repository"]:
                items_list['name'].append(item.replace('/', '_'))
                items_list['closed_365'].append(agg['closed_365'])
                items_list['closers_365'].append(agg['closers_365'])

            if filter_name in ["company","domain","repository"]:
                top = cls.get_top_data(startdate, enddate, identities_db, filter_item, npeople)
                fn = os.path.join(destdir, filter_item.get_top_filename(cls()))
                createJSON(top, fn)

        fn = os.path.join(destdir, filter_.get_filename(cls()))
        createJSON(items_list, fn)

        if (filter_name == "company"):
            ds = ITS
            summary = cls.get_filter_summary(
                filter_, period, startdate, enddate,
                identities_db, 10
                )
            createJSON (summary,
                        destdir + "/" + filter_.get_summary_filename(cls))

            # Perform ages study, if it is specified in Report
            cls.ages_study_com (items, period, startdate, enddate, destdir)


    @staticmethod
    def _check_report_all_data(data, filter_, startdate, enddate, idb,
                               evol = False, period = None):
        pass

    @classmethod
    def create_filter_report_all(cls, filter_, period, startdate, enddate, destdir, npeople, identities_db):
        check = False # activate to debug issues
        filter_name = filter_.get_name()

        # Change filter to GrimoireLib notation
        filter_name = filter_name.replace("+", MetricFilters.DELIMITER)

        if filter_name in ["people2","company","company"+MetricFilters.DELIMITER+"country",
                           "country","repository","domain"] :
            filter_all = Filter(filter_name, None)
            agg_all = cls.get_agg_data(period, startdate, enddate,
                                       identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_static_filename_all(cls()))
            createJSON(agg_all, fn)
            ITS.convert_all_to_single(agg_all, filter_, destdir, False)

            evol_all = cls.get_evolutionary_data(period, startdate, enddate,
                                                 identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_evolutionary_filename_all(cls()))
            createJSON(evol_all, fn)
            ITS.convert_all_to_single(evol_all, filter_, destdir, True)

            if check:
                cls._check_report_all_data(evol_all, filter_, startdate, enddate,
                                           identities_db, True, period)
                cls._check_report_all_data(agg_all, filter_, startdate, enddate,
                                           identities_db, False, period)
        else:
            logging.error(filter_name +" does not support yet group by items sql queries")

    @classmethod
    def get_top_people(cls, startdate, enddate, identities_db, npeople):
        top_data = cls.get_top_data (startdate, enddate, identities_db, None, npeople)
        if top_data is None: return None

        top = top_data['closers.']["id"]
        top += top_data['closers.last year']["id"]
        top += check_array_value(top_data['closers.last month']["id"])
        top += check_array_value(top_data['openers.']["id"])
        top += check_array_value(top_data['openers.last year']["id"])
        top += check_array_value(top_data['openers.last month']["id"])
        if 'stories_openers' in top_data:
            top += top_data['stories_openers.']["id"]
            top += top_data['stories_openers.last year']["id"]
            top += top_data['stories_openers.last month']["id"]

        # remove duplicates
        people = list(set(top))

        return people

    @classmethod
    def get_person_evol(cls, uuid, period, startdate, enddate, identities_db, type_analysis):
        closed_condition =  cls._get_closed_condition()

        evol = GetPeopleEvolITS(uuid, period, startdate, enddate, closed_condition)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol

    @classmethod
    def get_person_agg(cls, uuid, startdate, enddate, identities_db, type_analysis):
        closed_condition =  cls._get_closed_condition()
        return GetPeopleStaticITS(uuid, startdate, enddate, closed_condition)

    @classmethod
    def create_r_reports(cls, vizr, enddate, destdir):
        backend = cls._get_backend().its_type
        # Change name for R code
        if (backend == "bg"): backend = "bugzilla"
        if (backend == "lp"): backend = "launchpad"
        vizr.ReportTimeToCloseITS(backend, destdir)
        unique_ids = True

        # Demographics - created now with age study in Python
        # vizr.ReportDemographicsAgingITS(enddate, destdir, unique_ids)
        # vizr.ReportDemographicsBirthITS(enddate, destdir, unique_ids)

        # Markov
        vizr.ReportMarkovChain(destdir)

    @staticmethod
    def _remove_people(people_id):
        # Remove from people
        q = "DELETE FROM people_upeople WHERE people_id='%s'" % (people_id)
        ExecuteQuery(q)
        q = "DELETE FROM people WHERE id='%s'" % (people_id)
        ExecuteQuery(q)

    @classmethod
    def _remove_issue(cls, issue_id):
        # Backend name
        its_type = cls._get_backend().its_type
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
        from vizgrimoire.metrics.query_builder import ITSQuery
        return ITSQuery

    @staticmethod
    def get_metrics_core_agg():
        m = ['closed','closers','changed','changers',"opened",'openers','trackers']
        m += ['allhistory_participants','bmitickets']
        return m

    @staticmethod
    def get_metrics_core_ts():
        m = ['closed','closers','changed','changers',"opened",'openers','trackers']
        m += ['bmitickets']
        return m

    @staticmethod
    def get_metrics_core_trends():
        m = ['closed','closers','changed','changers',"opened",'openers']
        m += ['bmitickets']
        return m

###############
# Others
###############

def AggAllParticipants (startdate, enddate):
    # All participants from the whole history
    q = "SELECT count(distinct(pup.uuid)) as allhistory_participants from people_uidentities pup"

    return(ExecuteQuery(q))


################
# Top functions
################

def GetTopClosersByAssignee (days, startdate, enddate, identities_db, filter) :

    affiliations = ""
    for aff in filter:
        affiliations += " org.name<>'"+ aff +"' and "

    date_limit = ""
    if (days != 0 ) :
        sql = "SELECT @maxdate:=max(changed_on) from changes limit 1"
        ExecuteQuery(sql)
        date_limit = " AND DATEDIFF(@maxdate, changed_on)<"+str(days)

    q = "SELECT up.uuid as id, "+\
        "       up.identifier as closers, "+\
        "       count(distinct(ill.issue_id)) as closed "+\
        "FROM people_uidentities pup,  "+\
        "     "+ identities_db+ ".enrollments enr, "+\
        "     "+ identities_db+ ".uidentities up,  "+\
        "     "+ identities_db+ ".organizations org, "+\
        "     issues_log_launchpad ill  "+\
        "WHERE ill.assigned_to = pup.people_id and "+\
        "      pup.uuid = up.uuid and  "+\
        "      up.uuid = enr.uuid and  "+\
        "      enr.organization_id = org.id and "+\
        "      "+ affiliations+ " "+\
        "      ill.date >= enr.start and "+\
        "      ill.date < enr.end and  "+\
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
    tables = 'changes c, people_uidentities pup'
    if (table == "issues"): tables = 'issues i, people_uidentities pup'
    return (tables)

def GetFiltersOwnUniqueIdsITS (table='') :
    filters = 'pup.people_id = c.changed_by'
    if (table == "issues"): filters = 'pup.people_id = i.submitted_by'
    return (filters)


#################
# People information, to be refactored
#################

def GetPeopleListITS (startdate, enddate) :
    fields = "DISTINCT(pup.uuid) as pid, count(c.id) as total"
    tables = GetTablesOwnUniqueIdsITS()
    filters = GetFiltersOwnUniqueIdsITS()
    filters += " GROUP BY pid ORDER BY total desc"
    q = GetSQLGlobal('changed_on',fields,tables, filters, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)


def GetPeopleQueryITS (developer_id, period, startdate, enddate, evol,  closed_condition) :
    fields = " COUNT(distinct(c.issue_id)) AS closed"
    tables = GetTablesOwnUniqueIdsITS()
    filters = GetFiltersOwnUniqueIdsITS() + " AND pup.uuid = '"+ str(developer_id)+"'"
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

        elif (its_type == 'storyboard'):
            self.closed_condition = "(new_value='merged' or new_value='invalid')"

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
