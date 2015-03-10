## Copyright (C) 2013 Bitergia
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
## Queries for MediaWiki data analysis
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

import logging, os

from filter import Filter
from vizgrimoire.GrimoireSQL import GetSQLGlobal, GetSQLPeriod 
from vizgrimoire.GrimoireSQL import ExecuteQuery, BuildQuery
from vizgrimoire.GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, createJSON
from vizgrimoire.metrics.metrics_filter import MetricFilters


from vizgrimoire.data_source import DataSource


class Mediawiki(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_mediawiki"

    @staticmethod
    def get_name(): return "mediawiki"

    @staticmethod
    def get_date_init(startdate, enddate, identities_db, type_analysis):
        fields = "DATE_FORMAT(MIN(date),'%Y-%m-%d') AS first_date"
        tables = "wiki_pages_revs"
        filters = ""
        q = GetSQLGlobal('date',fields, tables, filters, startdate, enddate)
        return ExecuteQuery(q)

    @staticmethod
    def get_date_end(startdate, enddate, identities_db, type_analysis):
        fields = "DATE_FORMAT(MAX(date),'%Y-%m-%d') AS last_date"
        tables = "wiki_pages_revs"
        filters = ""
        q = GetSQLGlobal('date',fields, tables, filters, startdate, enddate)
        return ExecuteQuery(q)

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, filter_ = None):
        if filter_ is not None:
            if filter_.get_name() != "people2":
                logging.warn("Mediawiki only supports people2 filter.")
                return {}

        metrics =  DataSource.get_metrics_data(Mediawiki, period, startdate, enddate, i_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_studies_data(Mediawiki, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  Mediawiki.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = Mediawiki().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        if filter_ is not None:
            if filter_.get_name() != "people2":
                logging.warn("Mediawiki only supports people2 filter.")
                return {}

        metrics =  DataSource.get_metrics_data(Mediawiki, period, startdate, enddate, identities_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_studies_data(Mediawiki, period, startdate, enddate, False)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data = Mediawiki.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = Mediawiki().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_metrics ():
        return ["authors"]

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        top = {}
        mauthors = DataSource.get_metrics("authors", Mediawiki)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top['authors.'] = mauthors.get_list(mfilter, 0)
            top['authors.last month'] = mauthors.get_list(mfilter, 31)
            top['authors.last year'] = mauthors.get_list(mfilter, 365)
        else:
            logging.info("Mediawiki does not support yet top for filters.")

        return(top)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = Mediawiki.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+Mediawiki().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        items = None
        filter_name = filter_.get_name()

        if (filter_name == "people2"):
            metric = DataSource.get_metrics("authors", Mediawiki)
            items = metric.get_list()
            items['name'] = items.pop('authors')
        else:
            logging.error("Mediawiki " + filter_name + " not supported")
            return items

        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items = Mediawiki.get_filter_items(filter_, startdate, enddate, identities_db)
        if (items == None): return

    @staticmethod
    def create_filter_report_all(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        filter_name = filter_.get_name()
        if filter_name == "people2" or filter_name == "company":
            filter_all = Filter(filter_name, None)
            agg_all = Mediawiki.get_agg_data(period, startdate, enddate,
                                             identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_static_filename_all(Mediawiki()))
            createJSON(agg_all, fn)

            evol_all = Mediawiki.get_evolutionary_data(period, startdate, enddate,
                                                       identities_db, filter_all)
            fn = os.path.join(destdir, filter_.get_evolutionary_filename_all(Mediawiki()))
            createJSON(evol_all, fn)
        else:
            logging.error(Mediawiki.get_name()+ " " + filter_name +" does not support yet group by items sql queries")

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):

        top_data = Mediawiki.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['authors.']["id"]
        top += top_data['authors.last year']["id"]
        top += top_data['authors.last month']["id"]
        # remove duplicates
        people = list(set(top))
        return people

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        evol = GetEvolPeopleMediaWiki(upeople_id, period, startdate, enddate)
        evol = completePeriodIds(evol, period, startdate, enddate)
        return evol

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        return GetStaticPeopleMediaWiki(upeople_id, startdate, enddate)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_query_builder():
        from vizgrimoire.metrics.query_builder import MediawikiQuery
        return MediawikiQuery

    @staticmethod
    def get_metrics_core_agg():
        return ['reviews','authors','pages']

    @staticmethod
    def get_metrics_core_ts():
        return ['reviews','authors','pages']

    @staticmethod
    def get_metrics_core_trends():
        return ['reviews','authors']

# SQL Metaqueries

def GetTablesOwnUniqueIdsMediaWiki () :
    tables = 'wiki_pages_revs, people_upeople pup'
    return (tables)


def GetFiltersOwnUniqueIdsMediaWiki () :
    filters = 'pup.people_id = wiki_pages_revs.user'
    return (filters) 

#########
# PEOPLE
#########
def GetListPeopleMediaWiki (startdate, enddate) :
    fields = "DISTINCT(pup.upeople_id) as id, count(wiki_pages_revs.id) total"
    tables = GetTablesOwnUniqueIdsMediaWiki()
    filters = GetFiltersOwnUniqueIdsMediaWiki()
    filters += " GROUP BY user ORDER BY total desc"
    q = GetSQLGlobal('date',fields,tables, filters, startdate, enddate)

    data = ExecuteQuery(q)
    return (data)


def GetQueryPeopleMediaWiki (developer_id, period, startdate, enddate, evol) :
    fields = "COUNT(wiki_pages_revs.id) AS revisions"
    tables = GetTablesOwnUniqueIdsMediaWiki()
    filters = GetFiltersOwnUniqueIdsMediaWiki() + " AND pup.upeople_id = " + str(developer_id)

    if (evol) :
        q = GetSQLPeriod(period,'date', fields, tables, filters,
                startdate, enddate)
    else :
        fields += ",DATE_FORMAT (min(date),'%Y-%m-%d') as first_date, "+\
                  "DATE_FORMAT (max(date),'%Y-%m-%d') as last_date"
        q = GetSQLGlobal('date', fields, tables, filters,
                startdate, enddate)
    return (q)


def GetEvolPeopleMediaWiki (developer_id, period, startdate, enddate) :
    q = GetQueryPeopleMediaWiki(developer_id, period, startdate, enddate, True)

    data = ExecuteQuery(q)
    return (data)


def GetStaticPeopleMediaWiki (developer_id, startdate, enddate) :
    q = GetQueryPeopleMediaWiki(developer_id, None, startdate, enddate, False)

    data = ExecuteQuery(q)
    return (data)
