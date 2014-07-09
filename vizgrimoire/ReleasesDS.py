## Copyright (C) 2014 Bitergia
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
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

import logging, os

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod, ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, createJSON

from data_source import DataSource

from metrics_filter import MetricFilters



class ReleasesDS(DataSource):
    """Data source representing project releases"""
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_releases"

    @staticmethod
    def get_name(): return "releases"

    @staticmethod
    def get_date_init(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        """Get the date of the first activity in the data source"""
        q = "SELECT DATE_FORMAT (MIN(created_on), '%Y-%m-%d') AS first_date FROM projects"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        """Get the date of the last activity in the data source"""
        q1 = "SELECT MAX(updated_on) as ru, MAX(created_on) as rc FROM releases"
        q2 = "SELECT MAX(updated_on) as pu, MAX(created_on) as pr FROM projects"
        q  = "SELECT DATE_FORMAT (last_date,'%Y-%m-%d') as last_date FROM " 
        q += "(SELECT GREATEST(ru, rc, pu, pr) AS last_date FROM (%s) r, (%s) p) t" % (q1, q2)
        return(ExecuteQuery(q))

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, filter_ = None):
        metrics =  DataSource.get_metrics_data(ReleasesDS, period, startdate, enddate, i_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_studies_data(ReleasesDS, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())
 
    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  ReleasesDS.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = ReleasesDS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, i_db, filter_ = None):
        metrics =  DataSource.get_metrics_data(ReleasesDS, period, startdate, enddate, i_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_metrics_data(ReleasesDS, period, startdate, enddate, False)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data = ReleasesDS.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = ReleasesDS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        top = {}
        mauthors = DataSource.get_metrics("authors", ReleasesDS)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top['authors.'] = mauthors.get_list(mfilter, 0)
            top['authors.last month']= mauthors.get_list(mfilter, 31)
            top['authors.last year']= mauthors.get_list(mfilter, 365)
        else:
            logging.info("ReleasesDS does not support yet top for filters.")

        return(top)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = ReleasesDS.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+ReleasesDS().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        logging.error("ReleasesDS " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items = ReleasesDS.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):

        top_data = ReleasesDS.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['authors.']["id"]
        top += top_data['authors.last year']["id"]
        top += top_data['authors.last month']["id"]
        # remove duplicates
        people = list(set(top))
        return people

    @staticmethod
    def _get_people_sql (upeople_id, period, startdate, enddate, evol):
        fields = "COUNT(r.id) AS releases"
        tables = "users u, releases r, people_upeople pup"
        filters = "pup.people_id = u.id AND r.author_id = u.id AND pup.upeople_id = '" + str(upeople_id) + "'"
        q = BuildQuery (period, startdate, enddate, 'r.created_on', fields, tables, filters, evol)
        return (q)

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        q = ReleasesDS._get_people_sql (upeople_id, period, startdate, enddate, True)
        return completePeriodIds(ExecuteQuery(q), period, startdate, enddate)

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        q = ReleasesDS._get_people_sql (upeople_id, None, startdate, enddate, False)
        return ExecuteQuery(q)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_query_builder ():
        from query_builder import ReleasesDSQuery
        return ReleasesDSQuery

    @staticmethod
    def get_metrics_core_agg():
        return ['modules','authors','releases']

    @staticmethod
    def get_metrics_core_ts():
        return ['modules','authors','releases']

    @staticmethod
    def get_metrics_core_trends():
        return ['modules','authors','releases']
