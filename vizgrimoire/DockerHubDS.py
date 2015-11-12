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

from vizgrimoire.GrimoireSQL import GetSQLGlobal, GetSQLPeriod, ExecuteQuery, BuildQuery
from vizgrimoire.GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, createJSON

from vizgrimoire.data_source import DataSource

from vizgrimoire.metrics.metrics_filter import MetricFilters



class DockerHubDS(DataSource):
    """Data source representing Docker repositories"""
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_dockerhub"

    @staticmethod
    def get_name(): return "dockerhub"

    @staticmethod
    def get_date_init(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        """Get the date of the first activity in the data source"""
        #q = "SELECT DATE_FORMAT (MIN(created_on), '%Y-%m-%d') AS first_date FROM projects"
        q = "SELECT DATE_FORMAT (MIN(date), '%Y-%m-%d') AS first_date FROM repositories_log"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        """Get the date of the last activity in the data source"""
        q  = "SELECT DATE_FORMAT (MAX(date),'%Y-%m-%d') as last_date FROM repositories_log"
        return(ExecuteQuery(q))

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, filter_ = None):
        metrics =  DockerHubDS.get_metrics_data(period, startdate, enddate, i_db, filter_, True)
        if filter_ is not None:
            studies = {}
        else:
            studies =  DataSource.get_studies_data(DockerHubDS, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  DockerHubDS.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = DockerHubDS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, i_db, filter_ = None):
        metrics =  DockerHubDS.get_metrics_data(period, startdate, enddate, i_db, filter_, False)
        if filter_ is not None:
            studies = {}
        else:
            studies =  DockerHubDS.get_metrics_data(period, startdate, enddate, False)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data = DockerHubDS.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = DockerHubDS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def create_filter_report_all(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        filter_name = filter_.get_name()
        logging.error(DockerHubDS.get_name()+ " " + filter_name +" does not support yet group by items sql queries")

    @staticmethod
    def get_top_metrics ():
        return ["pulls"]

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        top = {}
        mauthors = DataSource.get_metrics("pulls", DockerHubDS)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top['pulls.'] = mauthors.get_list(mfilter, 0)
        else:
            logging.info("DockerHubDS does not support yet top for filters.")

        return(top)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = DockerHubDS.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+DockerHubDS().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        logging.error("DockerHubDS " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items = DockerHubDS.get_filter_items(filter_, startdate, enddate, identities_db)
            if (items == None): return

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        pass

    @staticmethod
    def _get_people_sql (uuid, period, startdate, enddate, evol):
        pass

    @staticmethod
    def get_person_evol(uuid, period, startdate, enddate, identities_db, type_analysis):
        pass

    @staticmethod
    def get_person_agg(uuid, startdate, enddate, identities_db, type_analysis):
        pass

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_query_builder ():
        from vizgrimoire.metrics.query_builder import DockerHubDSQuery
        return DockerHubDSQuery

    @staticmethod
    def get_metrics_core_agg():
        return ['pulls','starred','downloads','forks']

    @staticmethod
    def get_metrics_core_ts():
        return ['pulls','starred','downloads','forks']

    @staticmethod
    def get_metrics_core_trends():
        return ['pulls','starred','downloads','forks']
