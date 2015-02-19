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
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>


# All of the functions found in this file expect to find a database
# with the followin format:
# Table: downloads
#       Fields: date (datetime), ip (varchar), package (varchar), protocol (varchar)
#       

import logging, os

from vizgrimoire.GrimoireSQL import ExecuteQuery, BuildQuery
from vizgrimoire.GrimoireUtils import completePeriodIds, createJSON

from vizgrimoire.data_source import DataSource

from vizgrimoire.metrics.metrics_filter import MetricFilters


class DownloadsDS(DataSource):
    _metrics_set = []

    @staticmethod
    def get_db_name():
        return "db_downloads"

    @staticmethod
    def get_name(): return "downloads"

    # TODO: convert to use get_metrics_data from data_source
    @staticmethod
    def _get_data (period, startdate, enddate, i_db, filter_, evol):
        data = {}

        type_analysis = None
        if (filter_ is not None):
            type_analysis = [filter_.get_name(), filter_.get_item()]
            logging.warn(DownloadsDS.get_name() + " does not support filters.")
            return data

        if (evol):
            metrics_on = DownloadsDS.get_metrics_core_ts()
        else:
            metrics_on = DownloadsDS.get_metrics_core_agg()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis)
        all_metrics = DownloadsDS.get_metrics_set(DownloadsDS)

        for item in all_metrics:
            if item.id not in metrics_on: continue
            item.filters = mfilter
            if evol is False:
                mvalue = item.get_agg()
            else:
                mvalue = item.get_ts()
            data = dict(data.items() + mvalue.items())

            if evol is False:
                init_date = DownloadsDS.get_date_init(startdate, enddate, None, type_analysis)
                end_date = DownloadsDS.get_date_end(startdate, enddate, None, type_analysis)

                data = dict(data.items() + init_date.items() + end_date.items())

                # Tendencies
                metrics_trends = DownloadsDS.get_metrics_core_trends()

                for i in [7,30,365]:
                    for item in all_metrics:
                        if item.id not in metrics_trends: continue
                        period_data = item.get_trends(enddate, i)
                        data = dict(data.items() +  period_data.items())

        if filter_ is not None: studies = {}
        else:
            studies = DataSource.get_studies_data(DownloadsDS, period, startdate, enddate, evol)
        return dict(data.items()+studies.items())

    @staticmethod
    def get_date_init(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        q = " SELECT DATE_FORMAT (MIN(date), '%Y-%m-%d') as first_date FROM downloads"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end(startdate = None, enddate = None, identities_db = None, type_analysis = None):
        q = " SELECT DATE_FORMAT (MAX(date), '%Y-%m-%d') as last_date FROM downloads"
        return(ExecuteQuery(q))


    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, filter_ = None):
        return DownloadsDS._get_data(period, startdate, enddate, i_db, filter_, True)

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data =  DownloadsDS.get_evolutionary_data (period, startdate, enddate, i_db, filter_)
        filename = DownloadsDS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, i_db, filter_ = None):
        return DownloadsDS._get_data(period, startdate, enddate, i_db, filter_, False)

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, filter_ = None):
        data = DownloadsDS.get_agg_data (period, startdate, enddate, i_db, filter_)
        filename = DownloadsDS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def create_filter_report_all(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        filter_name = filter_.get_name()
        logging.error(DownloadsDS.get_name()+ " " + filter_name +" does not support yet group by items sql queries")

    @staticmethod
    def get_top_metrics ():
        return ["ips","packages"]

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_ = None, npeople = None):

        def filter_ips(ips):
            new_ips = {}
            new_ips['downloads'] = ips['downloads']
            new_ips['ips'] = []
            for ip in ips['ips']:
                # ipv4
                new_ip_aux = ip.split(".")
                new_ip = ip
                if len(new_ip_aux) == 4:
                    new_ip = "x.x."+new_ip_aux[2]+"."+new_ip_aux[3]
                # ipv6
                new_ip_aux = ip.split(":")
                if len(new_ip_aux) > 1:
                    raise
                    new_ip = new_ip_aux[0]+":X"
                new_ips['ips'].append(new_ip)
            return new_ips

        top = {}
        mips = DataSource.get_metrics("ips", DownloadsDS)
        mpackages = DataSource.get_metrics("packages", DownloadsDS)
        period = None
        type_analysis = None
        if filter_ is not None:
            type_analysis = filter_.get_type_analysis()
        mfilter = MetricFilters(period, startdate, enddate, type_analysis, npeople)

        if filter_ is None:
            top['ips.'] = filter_ips(mips.get_list(mfilter, 0))
            top['packages.'] = mpackages.get_list(mfilter, 0)
        else:
            logging.info("DownloadsDS does not support yet top for filters.")

        return top


    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = DownloadsDS.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+DownloadsDS().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        logging.error("DownloadsDS " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items = DownloadsDS.get_filter_items(filter_, startdate, enddate, identities_db)
            if (items == None): return

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        pass

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        pass

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        pass

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        pass

    @staticmethod
    def get_query_builder ():
        from vizgrimoire.metrics.query_builder import DownloadsDSQuery
        return DownloadsDSQuery

    @staticmethod
    def get_metrics_core_agg():
        return ['downloads','packages','protocols','ips']

    @staticmethod
    def get_metrics_core_ts():
        return ['downloads','packages','protocols','ips']

    @staticmethod
    def get_metrics_core_trends():
        return ['downloads','packages','ips']
