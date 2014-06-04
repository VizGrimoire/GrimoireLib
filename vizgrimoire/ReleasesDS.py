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
    def get_date_init():
        """Get the date of the first activity in the data source"""
        q = "SELECT MIN(created_on) AS date FROM projects"
        return(ExecuteQuery(q))

    @staticmethod
    def get_date_end():
        """Get the date of the last activity in the data source"""
        q1 = "SELECT MAX(updated_on) as ru, MAX(created_on) as rc FROM releases"
        q2 = "SELECT MAX(updated_on) as pu, MAX(created_on) as pr FROM projects"
        q = "SELECT GREATEST(ru, rc, pu, pr) AS date FROM (%s) r, (%s) p" % (q1, q2)
        return(ExecuteQuery(q))


    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, filter_ = None):
        data = {}

        type_analysis = None
        if filter_ is not None:
            type_analysis = [filter_.get_name(), filter_.get_item()]
            logging.warn("ReleasesDS does not support filters yet.")
            return data

        metrics_on = ['modules','authors','releases']
        filter_ = MetricFilters(period, startdate, enddate, type_analysis)
        all_metrics = ReleasesDS.get_metrics_set(ReleasesDS)

        for item in all_metrics:
            if item.id not in metrics_on: continue
            item.filters = filter_
            mvalue = item.get_ts()
            data = dict(data.items() + mvalue.items())

        return data
 
    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  ReleasesDS.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = ReleasesDS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        data = {}

        type_analysis = None
        if filter_ is not None:
            type_analysis = [filter_.get_name(), filter_.get_item()]
            logging.warn("ReleasesDS does not support filters yet.")
            return data

        filter_ = MetricFilters(period, startdate, enddate, type_analysis)

        metrics_on = ['modules','authors','releases']
        all_metrics = ReleasesDS.get_metrics_set(ReleasesDS)

        for item in all_metrics:
            if item.id not in metrics_on: continue
            item.filters = filter_
            mvalue = item.get_agg()
            data = dict(data.items() + mvalue.items())

        # Tendencies
        metrics_trends = ['modules','authors','releases']

        for i in [7,30,365]:
            for item in all_metrics:
                if item.id not in metrics_trends: continue
                period_data = item.get_agg_diff_days(enddate, i)
                data = dict(data.items() +  period_data.items())

        data["init_date"] = ReleasesDS.get_date_init()['date']
        data["last_date"] = ReleasesDS.get_date_end()['date']
        # data["url"] = ReleasesDS.get_url()

        return data

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data = ReleasesDS.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = ReleasesDS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_authors (days_period, startdate, enddate, identities_db, bots, npeople):
        # Unique identities not supported yet

        filter_bots = ''
        for bot in bots:
            filter_bots = filter_bots + " username<>'"+bot+"' and "
        # filter_bots = ''

        fields = "COUNT(r.id) as releases, username, u.id"
        tables = "users u, releases r, projects p"
        filters = filter_bots + "r.author_id = u.id AND r.project_id = p.id"
        if (days_period > 0):
            tables += ", (SELECT MAX(r.created_on) as last_date from releases r) t"
            filters += " AND DATEDIFF (last_date, r.created_on) < %s" % (days_period)
        filters += " GROUP by username"
        filters += " ORDER BY releases DESC, r.name"
        filters += " LIMIT %s" % (npeople)

        q = "SELECT %s FROM %s WHERE %s" % (fields, tables, filters)
        data = ExecuteQuery(q)
#        for id in data:
#            if not isinstance(data[id], (list)): data[id] = [data[id]]
        return(data)

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = ReleasesDS.get_bots()

        top_authors = {}
        top_authors['authors.'] = ReleasesDS.get_top_authors(0, startdate, enddate, identities_db, bots, npeople)
        top_authors['authors.last month']= ReleasesDS.get_top_authors(31, startdate, enddate, identities_db, bots, npeople)
        top_authors['authors.last year']= ReleasesDS.get_top_authors(365, startdate, enddate, identities_db, bots, npeople)

        return(top_authors)

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
    def _get_people_sql (developer_id, period, startdate, enddate, evol):
        fields = "COUNT(r.id) AS releases"
        tables = "users u, releases r"
        filters = " r.author_id = u.id AND u.id = '" + str(developer_id) + "'"
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
    def get_metrics_definition ():
        pass

    @staticmethod
    def get_query_builder ():
        from query_builder import ReleasesDSQuery
        return ReleasesDSQuery