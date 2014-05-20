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
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
## MediaWiki.R
##
## Queries for software Releases data analysis.
## First use in Puppet forge analysis.
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

import logging, os

from GrimoireSQL import GetSQLGlobal, GetSQLPeriod, ExecuteQuery, BuildQuery
from GrimoireUtils import GetPercentageDiff, GetDates, completePeriodIds, createJSON

from data_source import DataSource


class Releases(DataSource):

    @staticmethod
    def get_db_name():
        return "db_releases"

    @staticmethod
    def get_name(): return "releases"

    #
    # Metrics: modules, releases and authors
    #

    @staticmethod
    def get_modules(period, startdate, enddate, evol = False):
        fields = "COUNT(*) AS modules"
        tables = "projects p"
        filters = ""
        if evol:
            q = GetSQLPeriod(period,'p.created_on', fields, tables, filters,
                             startdate, enddate)
        else:
            q = GetSQLGlobal('p.created_on', fields, tables, filters,
                             startdate, enddate)
        return(ExecuteQuery(q))

    @staticmethod
    def get_releases(period, startdate, enddate, evol = False):
        fields = "COUNT(DISTINCT(r.id)) AS releases"
        tables = "releases r, projects p"
        filters = "r.project_id = p.id"
        if evol:
            q = GetSQLPeriod(period,'r.created_on', fields, tables, filters,
                             startdate, enddate)
        else:
            q = GetSQLGlobal('r.created_on', fields, tables, filters,
                             startdate, enddate)
        return(ExecuteQuery(q))

    @staticmethod
    def get_authors(period, startdate, enddate, evol = False):
        fields = "COUNT(DISTINCT(u.id)) AS authors"
        tables = "users u, releases r, projects p"
        filters = "r.author_id = u.id AND r.project_id = p.id"
        if evol:
            q = GetSQLPeriod(period,'r.created_on', fields, tables, filters,
                             startdate, enddate)
        else:
            q = GetSQLGlobal('r.created_on', fields, tables, filters,
                             startdate, enddate)
        return(ExecuteQuery(q))

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, type_analysis = None):
        evol = {}

        data = Releases.get_authors(period, startdate, enddate, True)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = Releases.get_modules(period, startdate, enddate, True)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())
        data = Releases.get_releases(period, startdate, enddate, True)
        evol = dict(evol.items() + completePeriodIds(data, period, startdate, enddate).items())

        return evol
 
    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  Releases.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = Releases().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_modules_days(period, enddate, identities_db, i):
        return {}

    @staticmethod
    def get_authors_days(period, enddate, identities_db, i):
        return {}

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        # Tendencies
        agg = {}

        if (filter_ is None):
            for i in [7,30,365]:
                data = Releases.get_modules_days(period, enddate, identities_db, i)
                agg = dict(agg.items() + data.items())
                data = Releases.get_authors_days(period, enddate, identities_db, i)
                agg = dict(agg.items() + data.items())
            data = Releases.get_authors(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = Releases.get_modules(period, startdate, enddate)
            agg = dict(agg.items() + data.items())
            data = Releases.get_releases(period, startdate, enddate)
            agg = dict(agg.items() + data.items())

        else:
            logging.warn("Releases does not support filters yet.")
        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data = Releases.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = Releases().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_top_authors (days_period, startdate, enddate, identities_db, bots, npeople):

        # Unique identities not supported yet
        fields = "COUNT(r.id) as releases, username"
        tables = "users u, releases r, projects p"
        filters = "r.author_id = u.id AND r.project_id = p.id"
        if (days_period > 0):
            tables += ", (SELECT MAX(r.created_on) as last_date from releases r) t"
            filters += " AND DATEDIFF (last_date, r.created_on) < %s" % (days_period)
        filters += " GROUP by username"
        filters += " ORDER BY releases DESC, r.name"
        filters += " LIMIT %s" % (npeople)

        q = "SELECT %s FROM %s WHERE %s" % (fields, tables, filters)
        return(ExecuteQuery(q))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        bots = Releases.get_bots()

        top_authors = {}
        top_authors['authors.'] = Releases.get_top_authors(0, startdate, enddate, identities_db, bots, npeople)
        top_authors['authors.last month']= Releases.get_top_authors(31, startdate, enddate, identities_db, bots, npeople)
        top_authors['authors.last year']= Releases.get_top_authors(365, startdate, enddate, identities_db, bots, npeople)

        return(top_authors)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = Releases.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+Releases().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        logging.error("Releases " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db, bots):
        items = Releases.get_filter_items(filter_, startdate, enddate, identities_db, bots)
        if (items == None): return

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):

        top_data = Releases.get_top_data (startdate, enddate, identities_db, None, npeople)

        return []

        top = top_data['authors.']["id"]
        top += top_data['authors.last year']["id"]
        top += top_data['authors.last month']["id"]
        # remove duplicates
        people = list(set(top))
        return people

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
    def get_metrics_definition ():
        pass