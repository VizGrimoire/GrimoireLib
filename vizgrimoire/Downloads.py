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
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>


# All of the functions found in this file expect to find a database
# with the followin format:
# Table: downloads
#       Fields: date (datetime), ip (varchar), package (varchar), protocol (varchar)
#       

import logging, os

from GrimoireSQL import ExecuteQuery, BuildQuery
from GrimoireUtils import completePeriodIds, read_options, createJSON

from data_source import DataSource


class Downloads(DataSource):

    @staticmethod
    def get_db_name():
        return "db_downloads"

    @staticmethod
    def get_name(): return "downloads"

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, type_analysis = None):
        downloads = EvolDownloads(period, startdate, enddate)
        packages = EvolPackages(period, startdate, enddate)
        protocols = EvolProtocols(period, startdate, enddate)
        ips = EvolIPs(period, startdate, enddate)
        evol = dict(downloads.items() + packages.items() + protocols.items() + ips.items())
        evol = completePeriodIds(evol,  period, startdate, enddate)
        return evol

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, i_db, type_analysis = None):
        opts = read_options()
        data =  Downloads.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = Downloads().get_evolutionary_filename()
        createJSON (data, os.path.join(opts.destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        # Tendencies
        agg = {}

        if (filter_ is None):
            downloads = AggDownloads(period, startdate, enddate)
            packages = AggPackages(period, startdate, enddate)
            protocols = AggProtocols(period, startdate, enddate)
            ips = AggIPs(period, startdate, enddate)
            agg = dict(downloads.items() + packages.items() + protocols.items() + ips.items())
        else:
            logging.warn("Downloads does not support filters.")

        return agg

    @staticmethod
    def create_agg_report (period, startdate, enddate, i_db, type_analysis = None):
        opts = read_options()
        data = Downloads.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = Downloads().get_agg_filename()
        createJSON (data, os.path.join(opts.destdir, filename))

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_ = None, npeople = None):
        top20 = {}
        top20['ips.'] = TopIPs(startdate, enddate, 20)
        top20['packages.'] = TopPackages(startdate, enddate, 20)
        return top20


    @staticmethod
    def create_top_report (startdate, enddate, i_db):
        opts = read_options()
        data = Downloads.get_top_data (startdate, enddate, i_db, None, opts.npeople)
        top_file = opts.destdir+"/"+Downloads().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db, bots):
        items = None
        filter_name = filter_.get_name()

        logging.error("Downloads " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, startdate, enddate, identities_db, bots):
        # opts = read_options()
        # period = getPeriod(opts.granularity)

        items = Downloads.get_filter_items(filter_, startdate, enddate, identities_db, bots)
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
    def get_metrics_definition ():
        pass


def GetDownloads(period, startdate, enddate, evolutionary):
    # Generic function to obtain number of downloads 
    fields = "count(*) as downloads"
    tables = "downloads"
    filters = ""
   
    query = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(query))

def EvolDownloads(period, startdate, enddate):
    # Evolution of downloads
    return GetDownloads(period, startdate, enddate, True)

def AggDownloads(period, startdate, enddate):
    # Agg number of downloads
    return GetDownloads(period, startdate, enddate, False)

def GetPackages(period, startdate, enddate, evolutionary):
    # Generic function to obtain number of packages
    fields = "count(distinct(package)) as packages"
    tables = "downloads"
    filters = ""

    query = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(query))


def EvolPackages(period, startdate, enddate):
    # Evolution of different packages per period
    return GetPackages(period, startdate, enddate, True)

def AggPackages(period, startdate, enddate):
    # Agg number of packages in a given period
    return GetPackages(period, startdate, enddate, False)

def GetProtocols(period, startdate, enddate, evolutionary):
    # Generic function to obtain number of protocols
    fields = "count(distinct(protocol)) as protocols"
    tables = "downloads"
    filters = ""

    query = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(query))


def EvolProtocols(period, startdate, enddate):
    # Evolution of different protocols per period
    return GetProtocols(period, startdate, enddate, True)

def AggProtocols(period, startdate, enddate):
    # Agg number of protocols in a given period
    return GetProtocols(period, startdate, enddate, False)

def GetIPs(period, startdate, enddate, evolutionary):
    # Generic function to obtain number of IPs
    fields = "count(distinct(ip)) as ips"
    tables = "downloads"
    filters = ""

    query = BuildQuery(period, startdate, enddate, " date ", fields, tables, filters, evolutionary)
    return(ExecuteQuery(query))


def EvolIPs(period, startdate, enddate):
    # Evolution of different IPs per period
    return GetIPs(period, startdate, enddate, True)

def AggIPs(period, startdate, enddate):
    # Agg number of IPs in a given period
    return GetIPs(period, startdate, enddate, False)


def TopIPs(startdate, enddate, numTop):
    # Top IPs downloading packages in a given period
    query = """
            select ip as ips, count(*) as downloads 
            from downloads
            where date >= %s and
                  date < %s
            group by ips
            order by downloads desc
            limit %s
            """ % (startdate, enddate, str(numTop))
    return ExecuteQuery(query)

def TopPackages(startdate, enddate, numTop):
    # Top Packages bein downloaded in a given period
    query = """
            select package as packages, count(*) as downloads
            from downloads
            where date >= %s and
                  date < %s
            group by packages
            order by downloads desc
            limit %s
            """ % (startdate, enddate, str(numTop))
    return ExecuteQuery(query)