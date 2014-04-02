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
##   Alvaro del Castillo <acs@bitergia.com>


from GrimoireSQL import SetDBChannel
from GrimoireUtils import read_options, read_main_conf
import logging
import SCM, ITS, MLS, SCR, Mediawiki, IRC
from filter import Filter

class Report(object):

    _filters = []
    _all_data_sources = []

    @staticmethod
    def init():
        Report._init_filters()
        Report._init_data_sources()

    @staticmethod
    def _init_filters():
        opts = read_options()
        automator = read_main_conf(opts.config_file)
        reports = automator['r']['reports']
        # Hack because we use repos in filters
        reports = reports.replace("repositories","repos")
        filters = reports.split(",")
        # people not a filter yet
        if 'people' in filters: filters.remove('people')
        for name in filters:
            filter_ = Filter.get_filter_from_plural(name)
            if filter_ is not None:
                Report._filters.append(filter_)
            else:
                logging.error("Wrong filter " + name + ", review " + opts.config_file)

    @staticmethod
    def _init_data_sources():
        Report._all_data_sources = [SCM.SCM, ITS.ITS, MLS.MLS, SCR.SCR, 
                                    Mediawiki.Mediawiki, IRC.IRC]

    @staticmethod
    def get_config():
        opts = read_options()

        # opts.config_file = "../../../conf/main.conf"
        automator = read_main_conf(opts.config_file)

        return automator

    @staticmethod
    def connect_ds(ds):
        opts = read_options()
        automator = Report.get_config()
        db = automator['generic'][ds.get_db_name()]
        SetDBChannel (database=db, user=opts.dbuser, password=opts.dbpassword)

    @staticmethod
    def get_data_sources():
        automator = Report.get_config() 
        data_sources= []

        for ds in Report._all_data_sources:
            if not ds.get_db_name() in automator['generic']: continue
            else: data_sources.append(ds)
        return data_sources

    @staticmethod
    def set_data_sources(dss):
        Report._all_data_sources = dss 

    @staticmethod
    def get_filters():
        return Report._filters

    @staticmethod
    def set_filters(filters):
        Report._filters = filters 
