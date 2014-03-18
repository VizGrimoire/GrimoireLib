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


class DataSource(object):
    _bots = []

    @staticmethod
    def get_bots():
        return DataSource._bots

    @staticmethod
    def set_bots(ds_bots):
        DataSource._bots = ds_bots

    @staticmethod
    def get_name():
        raise NotImplementedError

    # Automatoc config name for the data source database
    @staticmethod
    def get_db_name(self):
        raise NotImplementedError

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, type_analysis):
        raise NotImplementedError

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, identities_db, type_analysis):
        raise NotImplementedError

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, type_analysis):
        raise NotImplementedError

    @staticmethod
    def create_agg_report (period, startdate, enddate, identities_db, type_analysis):
        raise NotImplementedError

    @staticmethod
    def get_top_data (period, startdate, enddate, identities_db, npeople):
        raise NotImplementedError

    @staticmethod
    def create_top_report (period, startdate, enddate, identities_db):
        raise NotImplementedError

    @staticmethod
    def get_filter_items(period, startdate, enddate, identities_db, filter_):
        raise NotImplementedError

    @staticmethod
    def create_filter_report(filter_, startdate, enddate, identities_db, bots):
        raise NotImplementedError

    @staticmethod
    def create_people_report(period, startdate, enddate, identities_db):
        raise NotImplementedError

    @staticmethod
    def create_r_reports(vizr, enddate):
        raise NotImplementedError