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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

""" DataSource offers the API to get aggregated, evolutionary and top data with filter 
    support for Grimoire supported data sources """ 

import os
from GrimoireUtils import read_options, createJSON

class DataSource(object):
    _bots = []

    @staticmethod
    def get_name():
        """Get the name"""
        raise NotImplementedError

    @staticmethod
    def get_bots():
        """Get the bots to be filtered"""
        return DataSource._bots

    @staticmethod
    def set_bots(ds_bots):
        """Set the bots to be filtered"""
        DataSource._bots = ds_bots

    @staticmethod
    def get_db_name(self):
        """Get the name of the database with the data"""
        raise NotImplementedError

    def get_evolutionary_filename (self, filter_ = None):
        """Get the filename used to store evolutionary data"""
        name = None
        if (filter_ is None):
            name = self.get_name()+"-evolutionary.json"
        else:
            name = filter_.get_evolutionary_filename(self)
        return name

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, identities_db, filter_ = None):
        """Get the evolutionary data"""
        raise NotImplementedError

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, identities_db, filter_ = None):
        """Create the evolutionary data report"""
        raise NotImplementedError

    def get_agg_filename (self, filter_ = None):
        """Get the filename used to store aggregated data"""
        name = None
        if (filter_ is None):
            name = self.get_name()+"-static.json"
        else:
            name = filter_.get_static_filename(self)
        return name

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        """Get the aggregated data"""
        raise NotImplementedError

    @staticmethod
    def create_agg_report (period, startdate, enddate, identities_db, type_analysis = None):
        """Create the aggregated report"""
        raise NotImplementedError

    def get_top_filename (self, filter_ = None):
        """Get the filename used to store top data"""
        name = None
        if filter_ is None:
            name = self.get_name()+"-top.json"
        else:
            name = filter_.get_top_filename(self)
        return name

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        """Get the top data"""
        raise NotImplementedError

    @staticmethod
    def create_top_report (startdate, enddate, identities_db):
        """Create the top data report"""
        raise NotImplementedError

    @staticmethod
    def get_filter_items(filter_, period, startdate, enddate, identities_db, bots = None):
        """Get the items in the data source available for the filter"""
        raise NotImplementedError

    @staticmethod
    def get_filter_summary(filter_, period, startdate, enddate, identities_db, limit):
        """Get items with a summary for the data source available for the filter"""
        raise NotImplementedError

    @staticmethod
    def create_filter_report(filter_, startdate, enddate, identities_db, bots):
        """Create all files related to all filters in all data sources"""
        raise NotImplementedError

    @staticmethod
    def get_top_people_file(ds):
        """Get the filename used to store top people data"""
        return ds+"-people.json"

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        """Get top people data"""
        raise NotImplementedError

    def get_person_evol_file(self, upeople_id):
        """Get the filename used to store evolutionary data for a person activity"""
        ds = self.get_name()
        name = "people-"+str(upeople_id)+"-"+ds+"-evolutionary.json"
        return name

    @staticmethod
    def get_person_evol(upeople_id, period, startdate, enddate, identities_db, type_analysis):
        """Get the evolutionary data for a person activity"""
        raise NotImplementedError

    def get_person_agg_file(self, upeople_id):
        """Get the filename used to store aggregated data for a person activity"""
        ds = self.get_name()
        name = "people-"+str(upeople_id)+"-"+ds+"-static.json"
        return name

    @staticmethod
    def get_person_agg(upeople_id, startdate, enddate, identities_db, type_analysis):
        """Get aggregated data for a person activity"""
        raise NotImplementedError

    def create_people_report(self, period, startdate, enddate, identities_db):
        """Create all files related to people activity (aggregated, evolutionary)"""
        opts = read_options()
        fpeople = os.path.join(opts.destdir,self.get_top_people_file(self.get_name()))
        people = self.get_top_people(startdate, enddate, identities_db, opts.npeople)
        if people is None: return
        createJSON(people, fpeople)

        for upeople_id in people :
            evol_data = self.get_person_evol(upeople_id, period, startdate, enddate,
                                            identities_db, type_analysis = None)
            fperson = os.path.join(opts.destdir,self.get_person_evol_file(upeople_id))
            createJSON (evol_data, fperson)

            agg = self.get_person_agg(upeople_id, startdate, enddate,
                                     identities_db, type_analysis = None)
            fperson = os.path.join(opts.destdir,self.get_person_agg_file(upeople_id))
            createJSON (agg, fperson)

    @staticmethod
    def create_r_reports(vizr, enddate, destdir):
        """Create all files related to R reports"""
        raise NotImplementedError

    @staticmethod
    def get_metrics_definition ():
        """Return all metrics definition available"""
        raise NotImplementedError