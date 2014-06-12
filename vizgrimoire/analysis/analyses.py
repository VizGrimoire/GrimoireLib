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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>


class Analyses(object):

    id = None
    name = None
    desc = None

    def __init__(self, dbcon = None, filters = None):
        """db connection and filter to be used"""
        self.db = dbcon
        self.filters = filters


    def get_definition(self):
        def_ = {
               "id":self.id,
               "name":self.name,
               "desc":self.desc
        }
        return def_

    def get_ts (self, data_source):
        """ Returns time series data for a data source. """
        return {}

    def get_agg (self, data_source):
        """ Returns aggregated data for a data source. """
        return {}

    def create_report (self, destdir):
        """ Create a report in destdir for the study. """
        return None

    def __get_sql__(self, evolutionary):
        """ Returns specific sql for the provided filters """
        raise NotImplementedError

    def result(self):
        """ Returns final result """
        raise NotImplementedError
