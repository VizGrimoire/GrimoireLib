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
##  (a Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>


class Analyses(object):
    """Root class for GrimoireLib analysis.

    An analysis, in this context, is the production of values for
    several entities that are related (usually, a family of entities).

    """

    id = None
    name = None
    desc = None

    def __init__(self, dbcon = None, filters = None):
        """Intialization of the object.

        Parameters
        ----------

        dbcon: query_builder.DSQuery
           Connection to the database
        filters: metrics_filter.MetricsFilters
           Fiters to be used

        """

        self.db = dbcon
        self.filters = filters


    def get_definition(self):
        """Get the identifier, name and description of entity.

        Returns
        -------

        dictionary: fields "id", "name" and "desc".

        """

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

    def create_report (self, data_source, destdir):
        """ Create a report in destdir for the study. """
        return None

    def __get_sql__(self, evolutionary):
        """ Returns specific sql for the provided filters

        Parameters
        ----------

        evolutionary: Boolean
           We want the "evolutionary" (time series) version of the entity.

        """

        raise NotImplementedError

    def result(self):
        """ Returns the value for the entity.

        """

        raise NotImplementedError

    def get_report_files(self, destdir):
        """ Returns a list with the files created for the report. """
        return []