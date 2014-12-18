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


class MetricFilters(object):

    """ Specific filters for each analysis """
    def __init__(self, period, startdate, enddate, type_analysis, npeople=10,
                 people_out = None, companies_out = None, global_filter = None):
        self.period = period
        self.startdate = startdate
        self.enddate = enddate
        self.type_analysis = type_analysis
        self.npeople = npeople
        self.people_out = people_out
        self.companies_out = companies_out
        self.global_filter = global_filter

    def set_global_filter(self, value): self.global_filter = value

