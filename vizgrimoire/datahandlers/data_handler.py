# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details. 
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# This file is a part of GrimoireLib
#  (an Python library for the MetricsGrimoire and vizGrimoire systems)
#
#
# Authors:
#   Daniel Izquierdo <dizquierdo@bitergia.com>
#

import numpy as np
from scipy import stats


class DataHandler(object):
    """Root class for the hierarchy of data handler

    A data handler is defined as a structure that contains specific fields
    of information.
   
    As an example, a timeseries is a data handler with specific fields such as
    unixtime, dataset, labels and other fields.

    """

    def __init__(self, dataset, filters = None):
        """Initial instantiation of a DataHandler object
        
        Parameters
        ----------

        dataset: list of elements
        filters: MetricFilters object

        """

        raise Exception("__init__ should be provided by child class")


class DHESA(DataHandler):
    """Data Handler Early Statistical Approach

    This class manages a dataset, typically a list of elements, and provides
    an initial Early Statistical Approach.

    This returns a list of analysis for the list of elements.

    Analysis performed: min value, max value, mean value, median value, std value,
                        mode value, first quartile, third quartile, 

    """
    
    def __init__(self, dataset, filters = None):
        """DHESA builder class

        Parameters
        ----------

        dataset: list of elements
        filters: MetricFilters object

        """
        
        self.filters = filters
        self.data = {}

        if not isinstance(dataset, list):
            raise Exception("__init__ dataset should be a list")
        if len(dataset) == 0:
            self.data["median"] = 0
            self.data["mean"] = 0
            self.data["mode"] = 0
            self.data["min"] = 0
            self.data["max"] = 0
            self.data["percentile25"] = 0
            self.data["percentile75"] = 0
        else:
            self.data["median"] = np.median(dataset)
            self.data["mean"] = np.mean(dataset)
            self.data["mode"] = stats.mode(dataset)
            self.data["min"] = np.min(dataset)
            self.data["max"] = np.max(dataset)
            self.data["percentile25"] = np.percentile(dataset, 25)
            self.data["percentile75"] = np.percentile(dataset, 75)


if __name__ == '__main__':
    data = DHESA([1,2,3,4])
    print data.data

