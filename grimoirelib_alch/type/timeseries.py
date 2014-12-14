#! /usr/bin/python

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
## TimeSeries class, to hold time series related to *Grimoire
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from datetime import datetime
from dateutil.relativedelta import relativedelta
from json import dumps

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
    return serial

class TimeSeries:
    """Abstract data type for time series.

    Internally, a time series is a data structure with:

    - start: starting date for the time series (datetime)
    - end: end date for the time series (datetime)
    - period: sampling period (string)
    - data: list of tuples, each tuple being:
       - time for the beginning of the period (datetime)
       - string for representing the name of the period (datetime)
       - tuple with the values for that period
       The index in data is the period number, starting with 0.
    """

    def _min_date (self, data):
        """Calculate min date for all items in data

        - data list of tuples, each tuple of the form
            (date, values).
        """

        min = data[0][0]
        for (date, value) in data:
            if date < min:
                min = date
        return min

    def _max_date (self, data):
        """Calculate maximum date for all items in data

        Parameters
        ----------

        data: list of tuples
           Each tuple is of the form (date, values).

        """

        max = data[0][0]
        for (date, value) in data:
            if date > max:
                max = date
        return max

    def _period (self, date):
        """Get period id (from self.start) corresponding to a date

        period ids are integers, starting at 0
        """

        return date.year * 12 + date.month - \
            self.start.year * 12 - self.start.month

    def _normalize (self, data, novalue = None):
        """Normalize data intended to store as data in the class.

        - data: list of tuples, each tuple of the form
            (date, values), being values also a tuple of values
        - novalue: value to use for tuples with no value

        Produces data suitable for self.data, with
        tuples for those periods with no tuples,
        and sorts the result in ascending time order.
        """

        periods = self._period(self.end) + 1
        # Fill in result to return with dates and novalue
        result = [(self.start + relativedelta(months=period), novalue)
                  for period in xrange(periods)]
        # Fill in the values for the dates received in data
        for (date, value) in data:
            period = self._period(date)
            if result [period] == novalue:
                raise Exception("Dup period")
            else:
                result [period] = (result[period][0], value)
        return result

    def json (self, pretty=False):

        data = {"period": self.period,
                "first_date": self.start,
                "last_date": self.end,
                "values": self.data}
        separators=(',', ': ')
        encoding="utf-8"
        if pretty:
            sort_keys=True
            indent=4
        else:
            sort_keys=False
            indent=None
        return dumps(data, sort_keys=sort_keys,
                     indent=indent, separators=separators,
                     default=json_serial, encoding=encoding)

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__ (self):

        repr = "TimeSeries object (%s) " % (self.period)
        repr += "from %s to %s\n" % (self.start, self.end)
        repr += "data:\n"
        for item in self.data:
            repr += " %s: %s\n" % item
        return repr

    def __init__ (self, period, start, end, data, zerovalue = 0L):
        """Intialize a TimeSeries object
        
        - period: period for the time series (for now, "months")
        - start: starting time for the time series
        - end: ending time for the time series
        - data: list of tuples, each tuple of the form
            (date, values), being values also a tuple of values
        - zerovalue: value to use as "zero" for novalue tuples
            (those correspnding to periods without a value)

        start and/or end could be None
        """

        self.period = period
        if start is None:
            self.start = self._min_date(data)
        else:
            self.start = start
        if end is None:
            self.end = self._max_date(data)
        else:
            self.end = end
        # Use tuple of 0s for no values, same length as tuples in data
        novalue = (zerovalue,) * len (data[0][1])
        self.data = self._normalize(data, novalue)

if __name__ == "__main__":

    data = [(datetime(2011,12,1), (1, 2, 3)),
            (datetime(2012,1,1), (4, 5, 6)),
            (datetime(2012,2,1), (7, 8, 9)),
            (datetime(2012,3,1), (10, 11, 12)),
            (datetime(2012,4,1), (13, 14, 15)),
            (datetime(2012,5,1), (16, 17, 18)),
            (datetime(2014,6,1), (19, 20, 21))]
    start = None
    end = datetime(2014,8,1)
    ts = TimeSeries ("months", start=start, end=end, data=data, zerovalue=0)
    print ts
    print ts.json(pretty=True)
