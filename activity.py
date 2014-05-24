#! /usr/bin/python
# -*- coding: utf-8 -*-

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
## Classes related to activity, extracted from *Grimoire databases
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from datetime import datetime
from json import dumps

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
    return serial

class Period:
    """Abstract data type for activity periods.

    """

    def __init__ (self, start, end):
        """Intialize a TimeSeries object

        Parameters
        ----------

        start: datetime.datetime
           Start of the period
        end: datetime.datetime
           End of the period

        """

        self.start = start
        self.end = end

    def json (self, pretty=False):

        data = {"start": self.start,
                "end": self.end
                }
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

        repr = "Period, from %s to %s" % (self.start, self.end)
        return repr

if __name__ == "__main__":

    period = Period(datetime(2011,12,1), datetime(2012,11,1))
    print period
    print period.json(pretty=True)
