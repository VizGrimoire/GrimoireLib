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
from sqlalchemy.util import KeyedTuple

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
    if isinstance(obj, Period):
        serial = obj.json()
    return serial

class Period:
    """Abstract data type for activity periods.

    """

    def __init__ (self, start, end):
        """Intialize a Period object

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

class ActivityList:
    """Abstract data type for activity lists.

    List of agents, with activity information (start and end dates)
    for each of them.
    """

    def __init__ (self, list):
        """Intialize an ActivityList object

        Parameters
        ----------

        list: list of tuples
           List of activity tuples. Each activity tuple includes
           information about an actor, and a Period.
        """

        self.list = []
        for entry in list:
            actor = {"id": entry.id,
                     "name": entry.name}
            period = Period(start = entry.firstdate,
                            end = entry.lastdate)
            self.list.append([actor, period])

    def json (self, pretty=False):

        separators=(',', ': ')
        encoding="utf-8"
        if pretty:
            sort_keys=True
            indent=4
        else:
            sort_keys=False
            indent=None
        return dumps(self.list, sort_keys=sort_keys,
                     indent=indent, separators=separators,
                     default=json_serial, encoding=encoding)

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__ (self):

        repr = "ActivityList:\n"
        for item in self.list:
            repr = repr + str(item) + "\n"
        return repr

if __name__ == "__main__":

    period = Period(datetime(2011,12,1), datetime(2012,11,1))
    print period
    print period.json(pretty=True)

    rowlabels = ["id", "name", "firstdate", "lastdate"]
    list = ActivityList((KeyedTuple([12, "Fulano Largo",
                                     datetime(2011,12,1),
                                     datetime(2012,11,1)],
                                    labels = rowlabels),
                         KeyedTuple([3, "Mengana Corta",
                                     datetime(2010,2,3),
                                     datetime(2013,2,3)],
                                    labels = rowlabels)))
    print list
    print list.json(pretty=True)
