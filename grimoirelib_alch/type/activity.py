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

from datetime import datetime, timedelta
from sqlalchemy.util import KeyedTuple
import jsonpickle

class DatetimeHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        return obj.isoformat()

class TimedeltaHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        return obj.days

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

        Attributes
        ----------

        Same as parameters

        """

        self.start = start
        self.end = end

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__ (self):

        repr = "Period, from %s to %s" % (self.start, self.end)
        return repr


class ActorsDuration:
    """Duration for actors.

    List of actors, with duration information (timedelta)
    for each of them.

    """

    def __init__ (self, list = [], date = None):
        """Intialize ActorsDuration object
        
        Parameters
        ----------

        list: list of dictionaries
           Each dictionary includes
           information about an actor, with the following fields:
           id (integer), name (string), duration (datetime.timedelta)
        date: datetime.datetime
           Date for which the durations were calculated.
        """

        if len(list) > 0:
            self.durations = [key for key in list[0].keys()
                              if key not in ["id", "name"]]
        else:
            self.durations = []
        self.list = [actor for actor in list]
        self.date = date

    def __repr__ (self):

        repr = "ActorsDuration:\n"
        for item in self.list:
            repr = repr + str(item) + "\n"
        return repr

    def __getstate__(self):
        """Return the state to be used for pickling.

        Needed by jsonpickle, which will use this to produce
        the JSON string.

        """

        # Get data in long format
        long = self.long_format()
        return {"date": self.date,
                "persons": long
                }

    def __setstate__(self, state):
        """Set the state from pckling.

        Not really used, just needed by jsonpickle.

        """

        self.list = state

    def json (self):
        """Produce a JSON string from the object."""

        return jsonpickle.encode(self, unpicklable=False)

    def long_format (self):
        """Get long version of the object.

        The object is stored in wide format (that is, a list with
        one dictionary per entry, with components in the
        dictionary being variables for the entry). This function
        produces a long format (that is, a dictionry of lists,
        with each compoenent in the dictionary being the list
        of values for a variable).
        
        """

        long = {}
        if len(self.list) > 0:
            for key in self.list[0].keys():
                long[key] = [actor[key] for actor in self.list]
        return long


class ActivityList:
    """Activity lists.

    List of actors, with activity information (start and end dates)
    for each of them.
    """

    def __init__ (self, list = []):
        """Intialize ActivityList object

        Parameters
        ----------

        list: list of tuples
           List of activity tuples. Each tuple includes
           information about an actor, with the
           following fields: person_id (integer), name (string),
           firstdate (datetime), lastdate (datetime).

        """

        self.list = []
        for entry in list:
            item = {}
            item["id"] =  entry.person_id
            item["name"] = entry.name
            item["period"]  = Period(start = entry.firstdate,
                                     end = entry.lastdate)
            self.list.append(item)

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

    def __getstate__(self):
        """Return the state to be used for pickling.

        Needed by jsonpickle, which will use this to produce
        the JSON string.

        """

        return self.list

    def __setstate__(self, state):
        """Set the state from pckling.

        Not really used, just needed by jsonpickle.

        """

        self.list = state

    def json (self):
        """Produce a JSON string from the object."""

        return jsonpickle.encode(self, unpicklable=False)

    def maxend (self):
        """Obtain the maximum end date for all the periods.

        Returns
        -------

        datetime.datetime: maximum end date

        """

        maxend = self.list[0]["period"].end
        for actor in self.list:
            if maxend < actor["period"].end:
                maxend = actor["period"].end
        return maxend

    def active (self, after = None, before = None):
        """Get an ActivityList object with thse active between dates.

        Active actors are those with activity in the period
        after .. before.

        Parameters
        ----------

        after: datetime.datetime
           Start of the activity period to consider (default: None).
           None means "since the begining of time"
        before: datetime.datetime
           End of the activity period to consider (default: None).
           None means "until the end of time

        Returns
        -------

        ActivityList

        """

        active = ActivityList()
        active.list = [actor for actor in self.list
                       if (after == None or
                           actor["period"].end >= after) and
                          (before == None or
                           actor["period"].start <= before)]
        return active


    def age (self, date, offset = timedelta(0)):
        """Get age (in days) for each actor, relative to date.

        The age for each actor is the difference between date and their
        first activity. Age is positive for actors who were already
        active before date, and negative for actors active only after
        date.

        Parameters
        ----------

        date: datetime.datetime
           shanpshot date to calculate ages
        offset: datetime.timedelta
           Delta to add to each age. This is useful for considering
           actors of age 0 as of age offset

        Returns
        -------

        ActorsDuration: activity duration for each actor

        """

        ages = [{"id": actor["id"],
                 "name": actor["name"],
                 "age": date - actor["period"].start + offset}
                for actor in self.list]
        return ActorsDuration(ages, date)

    def idle (self, date, offset = timedelta(0)):
        """Get idle (in days) for each actor with activity before date.

        The idle period for each actor is the difference between date
        and their last activity, if last activity is before date, 0
        otherwise.

        Parameters
        ----------

        date: datetime.datetime
           shanpshot date to calculate ages
        offset: datetime.timedelta
           Delta to add to each period. This is useful for considering
           actors of age 0 as of age offset

        Returns
        -------

        ActorsDuration: idle duration for each actor

        """

        list = []
        for actor in self.list:
            id = actor["id"]
            name = actor["name"]
            if actor["period"].end >= date:
                idle = 0
            else:
                idle = date - actor["period"].end + offset
            list.append ({"id": id,
                          "name": name,
                          "age": idle}
                         )
        return ActorsDuration(list, date)


def init_json():
    """Initialize JSON encoder.

    """

    # Register datetime flattener for jsonpickle
    jsonpickle.handlers.registry.register(datetime, DatetimeHandler)
    jsonpickle.handlers.registry.register(timedelta, TimedeltaHandler)
    # Select json module
    jsonpickle.set_preferred_backend('json')
    # Opetions for producing nice JSON
    jsonpickle.set_encoder_options('json', sort_keys=True, indent=4,
                                   separators=(',', ': '),
                                   ensure_ascii=False,
                                   encoding="utf8")

# Ensure JSON encoder is properly intialized
init_json()

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()

    #---------------------------------
    print_banner("Period (str, json)")
    period = Period(datetime(2011,12,1), datetime(2012,11,1))
    print period
    print jsonpickle.encode(period, unpicklable=False)

    #---------------------------------
    print_banner("ActivityList (str, json)")
    rowlabels = ["person_id", "name", "firstdate", "lastdate"]
    list = ActivityList((KeyedTuple([12, "Fulano Larguiño",
                                     datetime(2011,12,1),
                                     datetime(2012,11,1)],
                                    labels = rowlabels),
                         KeyedTuple([3, "Mengana Corta",
                                     datetime(2010,2,3),
                                     datetime(2013,2,3)],
                                    labels = rowlabels)))
    print list
    print list.json()

    #---------------------------------
    print_banner("ActivityList.age() for 2013-01-01 (str, json)")
    age = list.age(datetime(2013,1,1))
    print age
    print age.json()

    #---------------------------------
    print_banner("ActivityList.age() for 2012-01-01 (str, json)")
    age = list.age(datetime(2012,1,1))
    print age.json()

    #---------------------------------
    print_banner("ActivityList.idle() for 2013-01-01 (str, json)")
    idle = list.idle(datetime(2013,1,1))
    print idle.json()

    #---------------------------------
    print_banner("ActivityList.idle() for 2012-01-01 (json)")
    idle = list.idle(datetime(2012,1,1))
    print idle.json()

    #---------------------------------
    print_banner("ActivityList.active() after 2012-11-02 (json)")
    filtered = list.active (after=datetime(2012,11,2))
    print filtered.json()
