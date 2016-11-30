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
##   Alvaro del Castillo <acs@bitergia.com>

""" Metrics for the issue tracking system """

import operator

from metrics.metrics import Metrics

class Opened(Metrics):
    """ Tickets Opened metric class for issue tracking systems """

    id = "opened"
    name = "Opened tickets"
    desc = "Number of opened tickets"
    FIELD_COUNT="id"
    FIELD_NAME="url"


class Openers(Metrics):
    """ Tickets Openers metric class for issue tracking systems """

    id = "openers"
    name = "Ticket submitters"
    desc = "Number of persons submitting new tickets"


class Closed(Metrics):
    """ Tickets Closed metric class for issue tracking systems
       "state": "closed" is the filter to be used in GitHub
    """
    id = "closed"
    name = "Closed tickets"
    desc = "Number of closed tickets"
    filters = {"state":"closed"}
    FIELD_COUNT="id"
    FIELD_NAME="url"


class TimeToClose(Metrics):
    """ Time to close since an issue is opened till this is close

    An issue is opened when this is submitted to the issue tracking system
    and this is closed once in the status field this is identified as closed.

    """

class Closers(Metrics):
    """ Tickets Closers metric class for issue tracking systems """
    id = "closers"
    name = "Tickets closers"
    desc = "Number of persons closing tickets"


class BMIIndex(Metrics):
    """ The Backlog Management Index measures efficiency dealing with tickets

        This is based on the book "Metrics and Models in Software Quality
        Engineering. Chapter 4.3.1. By Stephen H. Kan.

        BMI is calculated as the number of closed tickets out of the opened
        tickets in a given interval. This metric aims at having an overview of
        how the community deals with tickets. Continuous values under 1
        (or 100 if this is calculated as a percentage) shows low peformance
        given that the community leaves a lot of opened tickets. Continuous
        values close to 1 or over 1 shows a better performance. This would
        indicate that most of the tickets are being closed.
    """

    id = "bmitickets"
    name = "Backlog Management Index"
    desc = "Number of tickets closed out of the opened ones in a given interval"

    def __get_metrics(self):
        closed = Closed(self.es_url, self.es_index,
                        start=self.start, end=self.end,
                        esfilters=self.esfilters.copy(), interval=self.interval)
        opened = Opened(self.es_url, self.es_index,
                        start=self.start, end=self.end,
                        esfilters = self.esfilters.copy(),
                        interval=self.interval)

        return (closed, opened)

    def get_agg(self):
        (closed, opened) = self.__get_metrics()
        closed_agg = closed.get_agg()
        opened_agg = opened.get_agg()

        return closed_agg/opened_agg


    def get_ts(self):
        bmi = {}
        (closed, opened) = self.__get_metrics()
        closed_ts = closed.get_ts()
        opened_ts = opened.get_ts()

        bmi['date'] = closed_ts['date']
        bmi['unixtime'] = closed_ts['unixtime']
        bmi['value'] = list(map(operator.truediv, closed_ts['value'],
                                opened_ts['value']))
        return bmi



class ProjectsITS(Metrics):
    """ Projects metric class for issue tracking systems """
    id = "projects"
    name = "Projects"
    desc = "Number of distinct projects active in the ticketing system"
    FIELD_NAME = 'project' # field used to list projects
