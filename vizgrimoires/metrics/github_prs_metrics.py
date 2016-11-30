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
##   Daniel Izquierdo <dizquierdo@bitergia.com>

import operator

from metrics.metrics import Metrics


class SubmittedPR(Metrics):
    id = "submitted"
    name = "Submitted reviews"
    desc = "Number of submitted code review processes"
    FIELD_NAME = 'id'
    FIELD_COUNT = 'id'
    filters = {"pull_request":"true"}


class ClosedPR(Metrics):
    id = "closed"
    name = "Closed reviews"
    desc = "Number of closed review processes (merged or abandoned)"
    FIELD_NAME = 'id'
    FIELD_COUNT = 'id'
    filters = {"pull_request":"true", "state":"closed"}

class DaysToClosePR(Metrics):
    id = "days_to_close_pr"
    name = "Days to close reviews (median)"
    desc = "Number of days needed to close a review (median)"
    FIELD_COUNT = 'time_to_close_days'
    AGG_TYPE = 'median'
    filters = {"pull_request":"true", "state":"closed"}

class ProjectsPR(Metrics):
    """ Projects in the review code management system """

    id = "projects"
    name = "Projects"
    desc = "Projects in the review code management system"
    FIELD_NAME = 'project' # field used to list projects

class BMIPR(Metrics):
    """This class calculates the efficiency closing reviews

    It is calculated as the number of closed issues out of the total number
    of opened ones in a period.
    """

    id = "bmipr"
    name = "BMI Pull Requests"
    desc = "Efficiency reviewing: (closed prs)/(submitted prs)"

    def __get_metrics(self):
        esfilters = None
        if self.esfilters:
            esfilters = self.esfilters.copy()

        closed = ClosedPR(self.es_url, self.es_index,
                          start=self.start, end=self.end,
                          esfilters=esfilters, interval=self.interval)
        submitted = SubmittedPR(self.es_url, self.es_index,
                                start=self.start, end=self.end,
                                esfilters = esfilters,
                                interval=self.interval)

        return (closed, submitted)

    def get_agg(self):
        (closed, submitted) = self.__get_metrics()
        closed_agg = closed.get_agg()
        submitted_agg = submitted.get_agg()

        return closed_agg/submitted_agg


    def get_ts(self):
        bmi = {}
        (closed, submitted) = self.__get_metrics()
        closed_ts = closed.get_ts()
        submitted_ts = submitted.get_ts()

        bmi['date'] = closed_ts['date']
        bmi['unixtime'] = closed_ts['unixtime']
        bmi['value'] = list(map(operator.truediv, closed_ts['value'],
                                submitted_ts['value']))

        return bmi


class Reviewers(Metrics):
    """ People assigned to pull requests """
    id = "reviewers"
    name = "Reviewers"
    desc = "Number of persons reviewing code review activities"

class Closers(Metrics):
    id = "closers"
    name = "Closers"
    desc = "Number of persons closing code review activities"

# Pretty similar to ITS openers
class Submitters(Metrics):
    id = "submitters"
    name = "Submitters"
    desc = "Number of persons submitting code review processes"
