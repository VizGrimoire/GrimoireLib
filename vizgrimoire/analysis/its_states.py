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
#    Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#    Daniel Izquierdo <dizquierdo@bitergia.com>
#    Alvaro del Castillo <acs@bitergia.com>
#    Luis Cañas-Díaz <lcanas@bitergia.com>
#    Santiago Dueñas <sduenas@bitergia.com>
#

from analyses import Analyses

from GrimoireUtils import completePeriodIds


class TicketsStates(Analyses):
    """Analysis of issues states"""

    id = "tickets_states"
    name = "Tickets states"
    desc = "Analysis of issues states"

    def __get_sql_issues_states__(self, backend_type):
        """Returns the log of states"""

        if backend_type == "lp": backend_type = "launchpad" # openstack

        q = """SELECT issue_id, status, UNIX_TIMESTAMP(date) udate
               FROM issues_log_%s
               WHERE date >= %s AND date < %s
               ORDER BY udate""" % (backend_type, self.filters.startdate, self.filters.enddate)
        return q

    def __get_sql_current__(self, state, evolutionary):
        """This function returns the evolution or agg number of issues state"""

        fields = " count(distinct(id)) as `current_" + state + "` "
        tables = " issues i "
        filters = " status = '" + state + "' "

        q = self.db.BuildQuery(self.filters.period, self.filters.startdate,
                               self.filters.enddate, " i.submitted_on ",
                               fields, tables, filters, evolutionary)
        return q

    def __get_sql_state_types__(self, backend_type):
        """This function returns the list of states available on the database"""

        if backend_type == "lp": backend_type = "launchpad" # openstack

        q = """SELECT DISTINCT(status) status
               FROM issues_log_%s""" % backend_type
        return q

    def get_backlog(self, states, backend_type):
        import datetime
        import time

        def update_backlog_count(current_count, backlog_count):
            for state, count in current_count.items():
                backlog_count[state].append(count)

        # Dict to store the results
        data = {self.filters.period : [self.filters.startdate, self.filters.enddate]}
        data = completePeriodIds(data, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)

        tickets_states = {}
        current_status = {}

        # Initialize structures
        for state in states:
            current_status[state] = 0
            data[state] = []

        # Request issues log
        query = self.__get_sql_issues_states__(backend_type)
        issues_log = self.db.ExecuteQuery(query)

        periods = list(data['unixtime'][1:])

        # Add a one period more to avoid problems with
        # data from this period
        last_date = int(time.mktime(datetime.datetime.strptime(
                        self.filters.enddate, "'%Y-%m-%d'").timetuple()))
        periods.append(last_date)

        end_period = int(periods.pop(0))

        for i in range(len(issues_log['issue_id'])):
            issue_id = issues_log['issue_id'][i]
            issue_state = issues_log['status'][i]
            issue_date = int(issues_log['udate'][i])

            # Fill periods without changes on issues states
            while issue_date >= end_period:
                end_period = int(periods.pop(0))
                update_backlog_count(current_status, data)

            if issue_id not in tickets_states:
                # Add ticket to the status dict
                tickets_states[issue_id] = issue_state
            elif tickets_states[issue_id] != issue_state:
                # Decrease the count of tickets with the old state
                # only for predefined states
                old_state = tickets_states[issue_id]
                if old_state in states:
                    current_status[old_state] -= 1

                # Set new status
                tickets_states[issue_id] = issue_state
            else:
                continue # Ignore equal states

            # Increase the count of tickets with issue_state
            # only for predefined states
            if issue_state in states:
                current_status[issue_state] += 1

        # End of the loop. Add the last period values to the backlog count
        update_backlog_count(current_status, data)

        # Fill remaining periods without changes on issues states
        while periods:
            periods.pop(0)
            for state in states:
                data[state].append(data[state][-1])

        return data


    def get_current_states(self, states):
        current_states = {}

        for state in states:
            query = self.__get_sql_current__(state, True)
            data = self.db.ExecuteQuery(query)
            data = completePeriodIds(data, self.filters.period,
                                     self.filters.startdate, self.filters.enddate)
            current_states = dict(current_states.items() + data.items())

        return current_states

    def get_state_types(self, backend_type):
        query = self.__get_sql_state_types__(backend_type)
        result = self.db.ExecuteQuery(query)

        return [state  for state in result['status']]

    def get_ts (self, data_source = None):
        from ITS import ITS
        if data_source is not None and data_source != ITS: return {}
        return self.result()

    def result(self, data_source = None):
        # FIXME: this import is needed to get the list of
        # states available on the tracker. This should be moved
        # to configuration file to let the user choose among states.
        from ITS import ITS
        if data_source is not None and data_source != ITS: return None
        backend = ITS._get_backend()

        if backend.its_type == 'bg':
            backend_type = 'bugzilla'
        else:
            backend_type = backend.its_type

        states = self.get_state_types(backend_type)

        backlog = self.get_backlog(states, backend_type)
        current_states = self.get_current_states(states)
        return dict(backlog.items() + current_states.items())
