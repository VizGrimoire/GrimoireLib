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
## Reporting of demographics-related entities (those related to the age
## of developers in the project, as time contributing to it).
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

# from os.path import join
# from jsonpickle import encode, set_encoder_options
# import codecs

from grimoirelib_alch.family.duration_persons import DurationPersons
from grimoirelib_alch.family.duration_persons import SnapshotCondition, ActiveCondition

def report_demographics (activity_persons, snapshot_date,
                         activity_period, prefix):

    """Produce a demographics (ages) report.

    Produces two JSON files, with information about the ages (time
    since "birth" in the project) for all contributors (birth) and
    for those still active (aging). those are named
    demographics-birth.json and demographics-aging.json.
    
    Parameters
    ----------

    activity_persons: activity_persons.ActivityPersons
       Periods of activity for developers
    snapshot_date: datetime.datetime
       Shanpshot date (when to count as "now")
    activity_period: datetime.timedelta
       Period for determine who is still active
       (somebody is active if activity is found between
       snapshot_date - activity_period and snapshot_date)
    prefix: str
       Prefix of the files to be produced

    Returns
    -------

    dictionary: dictionary for create_report
       Keys are the names of JSON files to produce, values are the
       data to include in those JSON files.

    """

    # Birth has the ages of all actors, considering enddate as
    # current (snapshot) time
    snapshot = SnapshotCondition (date = snapshot_date)
    birth = DurationPersons (datasource = activity_persons,
                             name = "age",
                             conditions = (snapshot,))
    active_period = ActiveCondition (after = snapshot_date - activity_period,
                                     before = snapshot_date)
    aging = DurationPersons (datasource = activity_persons,
                             name = "age",
                             conditions = (snapshot, active_period))
    report = {
        prefix + 'demographics-birth.json': birth.durations(),
        prefix + 'demographics-aging.json': aging.durations()
        }
    return report

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta
    from grimoirelib_alch.query.scm import DB as SCMDatabase
    from grimoirelib_alch.family.scm import NomergesCondition as SCMNomergesCondition
    from grimoirelib_alch.query.its import DB as ITSDatabase
    from grimoirelib_alch.query.mls import DB as MLSDatabase
    from grimoirelib_alch.family.activity_persons import (
        SCMActivityPersons,
        ITSActivityPersons,
        MLSActivityPersons
        )
    from grimoirelib_alch.aux.reports import create_report, add_report

    stdout_utf8()

    snapshot_date = datetime(2014,7,1)
    activity_period = timedelta(days=182)

    #---------------------------------
    print_banner("Demographics with MLS database, MediaWiki")
    
    database = MLSDatabase (url = "mysql://jgb:XXX@localhost/",
                            schema = "mls_wikimedia",
                            schema_id = "scm_wikimedia")
    # Wikimedia mailing lists don't always keep "arrival_date", therefore
    # we have to use "fisrt_date".
    activity = MLSActivityPersons (
        datasource = database,
        name = "list_usenders",
        date_kind = "first")
    report = report_demographics (activity_persons = activity,
                                  snapshot_date = snapshot_date,
                                  activity_period = activity_period,
                                  prefix = 'mls-'
                                  )
    create_report (report_files = report, destdir = '/tmp/')
