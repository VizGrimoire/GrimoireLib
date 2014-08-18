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

from os.path import join
from jsonpickle import encode, set_encoder_options
import codecs

def produce_json (filename, data, compact = True):
    """Produce JSON content (data) into a file (filename).

    Parameters
    ----------

    filename: string
       Name of file to write the content.
    data: any
       Content to write in JSON format. It has to be ready to pack using
       jsonpickle.encode.

    """

    if compact:
        # Produce compact JSON output
        set_encoder_options('json', separators=(',', ': '),
                            ensure_ascii=False,
                            encoding="utf8")
    else:
        # Produce pretty JSON output
        set_encoder_options('json', sort_keys=True, indent=4,
                            separators=(',', ': '),
                            ensure_ascii=False,
                            encoding="utf8")
    data_json = encode(data, unpicklable=False)
    with codecs.open(filename, "w", "utf-8") as file:
        file.write(data_json)


def create_report (report_files, destdir):
    """Create report, by producing a collection of JSON files

    Parameters
    ----------

    report_files: dictionary
       Keys are the names of JSON files to produce, values are the
       data to include in those JSON files.
    destdir: str
       Name of the destination directory to write all JSON files

    """

    for file in report_files:
        print "Producing file: ", join (destdir, file)
        produce_json (join (destdir, file), report_files[file])

def add_report (report, to_add):
    """Add new files (with their data) to report.

    Adds new_files (which is a dictorionay in report format)
    to report, and returns the result.

    Parameters
    ----------

    report: dictionary
       Base report Keys are the names of JSON files to produce, values are the
       data to include in those JSON files.
    to_add: dictionary
       Report to add. Same format as report. Keys in to_add will
       replace the same keys in report, or just be added to it.
    """

    for file in to_add:
        report[file] = to_add[file]
    return report


from duration_persons import DurationPersons
from duration_persons import SnapshotCondition, ActiveCondition

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
    active_period = ActiveCondition (after = snapshot_date - activity_period)
    aging = DurationPersons (datasource = activity_persons,
                             name = "age",
                             conditions = (snapshot, active_period))
    report = {
        prefix + 'demographics-birth.json': birth.durations(),
        prefix + 'demographics-aging.json': aging.durations()
        }

    return report

if __name__ == "__main__":

    from standalone import stdout_utf8, print_banner
    from datetime import datetime, timedelta
    from scm import SCMDatabaseDefinition
    from its import ITSDatabaseDefinition
    from mls import MLSDatabaseDefinition
    from activity_persons import SCMActivityPersons,  ITSActivityPersons, \
        MLSActivityPersons

    stdout_utf8()

    snapshot_date = datetime(2014,7,1)
    activity_period = timedelta(days=182)

    #---------------------------------
    print_banner("Demographics with SCM database, OpenStack")
    
    database = SCMDatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
                                      schema = "oscon_openstack_scm",
                                      schema_id = "oscon_openstack_scm")
    activity = SCMActivityPersons (
        datasource = database,
        name = "list_uauthors")

    report = report_demographics (activity_persons = activity,
                                  snapshot_date = snapshot_date,
                                  activity_period = activity_period,
                                  prefix = 'scm-')
    
    #---------------------------------
    print_banner("Demographics with ITS database, OpenStack")
    
    database = ITSDatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
                                      schema = "oscon_openstack_its",
                                      schema_id = "oscon_openstack_scm")
    activity = ITSActivityPersons (
        datasource = database,
        name = "list_uchangers")

    report = add_report (report,
                         report_demographics (activity_persons = activity,
                                              snapshot_date = snapshot_date,
                                              activity_period = activity_period,
                                              prefix = 'its-')
                         )

    #---------------------------------
    print_banner("Demographics with MLS database, OpenStack")
    
    database = MLSDatabaseDefinition (url = "mysql://jgb:XXX@localhost/",
                                      schema = "oscon_openstack_mls",
                                      schema_id = "oscon_openstack_scm")
    activity = MLSActivityPersons (
        datasource = database,
        name = "list_usenders")

    report = add_report (report,
                         report_demographics (activity_persons = activity,
                                              snapshot_date = snapshot_date,
                                              activity_period = activity_period,
                                              prefix = 'mls-')
                         )

    create_report (report_files = report, destdir = '/tmp/')
