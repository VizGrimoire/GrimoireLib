#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
# Authors:
#       Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
#
# Demography analysis. Age of developers in the project, age of
# developers still with activity, and so on.

from grimoirelib_alch.scm_query import DB as SCMDatabase
from grimoirelib_alch.scm import NomergesCondition as SCMNomergesCondition
from grimoirelib_alch.scm import PeriodCondition as SCMPeriodCondition
from grimoirelib_alch.its_query import DB as ITSDatabase
from grimoirelib_alch.its import PeriodCondition as ITSPeriodCondition
from grimoirelib_alch.mls_query import DB as MLSDatabase
from grimoirelib_alch.mls import PeriodCondition as MLSPeriodCondition
from grimoirelib_alch.activity_persons import SCMActivityPersons
from grimoirelib_alch.activity_persons import ITSActivityPersons
from grimoirelib_alch.activity_persons import MLSActivityPersons
from grimoirelib_alch.duration_persons import DurationPersons
from grimoirelib_alch.duration_persons import SnapshotCondition
from grimoirelib_alch.duration_persons import ActiveCondition

from analyses import Analyses
from SCM import SCM
from ITS import ITS
from MLS import MLS

from datetime import datetime, timedelta
from jsonpickle import encode, set_encoder_options
import codecs
import logging

def produce_json (filename, data, compact = True):

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

class Ages(Analyses):
    """Clase for calculating the Ages analysis.

    Produces data about aging and birth in a project, suitable to
    produce aging dempgraphic pyramids.

    """

    id = "ages"
    name = "Ages"
    desc = "Age of developers in project"

    def __get_sql__(self):
   
        raise NotImplementedError

    def result(self, data_source = None):
        """Produce result data for the analysis

        Parameters
        ----------

        data_source: { SCM.SCM | ITS.ITS | MLS.MLS }

        Returns
        -------

        dictionary: birth and aging data.
          The dictionary has two entries, keyed "birth" and "data".
          For each of them, information about the duration of all actors
          in the project is included.

        """

        logging.info("Producing data for study: Aging")
        if data_source is None:
            logging.info("Error: no data source for study!")
            return
        # Prepare the SQLAlchemy database url
        url = 'mysql://' + self.db.user + ':' + \
            self.db.password + '@' + self.db.host + '/'
        schema = self.db.database
        schema_id = self.db.identities_db
        # Get startdate, endate as datetime objects
        startdate = datetime.strptime(self.filters.startdate, "'%Y-%m-%d'")
        enddate = datetime.strptime(self.filters.enddate, "'%Y-%m-%d'")
        if data_source == SCM:
            logging.info("Analyzing aging for SCM")
            # Activity data (start time, end time for contributions) for
            # all the actors, considering only activty during
            # the startdate..enddate period (merges are not considered
            # as activity)
            period = SCMPeriodCondition (start = startdate, end = enddate)
            nomerges = SCMNomergesCondition()
            database = SCMDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = SCMActivityPersons (
                datasource = database,
                name = "list_uauthors",
                conditions = (period, nomerges))
        elif data_source == ITS:
            logging.info("Analyzing aging for ITS")
            # Activity data (start time, end time for contributions) for
            # all the actors, considering only activty during
            # the startdate..enddate period
            period = ITSPeriodCondition (start = startdate, end = enddate)
            database = ITSDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = ITSActivityPersons (
                datasource = database,
                name = "list_uchangers",
                conditions = (period,))
        elif data_source == MLS:
            logging.info("Analyzing aging for MLS")
            # Activity data (start time, end time for contributions) for
            # all the actors, considering only activty during
            # the startdate..enddate period
            period = MLSPeriodCondition (start = startdate,
                                          end = enddate)
            database = MLSDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = MLSActivityPersons (
                datasource = database,
                name = "list_usenders",
                conditions = (period,))
        else:
            logging.info("Error: No aging analysys for this data source!")
        if data_source in (SCM, ITS, MLS):
            # Birth has the ages of all actors, consiering enddate as
            # current (snapshot) time
            snapshot = SnapshotCondition (date = enddate)
            birth = DurationPersons (datasource = data,
                                     name = "age",
                                     conditions = (snapshot,),
                                     )
            # "Aging" has the ages of those actors active during the 
            # last half year (that is, the period from enddate - half year
            # to enddate)
            active_period = ActiveCondition (after = enddate - \
                                                 timedelta(days=182))
            aging = DurationPersons (datasource = data,
                                     name = "age",
                                     conditions = (snapshot, active_period),
                                     )
            demos = {"birth": birth.durations(),
                     "aging": aging.durations()}
            return demos
        else:
            return {"birth": {},
                    "aging": {}}


    def create_report(self, data_source, destdir):
        """Create report for the analysis.

        Creates JSON files with the results of the report for birth and aging:
         ds-demographics-birth.json, ds-demographics-aging.json,
         with ds being "scm" or "its".
        Only works for SCM or ITS data sources.
        
        Parameters
        ----------

        data_source: { SCM.SCM | ITS.ITS | MLS.MLS }
        destdir: name of directory for writing JSON files

        """

        logging.info("Producing report for study: Aging")
        if data_source == SCM:
            prefix = "scm"
        elif data_source == ITS:
            prefix = "its"
        elif data_source == MLS:
            prefix = "mls"
        else:
            logging.info("Error: data_source not supported for Aging")
            return
        demos = self.result(data_source)
        produce_json (destdir + "/" + prefix + "-demographics-birth.json",
                      demos["birth"])
        produce_json (destdir + "/" + prefix + "-demographics-aging.json",
                      demos["aging"])
        logging.info("Producing report for study: Aging (done!)")


if __name__ == '__main__':

    from query_builder import DSQuery
    from metrics_filter import MetricFilters

    # Get 
    filters = MetricFilters("months", "'2013-06-01'", "'2014-01-01'", [])
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "openstack_cvsanaly_2014-06-06",
                    identities_db = "openstack_cvsanaly_2014-06-06")
    ages = Ages(dbcon, filters)

    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(SCM), unpicklable=False)

    # Produce compact JSON output
    set_encoder_options('json', separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(SCM), unpicklable=False)

    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "vizgrimoire_bicho",
                    identities_db = "vizgrimoire_cvsanaly")
    ages = Ages(dbcon, filters)
    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(ITS), unpicklable=False)

    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "oscon_openstack_mls",
                    identities_db = "oscon_openstack_scm")
    ages = Ages(dbcon, filters)
    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(MLS), unpicklable=False)
