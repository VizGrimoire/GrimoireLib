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
# Timezone analysis. Time zones for developers and contributions.
#

from grimoirelib_alch.family.scm import (
    PeriodCondition as SCMPeriodCondition,
    NomergesCondition as SCMNomergesCondition
    )
from grimoirelib_alch.query.scm import DB as SCMDatabase
from grimoirelib_alch.family.mls import PeriodCondition as MLSPeriodCondition
from grimoirelib_alch.query.mls import DB as MLSDatabase
from grimoirelib_alch.family.activity_timezones import (
    SCMActivityTZ, MLSActivityTZ
    )

from vizgrimoire.analysis.analyses import Analyses
from vizgrimoire.SCM import SCM
from vizgrimoire.MLS import MLS

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

class Timezone(Analyses):
    """Clase for calculating the Timezone analysis.

    Produces data about timezones for contributions.

    """

    id = "timezone"
    name = "Timezone"
    desc = "Timezone of contributions in project"

    def __get_sql__(self):
   
        raise NotImplementedError

    def result(self, data_source = None):
        """Produce result data for the analysis

        Parameters
        ----------

        data_source: SCM.SCM

        Returns
        -------

        dictionary: timezone data.
          It includes three components, with fillowing keys:
          "tz": list of timezones from -12 to 11,
          "commits": list of commits for each timezone
          "authors": list of authors for each timezone
                    
        """

        logging.info("Producing data for study: Timezone")
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
            logging.info("Analyzing timezone for SCM")
            period = SCMPeriodCondition (start = startdate, end = enddate)
            nomerges = SCMNomergesCondition()
            database = SCMDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = SCMActivityTZ (
                datasource = database,
                name = "authors",
                conditions = (period, nomerges))
        elif data_source == MLS:
            logging.info("Analyzing timezone for MLS")
            period = MLSPeriodCondition (start = startdate, end = enddate)
            database = MLSDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = MLSActivityTZ (
                datasource = database,
                name = "senders",
                conditions = (period,))
        if data_source in (SCM, MLS):
            timezones = data.timezones()
            return timezones
        else:
            logging.info("Error: data_source not supported!")
            return {}

    def create_report(self, data_source, destdir):
        """Create report for the analysis.

        Creates JSON files with the results of the report for timezone:
         ds-timezone.json
         with ds being "scm" or "its".
        Only works for SCM data sources.
        
        Parameters
        ----------

        data_source: SCM.SCM
        destdir: name of directory for writing JSON files

        """

        logging.info("Producing report for study: Timezone")
        if data_source == SCM:
            prefix = "scm"
        elif data_source == MLS:
            prefix = "mls"
        else:
            logging.info("Error: data_source not supported for Timezone")
            return
        result = self.result(data_source)
        produce_json (destdir + "/" + prefix + "-timezone.json",
                      result)
        logging.info("Producing report for study: Timezone (done!)")


if __name__ == '__main__':

    from vizgrimoire.metrics.query_builder import DSQuery
    from vizgrimoire.metrics.metrics_filter import MetricFilters

    # Get 
    filters = MetricFilters("months", "'2013-06-01'", "'2014-01-01'", [])
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "oscon_openstack_scm_tz",
                    identities_db = "oscon_openstack_scm_tz")
    tz = Timezone(dbcon, filters)

    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(tz.result(SCM), unpicklable=False)

    # Produce compact JSON output
    set_encoder_options('json', separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(tz.result(SCM), unpicklable=False)

    # Nos, MLS
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "oscon_openstack_mls",
                    identities_db = "oscon_openstack_scm")
    tz = Timezone(dbcon, filters)

    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(tz.result(MLS), unpicklable=False)
