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

from grimoirelib_alch.query.scm import DB as SCMDatabase
from grimoirelib_alch.family.scm import (
    NomergesCondition as SCMNomergesCondition,
    PeriodCondition as SCMPeriodCondition,
    OrgsCondition as SCMOrgsCondition
    )
from grimoirelib_alch.query.its import DB as ITSDatabase
from grimoirelib_alch.family.its import (
    PeriodCondition as ITSPeriodCondition,
    OrgsCondition as ITSOrgsCondition
    )
from grimoirelib_alch.query.mls import DB as MLSDatabase
from grimoirelib_alch.family.mls import (
    PeriodCondition as MLSPeriodCondition,
    OrgsCondition as MLSOrgsCondition
    )
from grimoirelib_alch.family.activity_persons import (
    SCMActivityPersons,
    ITSActivityPersons,
    MLSActivityPersons
    )
from grimoirelib_alch.family.duration_persons import (
    DurationPersons,
    SnapshotCondition,
    ActiveCondition
    )
from vizgrimoire.analysis.analyses import Analyses
from vizgrimoire.SCM import SCM
from vizgrimoire.ITS import ITS
from vizgrimoire.MLS import MLS

from datetime import datetime, timedelta
from jsonpickle import encode, set_encoder_options
import codecs
import logging
import os.path

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

def parse_analysis (type_analysis):
    """Parse a "type_analysis", returning a dictionary.

    Gets as parameter a "type_analysis", which is one of the
    filters that a metrics object can include, and produces
    a dictionary in it. "type_analysis" is a list with two
    elements, of the form ["company,repo", "'SwiftStack','onerepo'"].
    With it, produces a dictionary such as:
    {"company": "SwiftStack", "repo": "onerepo"}
    
    Parametrers
    -----------
    
    type_analysis: list of str
        type_analysis to parse
    
    Returns
    -------

    dict: dictionary
    
    """

    analysis_dict = {}
    if len(type_analysis) == 0: return analysis_dict
    analysis_list = type_analysis[0].split(",")
    values_list = type_analysis[1].split(",")
    for i, analysis in enumerate(analysis_list):
        value = values_list[i]
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        analysis_dict[analysis] = value
    return analysis_dict


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
        # Get dictionary with analysis, if any
        self.analysis_dict = parse_analysis (self.filters.type_analysis)
        if data_source == SCM:
            logging.info("Analyzing aging for SCM")
            # Activity data (start time, end time for contributions) for
            # all the actors, considering only activty during
            # the startdate..enddate period (merges are not considered
            # as activity)
            period = SCMPeriodCondition (start = startdate, end = enddate)
            nomerges = SCMNomergesCondition()
            conditions = [period, nomerges]
            if self.filters.COMPANY in self.analysis_dict:
                orgs = SCMOrgsCondition (
                    orgs = (self.analysis_dict[self.filters.COMPANY],),
                    actors = "authors")
                conditions.append(orgs)
            database = SCMDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = SCMActivityPersons (
                datasource = database,
                name = "list_uauthors",
                conditions = conditions)
        elif data_source == ITS:
            logging.info("Analyzing aging for ITS")
            # Activity data (start time, end time for contributions) for
            # all the actors, considering only activty during
            # the startdate..enddate period
            period = ITSPeriodCondition (start = startdate, end = enddate)
            conditions = [period,]
            if self.filters.COMPANY in self.analysis_dict:
                orgs = ITSOrgsCondition (
                    orgs = (self.analysis_dict[self.filters.COMPANY],)
                    )
                conditions.append(orgs)
            database = ITSDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = ITSActivityPersons (
                datasource = database,
                name = "list_uchangers",
                conditions = conditions)
        elif data_source == MLS:
            logging.info("Analyzing aging for MLS")
            # Activity data (start time, end time for contributions) for
            # all the actors, considering only activty during
            # the startdate..enddate period
            period = MLSPeriodCondition (start = startdate,
                                         end = enddate,
                                         date = "check")
            conditions = [period,]
            if self.filters.COMPANY in self.analysis_dict:
                orgs = MLSOrgsCondition (
                    orgs = (self.analysis_dict[self.filters.COMPANY],),
                    actors = "senders",
                    date = "check"
                    )
                conditions.append(orgs)
            database = MLSDatabase (url = url,
                                    schema = schema,
                                    schema_id = schema_id)
            data = MLSActivityPersons (
                datasource = database,
                name = "list_usenders",
                date_kind = "check",
                conditions = conditions)
        else:
            logging.info("Error: No aging analysis for this data source!")
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
                                                 timedelta(days=182),
                                             before = enddate)
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
        Only works for SCM, ITS, MLS data sources.
        
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
        log_message = "Producing report for study: Aging " + prefix + \
            " (done!)"
        if self.filters.COMPANY in self.analysis_dict:
            org = self.analysis_dict[self.filters.COMPANY]
            org = org.replace("/", "_")
            file_birth = os.path.join (
                destdir,
                org + "-" + prefix + "-com-demographics-birth.json"
                )
            file_aging = os.path.join (
                destdir,
                org + "-" + prefix + "-com-demographics-aging.json"
                )
            log_message = log_message + " Organization: " + org
        else:
            file_birth = os.path.join (destdir,
                                       prefix + "-demographics-birth.json")
            file_aging = os.path.join (destdir,
                                       prefix + "-demographics-aging.json")
        produce_json (file_birth, demos["birth"])
        produce_json (file_aging, demos["aging"])
        logging.info(log_message)


if __name__ == '__main__':

    from vizgrimoire.metrics.query_builder import DSQuery
    from vizgrimoire.metrics.metrics_filter import MetricFilters

    filters = MetricFilters("months", "'2013-12-01'", "'2014-01-01'", [])

    ##
    ## SCM
    ##

    # Warning: next "Red Hat" string will be changed to "'Red Hat'" by
    # the internals of Automator etc.
    filters_scm = filters
    filters_scm.add_filter (filters.COMPANY, "SwiftStack")
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "oscon_openstack_scm",
                    identities_db = "oscon_openstack_scm")
    ages = Ages(dbcon, filters_scm)

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

    ##
    ## ITS
    ##

    filters_its = filters
    filters_its.add_filter (filters_its.COMPANY, "company2")
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "cp_bicho_GrimoireLibTests",
                    identities_db = "cp_cvsanaly_GrimoireLibTests")
    ages = Ages(dbcon, filters_its)
    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(ITS), unpicklable=False)

    ##
    ## MLS
    ##

    filters_mls = filters
    filters_mls.add_filter (filters_mls.COMPANY, "company3")
    dbcon = DSQuery(user = "jgb", password = "XXX", 
                    database = "cp_mlstats_GrimoireLibTests",
                    identities_db = "cp_cvsanaly_GrimoireLibTests")
    ages = Ages(dbcon, filters_mls)
    # Produce pretty JSON output
    set_encoder_options('json', sort_keys=True, indent=4,
                        separators=(',', ': '),
                        ensure_ascii=False,
                        encoding="utf8")
    print encode(ages.result(MLS), unpicklable=False)
