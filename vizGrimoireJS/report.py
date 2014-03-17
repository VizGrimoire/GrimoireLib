#!/usr/bin/env python

# Copyright (C) 2012, 2013 Bitergia
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
# This file is a part of the vizGrimoire.R package
#
# Authors:
#   Alvaro del Castillo <acs@bitergia.com>
#

import logging
import sys

from ConfigParser import SafeConfigParser

import GrimoireUtils, GrimoireSQL
from GrimoireUtils import createJSON, completePeriodIds
from GrimoireUtils import read_options, getPeriod
from SCM import GetPeopleListSCM
from data_source import DataSource

def read_main_conf(config_file):
    options = {}
    parser = SafeConfigParser()
    fd = open(config_file, 'r')
    parser.readfp(fd)
    fd.close()

    sec = parser.sections()
    # we'll read "generic" for db information and "r" for start_date
    for s in sec:
        if not((s == "generic") or (s == "r")):
            continue
        options[s] = {}
        opti = parser.options(s)
        for o in opti:
            options[s][o] = parser.get(s, o)
    return options

def create_evol_report(startdate, enddate, identities_db, bots, type_analysis, its_backend):
    # TODO: DRY this options
    opts = read_options()
    opts.config_file = "../../../conf/main.conf"
    automator = read_main_conf(opts.config_file)

    import SCM, ITS, MLS, SCR, Mediawiki, IRC
    from ITS import Backend
    backend = Backend(its_backend)
    data_sources = [SCM.SCM, ITS.ITS, MLS.MLS, SCR.SCR, Mediawiki.Mediawiki, IRC.IRC]

    for ds in data_sources:
        if not ds.get_db_name() in automator['generic']: continue
        db = automator['generic'][ds.get_db_name()]
        GrimoireSQL.SetDBChannel (database=db, user=opts.dbuser, password=opts.dbpassword)
        if (ds == ITS.ITS):
            ds.get_evolutionary_data (period, startdate, enddate, identities_db, type_analysis, backend.closed_condition)
        else:
            ds.get_evolutionary_data (period, startdate, enddate, identities_db, type_analysis)



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting Report analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")
    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+opts.startdate+"'"
    enddate = "'"+opts.enddate+"'"

    # Working at the same time with VizR and VizPy yet
    # vizr.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)
    GrimoireSQL.SetDBChannel (database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)

    opts.config_file = "../../../conf/main.conf"
    automator = read_main_conf(opts.config_file)
    create_evol_report(startdate, enddate, opts.identities_db, [], [], automator['generic']['bicho_backend'])

    logging.info("Report data source analysis OK")