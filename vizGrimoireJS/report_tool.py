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


from GrimoireUtils import createJSON
from GrimoireUtils import read_options, getPeriod, read_main_conf
from report import Report

def get_evol_report(startdate, enddate, identities_db, bots, type_analysis):
    all_ds = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        all_ds[ds.get_name()] = ds.get_evolutionary_data (period, startdate, enddate, identities_db, type_analysis)
    return all_ds

def get_agg_report(startdate, enddate, identities_db, bots, type_analysis):
    all_ds = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        all_ds[ds.get_name()] = ds.get_agg_data (period, startdate, enddate, identities_db, type_analysis)
    return all_ds

def create_reports(startdate, enddate, identities_db, bots):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        logging.info("Creating reports for " + ds.get_name())
        for filter_ in Report.get_filters():
            logging.info("-> " + filter_.get_name())
            ds.create_filter_report(filter_, startdate, enddate, identities_db, bots)

if __name__ == '__main__':

    Report.init()

    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting Report analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")
    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+opts.startdate+"'"
    enddate = "'"+opts.enddate+"'"

    opts.config_file = "../../../conf/main.conf"
    automator = read_main_conf(opts.config_file)
    evol = get_evol_report(startdate, enddate, opts.identities_db, [], [])
    agg = get_agg_report(startdate, enddate, opts.identities_db, [], [])
    create_reports(startdate, enddate, opts.identities_db, [])

    logging.info("Report data source analysis OK")