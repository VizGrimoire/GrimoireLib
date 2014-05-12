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
# This file is a part of the vizGrimoire.R package
#
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>
#     Luis Cañas-Díaz <lcanas@bitergia.com>
#
#
# Usage:
#     PYTHONPATH=../vizgrimoire LANG= R_LIBS=../../r-lib ./qaforums-analysis.py 
#                                                -d acs_qaforums_automatortest_2388_2 -u root 
#                                                -i acs_cvsanaly_automatortest_2388 
#                                                -s 2010-01-01 -e 2014-01-20 
#                                                -o ../../../json -r people,repositories
#
# For migrating to Python3: z = dict(list(x.items()) + list(y.items()))


import logging
import sys

import GrimoireUtils, GrimoireSQL
from GrimoireUtils import dataFrame2Dict, createJSON, completePeriodIds
from GrimoireUtils import valRtoPython, getPeriod
import IRC
from QAForums import QAForums

from utils import read_options

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting QAForums data source analysis")
    opts = read_options()
    period = getPeriod(opts.granularity)
    reports = opts.reports.split(",")
    # filtered bots
    bots = ['wikibugs','gerrit-wm','wikibugs_','wm-bot','']
    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+opts.startdate+"'"
    enddate = "'"+opts.enddate+"'"

    GrimoireSQL.SetDBChannel(database=opts.dbname, user=opts.dbuser, password=opts.dbpassword)

    QAForums.create_agg_report(period, startdate, enddate, opts.identities_db, None)
    QAForums.create_evolutionary_report(period, startdate, enddate, opts.identities_db, None)
    QAForums.create_top_report(startdate, enddate, opts.destdir, opts.npeople, opts.identities_db)
    # if ('people' in reports):
    #     peopleData (period, startdate, enddate, opts.identities_db, opts.destdir, top)

    logging.info("Done QAForums data source analysis")
