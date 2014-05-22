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

""" Tool for metrics management """

import logging
from optparse import OptionParser

from commit_metric import Commit
from metric import Metric
from SCM import SCM
from report import Report

def get_options():
    parser = OptionParser(usage='Usage: %prog [options]',
                          description='Tool for metrics management',
                          version='0.1')
    parser.add_option("-a", "--automator",
                  action="store",
                  dest="automator_file",
                  help="Automator config file")

    parser.add_option("-m", "--metrics",
                  action="store",
                  dest="metrics_path",
                  help="Path to the metrics modules to be loaded")


    (opts, args) = parser.parse_args()

    if len(args) != 0:
        parser.error("Wrong number of arguments")

    if not opts.automator_file or not opts.metrics_path:
        parser.error("automator file and metrics path param is needed.")

    return opts

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Grimoire Metrics Tool")
    opts = get_options()

    Report.init(opts.automator_file, opts.metrics_path)

    metrics = SCM.get_metrics_definition()
    for name in metrics:
        print name

    metrics_set = SCM.get_metrics_set()
    for metrics in metrics_set:
        print metrics.get_definition()['name']
        agg = metrics.get_agg()
        if agg is not None: print(agg)
        evol = metrics.get_ts()
        if evol is not None: print(evol)
