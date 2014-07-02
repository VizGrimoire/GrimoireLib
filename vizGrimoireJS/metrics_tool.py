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

import logging, os, sys
from optparse import OptionParser

def get_options():
    parser = OptionParser(usage='Usage: %prog [options]',
                          description='Tool for metrics management',
                          version='0.1')
    parser.add_option("-a", "--automator",
                  action="store",
                  dest="automator_file",
                  default = "../../../conf/main.conf",
                  help="Automator config file")

    parser.add_option("-m", "--metrics",
                  action="store",
                  dest="metrics_path",
                  default = "../vizgrimoire/metrics",
                  help="Path to the metrics modules to be loaded")
    parser.add_option("--data-source",
                      action="store",
                      dest="data_source",
                      help="data source to be generated")
    parser.add_option("-l", "--list",
                      action="store_true",
                      dest="list",
                      help="Only list metrics, don't compute them. Default true.")

    (opts, args) = parser.parse_args()

    if len(args) != 0:
        parser.error("Wrong number of arguments")

    if not opts.automator_file or not opts.metrics_path:
        parser.error("automator file and metrics path param is needed.")

    return opts

def init_env():
    grimoirelib = os.path.join("..","vizgrimoire")
    metricslib = os.path.join("..","vizgrimoire","metrics")
    studieslib = os.path.join("..","vizgrimoire","analysis")
    alchemy = os.path.join("..","grimoirelib_alch")
    for dir in [grimoirelib,metricslib,studieslib,alchemy]:
        sys.path.append(dir)

    # env vars for R
    os.environ["LANG"] = ""
    os.environ["R_LIBS"] = "../../r-lib"

if __name__ == '__main__':
    init_env()
    from metric import Metric
    from SCM import SCM
    from report import Report

    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Grimoire Metrics Tool")
    opts = get_options()

    Report.init(opts.automator_file, opts.metrics_path)

    # metrics = SCM.get_metrics_definition()
    # for name in metrics:
    #     print name

    dss = Report.get_data_sources()

    if (opts.data_source):
        ds = Report.get_data_source(opts.data_source)
        dss = [ds]
        if ds is None:
            logging.error("Data source not found " + opts.data_source)
            dss = []

    total_metrics = 0
    total_studies = 0

    for ds in dss:
        print("\nGetting metrics for " + ds.get_name())
        metrics_set = ds.get_metrics_set(ds)
        for metrics in metrics_set:
            print "->" + metrics.get_definition()['name']
            total_metrics += 1
            if opts.list is None:
                agg = metrics.get_agg()
                if agg is not None: print(agg)
                evol = metrics.get_ts()
                # if evol is not None: print(evol)

    print("\nTotal metrics: " + str(total_metrics))
    print("Total studies: " + str(total_studies))