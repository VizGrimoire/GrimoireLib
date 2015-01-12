## Copyright (C) 2012, 2013 Bitergia
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
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##

from optparse import OptionParser

from os import listdir

from os.path import isfile, join

import imp, inspect

from vizgrimoire.metrics.metrics import Metrics

from vizgrimoire.metrics.query_builder import DSQuery, SCMQuery, ITSQuery

from vizgrimoire.metrics.metrics_filter import MetricFilters



def read_options():

    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 0.1")

    parser.add_option("-a", "--dbcvsanaly",
                      action="store",
                      dest="dbcvsanaly",
                      help="CVSAnalY db where information is stored")
    parser.add_option("-b", "--dbmlstats",
                      action="store",
                      dest="dbmlstats",
                      help="Mailing List Stats db where information is stored")
    parser.add_option("-c", "--dbbicho",
                      action="store",
                      dest="dbbicho",
                      help="Bicho db where information is stored")
    parser.add_option("-d", "--dbreview",
                      action="store",
                      dest="dbreview",
                      help="CVSAnalY where information is stored")
    parser.add_option("-e", "--dbirc",
                      action="store",
                      dest="dbirc",
                      help="IRC where information is stored")

    parser.add_option("-i", "--identities",
                      action="store",
                      dest="identities_db",
                      help="Database with unique identities and affiliations")

    parser.add_option("-u","--dbuser",
                      action="store",
                      dest="dbuser",
                      default="root",
                      help="Database user")
    parser.add_option("-p","--dbpassword",
                      action="store",
                      dest="dbpassword",
                      default="",
                      help="Database password")

    parser.add_option("-r", "--releases",
                      action="store",
                      dest="releases",
                      default="2010-01-01,2011-01-01,2012-01-01",
                      help="Releases for the report")
    parser.add_option("-t", "--type",
                      action="store",
                      dest="backend",
                      default="bugzilla",
                      help="Type of backend: bugzilla, allura, jira, github")
    parser.add_option("-g", "--granularity",
                      action="store",
                      dest="granularity",
                      default="months",
                      help="year,months,weeks granularity")
    parser.add_option("--npeople",
                      action="store",
                      dest="npeople",
                      default="10",
                      help="Limit for people analysis")
    parser.add_option("--metrics",
                      action="store",
                      dest="metrics",
                      help="List of metrics to analyze")
    # TBD
    #parser.add_option("--list-metrics",
    #                  help="List available metrics")

    (opts, args) = parser.parse_args()

    return opts


def init_metrics():

    metrics_path="../vizgrimoire/metrics/"

    metrics_mod = [ f for f in listdir(metrics_path)
                       if isfile(join(metrics_path,f)) and f.endswith("_metrics.py")]

    # dict: key : metrics_class.id
    #       value : metrics_class
    available_metrics = {}
    for metric_mod in metrics_mod:
        mod_name = metric_mod.split(".py")[0]
        mod = __import__(mod_name)
        # Support for having more than one metric per module
        metrics_classes = [c for c in mod.__dict__.values()
                         if inspect.isclass(c) and issubclass(c, Metrics)]
        print "hola"
        print type(metrics_classes)

        for metrics_class in metrics_classes:
            print metrics_class.id
            available_metrics[metrics_class.id] = metrics_class
        
    return available_metrics

def build_releases(releases_dates):
    # Builds a list of tuples of dates that limit
    # each of the timeperiods to analyze

    releases = []
    dates = releases_dates.split(",")
    init = dates[0]
    for date in dates:
        if init <> date:
            releases.append((init, date))
            init = date

    return releases


if __name__ == '__main__':

    # parse options
    opts = read_options()    
    print opts

    # obtain list of releases by tuples [(date1, date2), (date2, date3), ...]
    releases = build_releases(opts.releases)
    print releases

    # list of available metrics classes to compare with the user requested
    available_metrics = init_metrics()    
    print available_metrics

    # retrieve list of metrics to analyze from the config file/command line options
    required_metrics = opts.metrics.split(",")
    print required_metrics
    # look for them in the list of metrics (metrics classes)
    for metric in required_metrics:
        if available_metrics.has_key(metric):
            # metric found
            print metric
            metric_class = available_metrics[metric]
            ds = metric_class.data_source
            builder = ds.get_query_builder()
            filters = MetricFilters("weeks", "'2010-01-01'", "'2014-01-01'", []) 
            dbcon = builder("root", "", "dic_cvsanaly_openstack_2259", "dic_cvsanaly_openstack_2259")
            metric_instance = metric_class(dbcon, filters)
            print metric_instance.get_agg()


