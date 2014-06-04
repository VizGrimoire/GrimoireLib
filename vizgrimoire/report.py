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
##   Alvaro del Castillo <acs@bitergia.com>


from GrimoireSQL import SetDBChannel
from GrimoireUtils import read_main_conf
import logging
import SCM, ITS, MLS, SCR, Mediawiki, IRC, DownloadsDS, QAForums, ReleasesDS
from filter import Filter
from metrics import Metrics
from query_builder import DSQuery

class Report(object):
    """Basic class for a Grimoire automator based dashboard"""

    _filters = []
    _all_data_sources = []
    _automator = None
    _automator_file = None

    @staticmethod
    def init(automator_file, metrics_path = None):
        Report._automator_file = automator_file
        Report._automator = read_main_conf(automator_file)
        Report._init_filters()
        Report._init_data_sources()
        if metrics_path is not None:
            Report._init_metrics(metrics_path)

    @staticmethod
    def _init_filters():
        reports = Report._automator['r']['reports']
        # Hack because we use repos in filters
        reports = reports.replace("repositories","repos")
        filters = reports.split(",")
        # people not a filter yet
        if 'people' in filters: filters.remove('people')
        for name in filters:
            filter_ = Filter.get_filter_from_plural(name)
            if filter_ is not None:
                Report._filters.append(filter_)
            else:
                logging.error("Wrong filter " + name + ", review " + Report._automator_file)

    @staticmethod
    def _init_data_sources():
        Report._all_data_sources = [SCM.SCM, ITS.ITS, MLS.MLS, SCR.SCR, 
                                    Mediawiki.Mediawiki, IRC.IRC, DownloadsDS.DownloadsDS,
                                    QAForums.QAForums, ReleasesDS.ReleasesDS]

    @staticmethod
    def _init_metrics(metrics_path):
        """Register all available metrics"""
        logging.info("Loading metrics modules from %s" % (metrics_path))
        from os import listdir
        from os.path import isfile, join
        import imp, inspect

        db_identities = Report._automator['generic']['db_identities']
        dbuser = Report._automator['generic']['db_user']
        dbpass = Report._automator['generic']['db_password']

        metrics_mod = [ f for f in listdir(metrics_path) 
                       if isfile(join(metrics_path,f)) and f.endswith("_metrics.py")]

        for metric_mod in metrics_mod:
            mod_name = metric_mod.split(".py")[0]
            mod = __import__(mod_name)
            # Support for having more than one metric per module
            metrics_classes = [c for c in mod.__dict__.values() 
                               if inspect.isclass(c) and issubclass(c, Metrics)]
            for metrics_class in metrics_classes:
                ds = metrics_class.data_source
                if ds is None: continue
                if ds.get_db_name() not in Report._automator['generic']: continue
                builder = ds.get_query_builder()
                db = Report._automator['generic'][ds.get_db_name()]
                metrics = metrics_class(builder(dbuser, dbpass, db, db_identities))
                ds.add_metrics(metrics, ds)

    @staticmethod
    def get_config():
        return Report._automator

    @staticmethod
    def connect_ds(ds):
        db = Report._automator['generic'][ds.get_db_name()]
        dbuser = Report._automator['generic']['db_user']
        dbpassword = Report._automator['generic']['db_password']
        SetDBChannel (database=db, user=dbuser, password=dbpassword)

    @staticmethod
    def get_data_sources():

        data_sources= []

        for ds in Report._all_data_sources:
            if not ds.get_db_name() in Report._automator['generic']: continue
            else: data_sources.append(ds)
        return data_sources

    @staticmethod
    def get_data_source(name):
        found = None
        for ds in Report.get_data_sources():
            if ds.get_name() == name:
                found = ds
        return found

    @staticmethod
    def set_data_sources(dss):
        Report._all_data_sources = dss 

    @staticmethod
    def get_filters():
        return Report._filters

    @staticmethod
    def set_filters(filters):
        Report._filters = filters 

    @staticmethod
    def get_filter(name):
        found = None
        for filter_ in Report.get_filters():
            if filter_.get_name() == name:
                found = filter_
        return found