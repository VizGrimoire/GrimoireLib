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


from vizgrimoire.GrimoireSQL import SetDBChannel
from vizgrimoire.GrimoireUtils import read_main_conf
import logging, time, sys
import vizgrimoire.SCM as SCM
import vizgrimoire.ITS as ITS
import vizgrimoire.ITS_1 as ITS_1
import vizgrimoire.MLS as MLS
import vizgrimoire.SCR as SCR
import vizgrimoire.Mediawiki as Mediawiki
import vizgrimoire.IRC as IRC
import vizgrimoire.DownloadsDS as DownloadsDS
import vizgrimoire.QAForums as QAForums
import vizgrimoire.ReleasesDS as ReleasesDS
import vizgrimoire.Pullpo as Pullpo
from vizgrimoire.filter import Filter
from vizgrimoire.metrics.metrics import Metrics
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.analysis.analyses import Analyses
from vizgrimoire.metrics.query_builder import DSQuery

class Report(object):
    """Basic class for a Grimoire automator based dashboard"""

    _filters = [] # filters active
    _filters_automator = [] # filters in automator file
    _items = None
    _all_data_sources = []
    _all_studies = []
    _on_studies = []
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
            studies_path = metrics_path.replace("metrics","analysis")
            Report._init_studies(studies_path)

    @staticmethod
    def _init_filters():
        reports = Report._automator['r']['reports']
        # Hack because we use repos in filters
        reports = reports.replace("repositories","repos")
        filters = reports.split(",")
        for name in filters:
            filter_ = Filter.get_filter_from_plural(name)
            if filter_ is not None:
                Report._filters.append(filter_)
                Report._filters_automator.append(filter_)
            else:
                logging.error("Wrong filter " + name + ", review " + Report._automator_file)
                raise Exception('Wrong automator config file')

    @staticmethod
    def _init_data_sources():
        Report._all_data_sources = [SCM.SCM, ITS.ITS, ITS_1.ITS_1, MLS.MLS, SCR.SCR,
                                    Mediawiki.Mediawiki, IRC.IRC, DownloadsDS.DownloadsDS,
                                    QAForums.QAForums, ReleasesDS.ReleasesDS, Pullpo.Pullpo]
        if 'people_out' in Report.get_config()['r']:
            bots = Report.get_config()['r']['people_out'].split(",")
            for ds in Report._all_data_sources:
                ds.set_bots(bots)
        # Global filters per data source
        for ds in Report._all_data_sources:
            if ds.get_name()+'_global_filter' in Report.get_config()['r']:
                ds.set_global_filter(ds, Report.get_config()['r'][ds.get_name()+'_global_filter'])

    @staticmethod
    def _init_metrics(metrics_path):
        """Register all available metrics"""
        def get_default_filter():
            npeople = Metrics.default_npeople
            people_out = None
            if 'people_out' in Report._automator['r']:
                people_out = Report._automator['r']['people_out'].split(",")
            companies_out = None
            if 'companies_out' in Report._automator['r']:
                companies_out = Report._automator['r']['companies_out'].split(",")
            type_analysis = None
            if 'start_date' not in Report._automator['r']:
                raise Exception("Start date not configured in automator main.conf")
            start_date = Report._automator['r']['start_date']
            if 'end_date' in Report._automator['r']:
                end_date = Report._automator['r']['end_date']
            else:
                end_date = time.strftime('%Y-%m-%d')


            metric_filters = MetricFilters(Metrics.default_period, "'"+start_date+"'", "'"+end_date+"'",
                                           type_analysis,
                                           npeople, people_out, companies_out)
            return metric_filters

        # logging.info("Loading metrics modules from %s" % (metrics_path))
        # sys.path.insert(1,metrics_path) # Prepend the metrics path

        from os import listdir
        from os.path import isfile, join, dirname
        import imp, inspect

        db_identities = Report._automator['generic']['db_identities']
        dbuser = Report._automator['generic']['db_user']
        dbpass = Report._automator['generic']['db_password']

        # Read all available metrics installed in GrimoireLib egg
        metrics_pkg = "vizgrimoire.metrics"
        import vizgrimoire.metrics
        mfile = inspect.getfile(vizgrimoire.metrics)
        if ".egg" in mfile:
            # Load list of metrics from files zipped inside lib installed egg
            mfile = mfile.split(".egg")[0] + ".egg"

            import zipfile
            zip = zipfile.ZipFile(mfile)
            metrics_mod = [f.replace("vizgrimoire/metrics/","") for f in zip.namelist()
                           if "vizgrimoire/metrics/" in f and f.endswith(".py")]
            zip.close()
        else:
            # Load list of metrics from metrics directory
            metrics_path = dirname(mfile)
            metrics_mod = [f for f in listdir(metrics_path)
                           if isfile(join(metrics_path,f)) and f.endswith("_metrics.py")]

        for metric_mod in metrics_mod:
            mod_name = metric_mod.split(".py")[0]
            mod = __import__(metrics_pkg+"."+mod_name)
            # Support for having more than one metric per module
            metrics_classes = [c for name, c in inspect.getmembers(sys.modules[metrics_pkg+"."+mod_name])
                               if inspect.isclass(c) and issubclass(c, Metrics)]

            for metrics_class in metrics_classes:
                ds = metrics_class.data_source
                if ds is None: continue
                if ds.get_db_name() not in Report._automator['generic']: continue
                builder = ds.get_query_builder()
                db = Report._automator['generic'][ds.get_db_name()]
                metric_filters = get_default_filter()
                if (ds.get_global_filter(ds) is not None):
                    metric_filters.global_filter = ds.get_global_filter(ds)
                metrics = metrics_class(builder(dbuser, dbpass, db, db_identities), metric_filters)
                ds.add_metrics(metrics, ds)
                if ds == ITS.ITS:
                    db_its1_name = ITS_1.ITS_1.get_db_name()
                    if db_its1_name in Report._automator['generic']:
                        db_its1 = Report._automator['generic'][db_its1_name]
                        metric_filters = get_default_filter()
                        metric_filters.set_closed_condition(ITS_1.ITS_1._get_closed_condition())
                        metrics = metrics_class(builder(dbuser, dbpass, db_its1, db_identities), metric_filters)
                        ITS_1.ITS_1.add_metrics(metrics, ITS_1.ITS_1)

                # Specific filters
                if ds.get_name() == "scr":
                    if 'scr_start_date' in Report._automator['r']:
                        metrics.filters.start_date = Report._automator['r']['scr_start_date']

    @staticmethod
    def _init_studies(studies_path):
        """Register all available studies"""
        # logging.info("Loading studies modules from %s" % (studies_path))
        # sys.path.insert(1,studies_path) # Prepend the studies path

        from os import listdir
        from os.path import isfile, join, dirname
        import imp, inspect

        db_identities = Report._automator['generic']['db_identities']
        dbuser = Report._automator['generic']['db_user']
        dbpass = Report._automator['generic']['db_password']
        if 'studies' not in Report._automator['r']:
            logging.info("No studies configured.")
            return
        studies_on = Report._automator['r']['studies'].split(",")

        # Read all available studies installed in GrimoireLib egg
        studies_pkg = "vizgrimoire.analysis"
        import vizgrimoire.analysis
        mfile = inspect.getfile(vizgrimoire.analysis)

        if ".egg" in mfile:
            # Load list of analysis from files zipped inside lib installed egg
            mfile = mfile.split(".egg")[0] + ".egg"
            import zipfile
            zip = zipfile.ZipFile(mfile)
            studies_mod = [f.replace("vizgrimoire/analysis/","") for f in zip.namelist()
                           if "vizgrimoire/analysis/" in f and f.endswith(".py")]
            zip.close()
        else:
            # Load list of analysis from analysis directory
            studies_path = dirname(mfile)
            studies_mod = [f for f in listdir(studies_path)
                           if isfile(join(studies_path,f)) and f.endswith(".py")]

        for study_mod in studies_mod:
            mod_name = study_mod.split(".py")[0]
            mod = __import__(studies_pkg+"."+mod_name)
            # Support for having more than one study per module
            studies_classes = [c for name, c in inspect.getmembers(sys.modules[studies_pkg+"."+mod_name])
                               if inspect.isclass(c) and issubclass(c, Analyses)]
            for study_class in studies_classes:
                if study_class == Analyses: continue
                Report._all_studies.append(study_class)
                if study_class.id is None or study_class.id not in studies_on: continue
                # logging.info("Adding new study: " + study_class.id)
                Report._on_studies.append(study_class)
        #  logging.info("Total studies: " + str(len(Report._on_studies)))


    @staticmethod
    def get_config():
        return Report._automator

    @staticmethod
    def get_start_date():
        return Report._automator['r']['start_date']

    @staticmethod
    def get_end_date():
        return Report._automator['r']['end_date']

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
                break
        return found

    @staticmethod
    def get_filter_automator(name):
        # Sometimes not all automator filters are active
        found = None
        for filter_ in Report._filters_automator:
            if filter_.get_name() == name:
                found = filter_
                break
        return found

    @staticmethod
    def get_items():
        return Report._items

    @staticmethod
    def set_items(items):
        Report._items = items

    @staticmethod
    def get_studies():
        return Report._on_studies

    @staticmethod
    def get_all_studies():
        return Report._all_studies

    @staticmethod
    def set_studies(studies):
        Report._on_studies = studies

    @staticmethod
    def get_study_by_id(sid):
        found = None
        for study in Report.get_studies():
            if study.id == sid:
                found = study
                break
        return found
