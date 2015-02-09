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

import logging, os, sys, time

from utils import read_options

def get_evol_report(startdate, enddate, identities_db):
    all_ds = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        all_ds[ds.get_name()] = ds.get_evolutionary_data (period, startdate, enddate, identities_db)
    return all_ds

def create_evol_report(startdate, enddate, destdir, identities_db):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        ds.create_evolutionary_report (period, startdate, enddate, destdir, identities_db)

def get_agg_report(startdate, enddate, identities_db):
    all_ds = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        all_ds[ds.get_name()] = ds.get_agg_data (period, startdate, enddate, identities_db)
    return all_ds

def create_agg_report(startdate, enddate, destdir, identities_db):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        ds.create_agg_report (period, startdate, enddate, destdir, identities_db)

def get_top_report(startdate, enddate, npeople, identities_db):
    all_ds_top = {}

    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        top = ds.get_top_data (startdate, enddate, identities_db, None, npeople)
        all_ds_top[ds.get_name()] = top 
    return all_ds_top

def create_top_report(startdate, enddate, destdir, npeople, identities_db):
    for ds in Report.get_data_sources():
        logging.info("Creating TOP for " + ds.get_name())
        Report.connect_ds(ds)
        ds.create_top_report (startdate, enddate, destdir, npeople, identities_db)

def create_reports_filters(period, startdate, enddate, destdir, npeople, identities_db):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        logging.info("Creating filter reports for " + ds.get_name())
        for filter_ in Report.get_filters():
            logging.info("-> " + filter_.get_name())
            # Tested in all this filters the group by
            # if filter_.get_name() in ["people2","company","company+country","country","repository","domain"]:
            if filter_.get_name() in ["people2","company+country"]:
                logging.info("---> Using new filter API")
                ds.create_filter_report_all(filter_, period, startdate, enddate, 
                                            destdir, npeople, identities_db)
            else:
                ds.create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db)

def create_report_people(startdate, enddate, destdir, npeople, identities_db):
    for ds in Report.get_data_sources():
        Report.connect_ds(ds)
        logging.info("Creating people for " + ds.get_name())
        ds().create_people_report(period, startdate, enddate, destdir, npeople, identities_db)

def create_reports_r(enddate, destdir):
    from rpy2.robjects.packages import importr
    opts = read_options()

    vizr = importr("vizgrimoire")

    for ds in Report.get_data_sources():
	if ds.get_name() != "its": continue
        automator = Report.get_config()
        db = automator['generic'][ds.get_db_name()]
        dbuser = Report._automator['generic']['db_user']
        dbpassword = Report._automator['generic']['db_password']
        vizr.SetDBChannel (database=db, user=dbuser, password=dbpassword)
        logging.info("Creating R reports for " + ds.get_name())
        ds.create_r_reports(vizr, enddate, destdir)

def create_people_identifiers(startdate, enddate, destdir, npeople, identities_db):
    logging.info("Generating people identifiers")

    people = get_top_report(startdate, enddate, npeople, identities_db);
    people_ids = [] # upeople_ids which need identifiers
    people_data = {} # identifiers for upeople_ids
    ds_scm = Report.get_data_source("scm")
    if ds_scm is None:
        # Without SCM (identities) data source can not continue
        return

    for ds in Report.get_data_sources():
        periods = [".",".last year",".last month"]
        top_names = ds.get_top_metrics();
        for period in periods:
            for top_name in top_names:
                if top_name+period in people[ds.get_name()]:
                    if 'id' in people[ds.get_name()][top_name+period]:
                        people_ids += people[ds.get_name()][top_name+period]['id']
    people_ids = list(set(people_ids))

    from vizgrimoire.SCM import GetPeopleListSCM
    import vizgrimoire.People as People

    # TODO: Identities db is the same than SCM
    Report.connect_ds(ds_scm)

    for upeople_id in people_ids:
        people_data[upeople_id] = People.GetPersonIdentifiers(upeople_id)

    createJSON(people_data, destdir+"/people.json")

def create_reports_studies(period, startdate, enddate, destdir):
    from vizgrimoire.metrics.metrics_filter import MetricFilters

    db_identities= Report.get_config()['generic']['db_identities']
    dbuser = Report.get_config()['generic']['db_user']
    dbpass = Report.get_config()['generic']['db_password']

    studies = Report.get_studies()

    metric_filters = MetricFilters(period, startdate, enddate, [])

    for ds in Report.get_data_sources():
        ds_dbname = ds.get_db_name()
        dbname = Report.get_config()['generic'][ds_dbname]
        dsquery = ds.get_query_builder()
        dbcon = dsquery(dbuser, dbpass, dbname, db_identities)
        # logging.info(ds.get_name() + " studies active " + str(studies))
        for study in studies:
            # logging.info("Creating report for " + study.id + " for " + ds.get_name())
            try:
                obj = study(dbcon, metric_filters)
                obj.create_report(ds, destdir)
            except TypeError:
                import traceback
                logging.info(study.id + " does no support standard API. Not used.")
                traceback.print_exc(file=sys.stdout)
                continue

def create_alerts(startdate, enddate, destdir):
    for ds in Report.get_data_sources():
        if ds.get_name() != "scm": continue
        logging.info("ALERTS for " + ds.get_name())
        alerts = ds.get_alerts()
        createJSON(alerts, destdir+"/"+ds.get_name()+"-alerts.json")

def set_data_source(ds_name):
    ds_ok = False
    dss_active = Report.get_data_sources()
    DS = None
    for ds in dss_active:
        if ds.get_name() == opts.data_source:
            ds_ok = True
            DS = ds
            Report.set_data_sources([DS])
    if not ds_ok:
        logging.error(opts.data_source + " data source not available")
        sys.exit(1)
    return DS

def set_filter(filter_name, item = None):
    filter_ok = False
    filters_active = Report.get_filters()
    for filter_ in filters_active:
        if filter_.get_name() == opts.filter:
            filter_ok = True
            Report.set_filters([filter_])
            if item is not None: Report.set_items([item])
    if not filter_ok:
        logging.error(opts.filter + " filter not available")
        sys.exit(1)

def set_metric(metric_name, ds_name):
    metric_ok = False
    DS = set_data_source(ds_name)

    metrics = DS.get_metrics_set(DS)
    for metric in metrics:
        if metric.id == metric_name:
            metric_ok = True
            DS.set_metrics_set(DS, [metric])
            logging.info("[metric] " + metric.name + " configured")
    if not metric_ok:
        logging.error(metric_name + " metric not available in " + DS.get_name())
        sys.exit(1)

def set_study(study_id):
    study_ok = False

    studies = Report.get_studies()
    for study in studies:
        if study.id == study_id:
            study_ok = True
            Report.set_studies([study])
    if not study_ok:
        logging.error(study_id + " study not available ")
        sys.exit(1)

def init_env():
    # env vars for R
    os.environ["LANG"] = ""
    os.environ["R_LIBS"] = "../../r-lib"

if __name__ == '__main__':

    init_env()
    from vizgrimoire.GrimoireUtils import getPeriod, read_main_conf, createJSON
    from vizgrimoire.report import Report

    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting Report analysis")
    opts = read_options()
    reports = opts.reports.split(",")

    Report.init(opts.config_file, opts.metrics_path)

    automator = read_main_conf(opts.config_file)
    if 'start_date' not in automator['r']:
        logging.error("start_date (yyyy-mm-dd) not found in " + opts.config_file)
        sys.exit()
    start_date = automator['r']['start_date']
    if 'end_date' not in automator['r']:
        end_date = time.strftime('%Y-%m-%d')
    else:
        end_date = automator['r']['end_date']

    if 'period' not in automator['r']:
        period = getPeriod("months")
    else:
        period = getPeriod(automator['r']['period'])
    logging.info("Period: " + period)

    if 'people_number' in automator['generic']:
	npeople = automator['generic']['people_number']
    	logging.info("Number of people: " + npeople)
	opts.npeople = npeople

    # TODO: hack because VizR library needs. Fix in lib in future
    startdate = "'"+start_date+"'"
    enddate = "'"+end_date+"'"

    identities_db = automator['generic']['db_identities']

    if (opts.data_source):
        set_data_source(opts.data_source)
    if (opts.filter):
        set_filter(opts.filter, opts.item)
    if (opts.metric):
        set_metric(opts.metric, opts.data_source)
    if (opts.study):
        set_study(opts.study)
    if (opts.alerts):
        create_alerts(startdate, enddate, opts.destdir)
        logging.info("Alerts generated OK")
        sys.exit(0)

    if not opts.filter and not opts.study:
        logging.info("Creating global evolution metrics...")
        evol = create_evol_report(startdate, enddate, opts.destdir, identities_db)
        logging.info("Creating global aggregated metrics...")
        agg = create_agg_report(startdate, enddate, opts.destdir, identities_db)
        if not opts.metric:
            logging.info("Creating global top metrics...")
            top = create_top_report(startdate, enddate, opts.destdir, opts.npeople, identities_db)
            if (automator['r']['reports'].find('people')>-1):
                create_report_people(startdate, enddate, opts.destdir, opts.npeople, identities_db)
            # create_reports_r(end_date, opts.destdir)
            create_people_identifiers(startdate, enddate, opts.destdir, opts.npeople, identities_db)

    if not opts.study and not opts.no_filters and not opts.metric:
        create_reports_filters(period, startdate, enddate, opts.destdir, opts.npeople, identities_db)
    if not opts.filter and not opts.metric and not opts.item:
        create_reports_studies(period, startdate, enddate, opts.destdir)

    logging.info("Report data source analysis OK")
