#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Bitergia
#
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
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#
# import MySQLdb, os, random, string, sys
# import data_source

import json, os, unittest
from report import Report
import logging

from GrimoireUtils import read_options, getPeriod, read_main_conf
from GrimoireUtils import compare_json_data, completePeriodIds
from GrimoireUtils import createJSON, compareJSON

class DataSourceTest(unittest.TestCase):
    @staticmethod
    def init():
        Report.init()
        logging.info("Init Data Source")

    @staticmethod
    def close():
        logging.info("End Data Source")

    def test_get_bots(self):
        for ds in Report.get_data_sources():
            bots = ds.get_bots()
            self.assertTrue(isinstance(bots, list))

    def test_set_bots(self):
        bots = ['test']
        for ds in Report.get_data_sources():
            ds.set_bots(bots)
            ds_bots = ds.get_bots()
            self.assertEqual(ds_bots[0], bots[0])

    def test_get_name(self):
        for ds in Report.get_data_sources():
            ds_name = ds.get_name()
            self.assertNotEqual(ds_name, "")

    def test_get_db_name(self):
        for ds in Report.get_data_sources():
            ds_db_name = ds.get_db_name()
            self.assertNotEqual(ds_db_name, "")

    def test_get_evolutionary_filename(self):
        for ds in Report.get_data_sources():
            f_evol = ds.get_evolutionary_filename(ds.get_name())
            self.assertNotEqual(f_evol, "")

    def test_get_evolutionary_data(self):
        opts = read_options()
        period = getPeriod(opts.granularity)
        startdate = "'"+opts.startdate+"'"
        enddate = "'"+opts.enddate+"'"

        automator = read_main_conf(opts.config_file)
        identities_db = automator['generic']['db_identities']

        # Test without filters
        for ds in Report.get_data_sources():
            # Create the evolutionary data from dbs and check with test JSON
            logging.info(ds.get_name() + " test_get_evolutionary_data")
            Report.connect_ds(ds)
            ds_data = ds.get_evolutionary_data (period, startdate,
                                                enddate, identities_db)

            test_json = ds.get_evolutionary_filename(ds.get_name())
            f_test_json = os.path.join("json", test_json)

            self.assertTrue(DataSourceTest._compare_data(ds_data, f_test_json))

    def test_create_evolutionary_report(self):
        opts = read_options()
        period = getPeriod(opts.granularity)
        startdate = "'"+opts.startdate+"'"
        enddate = "'"+opts.enddate+"'"

        automator = read_main_conf(opts.config_file)
        identities_db = automator['generic']['db_identities']

        # Test without filters
        for ds in Report.get_data_sources():
            # Create the evolutionary data from dbs and check with test JSON
            logging.info(ds.get_name() + " create_evolutionary_report")
            Report.connect_ds(ds)
            ds.create_evolutionary_report (period, startdate,
                                           enddate, identities_db)

            ds_json = ds.get_evolutionary_filename(ds.get_name())
            f_test_json = os.path.join("json", ds_json)
            f_report_json = os.path.join(opts.destdir, ds_json)

            self.assertTrue(compareJSON(f_test_json, f_report_json))

    @staticmethod
    def _compare_data(data, json_file):
        # Create a temporary JSON file with data
        from tempfile import NamedTemporaryFile
        data_file = NamedTemporaryFile()
        data_file_name = data_file.name
        data_file.close()
        createJSON(data, data_file_name, check=False, skip_fields = [])

        return compareJSON(data_file_name, json_file)

    def test_get_agg_filename(self):
        for ds in Report.get_data_sources():
            f_agg = ds.get_agg_filename(ds.get_name())
            self.assertNotEqual(f_agg, "")

    def test_get_agg_data(self):
        opts = read_options()
        period = getPeriod(opts.granularity)
        startdate = "'"+opts.startdate+"'"
        enddate = "'"+opts.enddate+"'"

        automator = read_main_conf(opts.config_file)
        identities_db = automator['generic']['db_identities']

        # Test without filters
        for ds in Report.get_data_sources():
            logging.info(ds.get_name() + " test_get_agg_data")
            # Create the evolutionary data from dbs and check with test JSON
            Report.connect_ds(ds)
            ds_data = ds.get_agg_data (period, startdate,
                                                enddate, identities_db)

            test_json = os.path.join("json",ds.get_agg_filename(ds.get_name()))

            self.assertTrue(DataSourceTest._compare_data(ds_data, test_json))

    def test_create_agg_report(self):
        opts = read_options()
        period = getPeriod(opts.granularity)
        startdate = "'"+opts.startdate+"'"
        enddate = "'"+opts.enddate+"'"

        automator = read_main_conf(opts.config_file)
        identities_db = automator['generic']['db_identities']

        # Test without filters
        for ds in Report.get_data_sources():
            logging.info(ds.get_name() + " create_agg_report")
            # Create the evolutionary data from dbs and check with test JSON
            Report.connect_ds(ds)
            ds.create_agg_report (period, startdate,
                                  enddate, identities_db)

            ds_json = ds.get_agg_filename(ds.get_name())
            f_test_json = os.path.join("json", ds_json)
            f_report_json = os.path.join(opts.destdir, ds_json)

            self.assertTrue(compareJSON(f_test_json, f_report_json))


    def test_get_agg_evol_filters_data(self):
        opts = read_options()
        startdate = "'"+opts.startdate+"'"
        enddate = "'"+opts.enddate+"'"

        automator = read_main_conf(opts.config_file)
        identities_db = automator['generic']['db_identities']
        bots = []

        # Test for all filters
        for ds in Report.get_data_sources():
            Report.connect_ds(ds)
            for filter_ in Report.get_filters():
                filter_name = filter_.get_name()
                filter_name_short = filter_.get_name_short()
                items = ds.get_filter_items(filter_, startdate, enddate, identities_db, bots)
                if items is None: continue
                if (isinstance(items, dict)): items = items['name']

                if not isinstance(items, (list)): items = [items]

                for item in items:
                    item_name = item
                    if ds.get_name() not in ["irc","scr"]:
                        item_name = "'"+item+"'"
                    type_analysis = [filter_name, item_name]
                    item_file = item
                    if ds.get_name() in ["its","scr"] :
                        item_file = item.replace("/","_")

                    elif ds.get_name() == "mls":
                        item_file = item.replace("/","_").replace("<","__").replace(">","___")

                    logging.info(ds.get_name() +","+ filter_name+","+ item+","+ "agg")
                    agg = ds.get_filter_item_agg(startdate, enddate, identities_db, type_analysis)
                    fn = item_file+"-"+ds.get_name()+"-"+filter_name_short+"-static.json"
                    test_json = os.path.join("json",fn)
                    self.assertTrue(DataSourceTest._compare_data(agg, test_json))

                    logging.info(ds.get_name() +","+ filter_name+","+ item+","+ "evol")
                    evol = ds.get_filter_item_evol(startdate, enddate, identities_db, type_analysis)
                    fn = item_file+"-"+ds.get_name()+"-"+filter_name_short+"-evolutionary.json"
                    test_json = os.path.join("json",fn)
                    self.assertTrue(DataSourceTest._compare_data(evol, test_json))

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN,format='%(asctime)s %(message)s')

    DataSourceTest.init()
    suite = unittest.TestLoader().loadTestsFromTestCase(DataSourceTest)
    unittest.TextTestRunner(verbosity=2).run(suite)

    DataSourceTest.close()