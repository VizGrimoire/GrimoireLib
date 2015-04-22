#!/usr/bin/env python

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
#
# Authors:
#     Daniel Izquierdo <dizquierdo@bitergia.com>
#     Alvaro del Castillo <acs@bitergia.com>

import logging, os

from vizgrimoire.data_source import DataSource
from vizgrimoire.GrimoireUtils import createJSON

class EventsDS(DataSource):

    @staticmethod
    def get_db_name():
        return "db_eventizer"

    @staticmethod
    def get_query_builder():
        """Class used to build queries to get metrics"""
        from vizgrimoire.metrics.query_builder import EventizerQuery
        return EventizerQuery

    @staticmethod
    def get_name():
        return "eventizer"

    @staticmethod
    def get_supported_filters():
        # return ['group','category','city']
        return []

    @staticmethod
    def get_metrics_core_agg():
        return ['events','members','cities','attendees']

    @staticmethod
    def get_metrics_core_ts():
        return ['events','members','cities','attendees']

    @staticmethod
    def get_metrics_core_trends():
        return ['events','members','cities','attendees']

    @staticmethod
    def get_evolutionary_data (period, startdate, enddate, i_db, filter_ = None):
        if filter_ is not None:
            sf = EventsDS.get_supported_filters()
            if filter_.get_name() not in sf:
                logging.warn("EventsDS only supports " + ",".join(sf))
                return {}

        metrics =  EventsDS.get_metrics_data(period, startdate, enddate, i_db, filter_, True)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_studies_data(EventsDS, period, startdate, enddate, True)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_evolutionary_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data =  EventsDS.get_evolutionary_data (period, startdate, enddate, i_db, type_analysis)
        filename = EventsDS().get_evolutionary_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_agg_data (period, startdate, enddate, identities_db, filter_ = None):
        if filter_ is not None:
            sf = EventsDS.get_supported_filters()
            if filter_.get_name() not in sf:
                logging.warn("EventsDS only supports " + ",".join(sf))
                return {}

        metrics =  EventsDS.get_metrics_data(period, startdate, enddate, identities_db, filter_, False)
        if filter_ is not None: studies = {}
        else:
            studies =  DataSource.get_studies_data(EventsDS, period, startdate, enddate, False)
        return dict(metrics.items()+studies.items())

    @staticmethod
    def create_agg_report (period, startdate, enddate, destdir, i_db, type_analysis = None):
        data = EventsDS.get_agg_data (period, startdate, enddate, i_db, type_analysis)
        filename = EventsDS().get_agg_filename()
        createJSON (data, os.path.join(destdir, filename))

    @staticmethod
    def get_filter_items(filter_, startdate, enddate, identities_db):
        items = None
        filter_name = filter_.get_name()

        logging.error("EventsDS " + filter_name + " not supported")
        return items

    @staticmethod
    def create_filter_report(filter_, period, startdate, enddate, destdir, npeople, identities_db):
        from vizgrimoire.report import Report
        items = Report.get_items()
        if items is None:
            items = EventsDS.get_filter_items(filter_, startdate, enddate, identities_db)
        if (items == None): return

    @staticmethod
    def get_top_metrics ():
        return ["attendes"]

    @staticmethod
    def get_top_data (startdate, enddate, identities_db, filter_, npeople):
        top = {}
        attendees = DataSource.get_metrics("attendees", EventsDS)
        period = None
        type_analysis = None
        if filter_ is not None:
            logging.info("Mediawiki does not support yet top for filters.")
            return top

        top['attendees.'] = attendees.get_list(None, 0)
        top['attendees.last month'] = attendees.get_list(None, 31)
        top['attendees.last year'] = attendees.get_list(None, 365)

        return(top)

    @staticmethod
    def create_top_report (startdate, enddate, destdir, npeople, i_db):
        data = EventsDS.get_top_data (startdate, enddate, i_db, None, npeople)
        top_file = destdir+"/"+EventsDS().get_top_filename()
        createJSON (data, top_file)

    @staticmethod
    def get_top_people(startdate, enddate, identities_db, npeople):
        return []

        top_data = EventsDS.get_top_data (startdate, enddate, identities_db, None, npeople)

        top = top_data['attendees.']["id"]
        top += top_data['attendees.last year']["id"]
        top += top_data['attendees.last month']["id"]
        # remove duplicates
        people = list(set(top))
        return people

