## -*- coding: utf-8 -*-
##
## Copyright (C) 2014 Bitergia
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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>
##   Luis Cañas-Díaz <lcanas@bitergia.com>



import logging
import MySQLdb

import re, sys

from vizgrimoire.GrimoireUtils import completePeriodIds, GetDates, GetPercentageDiff

from vizgrimoire.metrics.metrics import Metrics

from vizgrimoire.metrics.metrics_filter import MetricFilters

from vizgrimoire.metrics.query_builder import MLSQuery

from vizgrimoire.DockerHubDS import DockerHubDS

class Pulls(Metrics):
    """ Number of pulls for a Docker Hub repo """

    id = "pulls"
    name = "Pulls"
    desc = "Number of pulls for a repo"
    data_source = DockerHubDS

    def _get_sql(self, evolutionary):

        if evolutionary:
            q = """
            SELECT YEAR(t.date)*12+MONTH(t.date) AS month, SUM(subtotal) as pulls
            FROM (SELECT MAX(pulls) as subtotal, date FROM repositories_log
            GROUP BY repo_id,YEAR(date)*12+MONTH(date)) t
            WHERE t.date>=""" + self.filters.startdate + """ AND t.date<""" + self.filters.enddate + """
            GROUP BY  YEAR(t.date),MONTH(t.date) ORDER BY YEAR(t.date),MONTH(t.date)
            """
        else:
            q ="""
            SELECT SUM(subtotal) as pulls FROM
            (SELECT MAX(pulls) as subtotal, date, repo_id
            FROM repositories_log
            WHERE date>=""" + self.filters.startdate + """ AND date<""" + self.filters.enddate + """
            GROUP BY repo_id ) t
            """
        return q

    def _get_top_global (self, days = 0, metric_filters = None):
            if metric_filters == None:
                metric_filters = self.filters

            startdate = metric_filters.startdate
            enddate = metric_filters.enddate
            limit = metric_filters.npeople

            query = """
                    SELECT name, pulls
                    FROM repositories
                    ORDER BY pulls
                    LIMIT %s
                    """ % (str(limit))
            return self.db.ExecuteQuery(query)

class Starred(Metrics):
    """ Number of starred for a Docker Hub repo """

    id = "starred"
    name = "Starred"
    desc = "Number of starred for a repo"
    data_source = DockerHubDS

    def _get_sql(self, evolutionary):

        if evolutionary:
            q = """
            SELECT YEAR(t.date)*12+MONTH(t.date) AS month, SUM(subtotal) as starred
            FROM (SELECT MAX(starred) as subtotal, date FROM repositories_log
            GROUP BY repo_id,YEAR(date)*12+MONTH(date)) t
            WHERE t.date>=""" + self.filters.startdate + """ AND t.date<""" + self.filters.enddate + """
            GROUP BY  YEAR(t.date),MONTH(t.date) ORDER BY YEAR(t.date),MONTH(t.date)
            """
        else:
            q ="""
            SELECT SUM(subtotal) as starred FROM
            (SELECT MAX(starred) as subtotal, date, repo_id
            FROM repositories_log
            WHERE date>=""" + self.filters.startdate + """ AND date<""" + self.filters.enddate + """
            GROUP BY repo_id ) t
            """
        return q

class Downloads(Metrics):
    """ Number of downloads for a Docker Hub repo """

    id = "downloads"
    name = "Downloads"
    desc = "Number of downloads for a repo"
    data_source = DockerHubDS

    def _get_sql(self, evolutionary):

        if evolutionary:
            q = """
            SELECT YEAR(t.date)*12+MONTH(t.date) AS month, SUM(subtotal) as downloads
            FROM (SELECT MAX(downloads) as subtotal, date FROM repositories_log
            GROUP BY repo_id,YEAR(date)*12+MONTH(date)) t
            WHERE t.date>=""" + self.filters.startdate + """ AND t.date<""" + self.filters.enddate + """
            GROUP BY  YEAR(t.date),MONTH(t.date) ORDER BY YEAR(t.date),MONTH(t.date)
            """
        else:
            q ="""
            SELECT SUM(subtotal) as downloads FROM
            (SELECT MAX(downloads) as subtotal, date, repo_id
            FROM repositories_log
            WHERE date>=""" + self.filters.startdate + """ AND date<""" + self.filters.enddate + """
            GROUP BY repo_id ) t
            """
        return q

class Forks(Metrics):
    """ Number of forks for a Docker Hub repo """

    id = "forks"
    name = "Forks"
    desc = "Number of forks for a repo"
    data_source = DockerHubDS

    def _get_sql(self, evolutionary):

        if evolutionary:
            q = """
            SELECT YEAR(t.date)*12+MONTH(t.date) AS month, SUM(subtotal) as forks
            FROM (SELECT MAX(forks) as subtotal, date FROM repositories_log
            GROUP BY repo_id,YEAR(date)*12+MONTH(date)) t
            WHERE t.date>=""" + self.filters.startdate + """ AND t.date<""" + self.filters.enddate + """
            GROUP BY  YEAR(t.date),MONTH(t.date) ORDER BY YEAR(t.date),MONTH(t.date)
            """
        else:
            q ="""
            SELECT SUM(subtotal) as forks FROM
            (SELECT MAX(forks) as subtotal, date, repo_id
            FROM repositories_log
            WHERE date>=""" + self.filters.startdate + """ AND date<""" + self.filters.enddate + """
            GROUP BY repo_id ) t
            """
        return q

class Watchers(Metrics):
    """ Number of watchers for a Docker Hub repo """

    id = "watchers"
    name = "Watchers"
    desc = "Number of watchers for a repo"
    data_source = DockerHubDS

    def _get_sql(self, evolutionary):

        if evolutionary:
            q = """
            SELECT YEAR(t.date)*12+MONTH(t.date) AS month, SUM(subtotal) as watchers
            FROM (SELECT MAX(watchers) as subtotal, date FROM repositories_log
            GROUP BY repo_id,YEAR(date)*12+MONTH(date)) t
            WHERE t.date>=""" + self.filters.startdate + """ AND t.date<""" + self.filters.enddate + """
            GROUP BY  YEAR(t.date),MONTH(t.date) ORDER BY YEAR(t.date),MONTH(t.date)
            """
        else:
            q ="""
            SELECT SUM(subtotal) as watchers FROM
            (SELECT MAX(watchers) as subtotal, date, repo_id
            FROM repositories_log
            WHERE date>=""" + self.filters.startdate + """ AND date<""" + self.filters.enddate + """
            GROUP BY repo_id ) t
            """
        return q
